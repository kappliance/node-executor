package nodeexec

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"io"
	"maps"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type dispatchClaim struct {
	Token       string    `json:"token"`
	ReceiptUID  types.UID `json:"receiptUID"`
	Sequence    uint64    `json:"sequence"`
	Fingerprint string    `json:"fingerprint"`
	JobUID      types.UID `json:"jobUID"`
}

func decodeDispatch(cm *corev1.ConfigMap, r receipt) (*dispatchClaim, *receipt, error) {
	var claim *dispatchClaim
	if data := cm.Data["dispatch.json"]; data != "" {
		decoder := json.NewDecoder(bytes.NewBufferString(data))
		decoder.DisallowUnknownFields()
		if len(data) > 1024 || decoder.Decode(&claim) != nil || decoder.Decode(new(any)) != io.EOF || claim == nil || claim.Token == "" || len(claim.Token) > 128 || claim.ReceiptUID != cm.UID || claim.Sequence != r.Sequence || claim.Fingerprint != r.Fingerprint || claim.JobUID != r.JobUID || !r.Dispatched || r.JobUID == "" {
			return nil, nil, errors.New("invalid execution dispatch claim")
		}
	}
	copy := cm.DeepCopy()
	copy.Data["unknown.json"] = cm.Data["pending-unknown.json"]
	owner := metav1.GetControllerOf(cm)
	if owner == nil {
		return nil, nil, errors.New("execution receipt owner is missing")
	}
	pending, err := decodeUnknown(copy, r, *owner)
	return claim, pending, err
}

func (e *Executor) readDispatchReceipt(ctx context.Context, original *corev1.ConfigMap, identity receipt) (*corev1.ConfigMap, receipt, error) {
	var current corev1.ConfigMap
	if err := e.reader.Get(ctx, client.ObjectKeyFromObject(original), &current); err != nil {
		return nil, receipt{}, err
	}
	owner := metav1.GetControllerOf(original)
	if current.UID != original.UID || owner == nil {
		return nil, receipt{}, errors.New("execution receipt identity changed")
	}
	r, err := decodeReceipt(&current, identity, *owner)
	return &current, r, err
}

func (e *Executor) patchDispatch(ctx context.Context, cm *corev1.ConfigMap, changes map[string]string) error {
	data := maps.Clone(cm.Data)
	for key, value := range changes {
		data[key] = value
	}
	patch, _ := json.Marshal([]map[string]any{
		{"op": "test", "path": "/metadata/uid", "value": string(cm.UID)},
		{"op": "test", "path": "/metadata/resourceVersion", "value": cm.ResourceVersion},
		{"op": "replace", "path": "/data", "value": data},
	})
	return e.client.Patch(ctx, cm, client.RawPatch(types.JSONPatchType, patch))
}

func blockedDispatch(r receipt, namespace string) Result {
	r.Phase, r.Reason = Pending, "DispatchClaimUnresolved"
	return r.result(namespace)
}

func (e *Executor) unknown(ctx context.Context, original *corev1.ConfigMap, evidence receipt, reason string) (Result, error) {
	evidence.Phase, evidence.Reason = Unknown, reason
	for {
		cm, current, err := e.readDispatchReceipt(ctx, original, evidence)
		if err != nil {
			return Result{}, err
		}
		uncertain, err := decodeUnknown(cm, evidence, *metav1.GetControllerOf(original))
		if err != nil {
			return Result{}, err
		}
		if uncertain != nil {
			return uncertain.result(e.namespace), nil
		}
		claim, pending, err := decodeDispatch(cm, current)
		if err != nil {
			return Result{}, err
		}
		if pending != nil {
			evidence = *pending
			if claim != nil {
				return blockedDispatch(current, e.namespace), nil
			}
		}
		data, _ := json.Marshal(evidence)
		changes := map[string]string{"unknown.json": string(data), "pending-unknown.json": ""}
		if claim != nil {
			changes = map[string]string{"pending-unknown.json": string(data)}
		}
		version := cm.ResourceVersion
		if err := e.patchDispatch(ctx, cm, changes); err != nil {
			latest, _, readErr := e.readDispatchReceipt(ctx, original, evidence)
			if readErr != nil {
				return Result{}, readErr
			}
			if latest.ResourceVersion == version {
				return Result{}, err
			}
			continue
		}
		if claim != nil {
			return blockedDispatch(current, e.namespace), nil
		}
		return evidence.result(e.namespace), nil
	}
}

func (e *Executor) finishDispatch(ctx context.Context, original *corev1.ConfigMap, identity receipt, token string) (Result, error) {
	for {
		cm, current, err := e.readDispatchReceipt(ctx, original, identity)
		if err != nil {
			return Result{}, err
		}
		uncertain, err := decodeUnknown(cm, identity, *metav1.GetControllerOf(original))
		if err != nil {
			return Result{}, err
		}
		if uncertain != nil {
			return uncertain.result(e.namespace), nil
		}
		claim, pending, err := decodeDispatch(cm, current)
		if err != nil {
			return Result{}, err
		}
		if claim == nil {
			return current.result(e.namespace), nil
		}
		if claim.Token != token {
			return blockedDispatch(current, e.namespace), nil
		}
		changes := map[string]string{"dispatch.json": ""}
		if pending != nil {
			changes["unknown.json"], changes["pending-unknown.json"] = cm.Data["pending-unknown.json"], ""
		}
		version := cm.ResourceVersion
		if err := e.patchDispatch(ctx, cm, changes); err != nil {
			latest, _, readErr := e.readDispatchReceipt(ctx, original, identity)
			if readErr != nil {
				return Result{}, readErr
			}
			if latest.ResourceVersion == version {
				return Result{}, err
			}
			continue
		}
		if pending != nil {
			return pending.result(e.namespace), nil
		}
		return current.result(e.namespace), nil
	}
}

func (e *Executor) dispatchBlocked(ctx context.Context, cm *corev1.ConfigMap, r receipt) (Result, bool, error) {
	claim, pending, err := decodeDispatch(cm, r)
	if err != nil {
		return Result{}, true, err
	}
	if claim == nil {
		if pending != nil {
			result, err := e.unknown(ctx, cm, *pending, pending.Reason)
			return result, true, err
		}
		return Result{}, false, nil
	}
	var job batchv1.Job
	if err := e.reader.Get(ctx, client.ObjectKey{Namespace: cm.Namespace, Name: r.JobName}, &job); err != nil {
		return blockedDispatch(r, e.namespace), true, err
	}
	if validJob(&job, r, cm.Name, *metav1.GetControllerOf(cm)) && !ptr.Deref(job.Spec.Suspend, false) {
		// An accepted release is observable; a suspended Job cannot prove non-acceptance.
		result, err := e.finishDispatch(ctx, cm, r, claim.Token)
		return result, true, err
	}
	return blockedDispatch(r, e.namespace), true, nil
}

func (e *Executor) dispatch(ctx context.Context, owner client.Object, original *corev1.ConfigMap, r receipt, job *batchv1.Job) (Result, error) {
	claim := dispatchClaim{Token: rand.Text(), ReceiptUID: original.UID, Sequence: r.Sequence, Fingerprint: r.Fingerprint, JobUID: r.JobUID}
	data, _ := json.Marshal(claim)
	acquireErr := e.patchDispatch(ctx, original, map[string]string{"dispatch.json": string(data)})
	cm, current, err := e.readDispatchReceipt(ctx, original, r)
	if err != nil {
		return Result{}, err
	}
	uncertain, err := decodeUnknown(cm, r, *metav1.GetControllerOf(original))
	if err != nil {
		return Result{}, err
	}
	if uncertain != nil {
		return uncertain.result(e.namespace), nil
	}
	held, pending, err := decodeDispatch(cm, current)
	if err != nil {
		return Result{}, err
	}
	if held == nil || held.Token != claim.Token {
		if acquireErr != nil && cm.ResourceVersion == original.ResourceVersion {
			return Result{}, acquireErr
		}
		return blockedDispatch(current, e.namespace), nil
	}
	if pending != nil {
		return e.finishDispatch(ctx, cm, current, claim.Token)
	}
	live, err := e.liveOwner(ctx, owner)
	if err == nil && live.GetDeletionTimestamp() != nil {
		err = errors.New("deleting owner cannot authorize dispatch")
	}
	if err == nil {
		err = e.checkNode(ctx, r.Node)
	}
	if err != nil {
		_, finishErr := e.finishDispatch(ctx, cm, current, claim.Token)
		return Result{}, errors.Join(err, finishErr)
	}
	before := job.DeepCopy()
	job.Spec.Suspend = ptr.To(false)
	if err := e.client.Patch(ctx, job, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
		switch apierrors.ReasonForError(err) {
		case metav1.StatusReasonConflict, metav1.StatusReasonInvalid, metav1.StatusReasonForbidden,
			metav1.StatusReasonUnauthorized, metav1.StatusReasonBadRequest, metav1.StatusReasonNotFound,
			metav1.StatusReasonMethodNotAllowed, metav1.StatusReasonRequestEntityTooLarge, metav1.StatusReasonTooManyRequests:
			_, finishErr := e.finishDispatch(ctx, cm, current, claim.Token)
			return Result{}, errors.Join(err, finishErr)
		}
		// Keep an ambiguous claim: a different invocation must never retry its release.
		return Result{}, err
	}
	return e.finishDispatch(ctx, cm, current, claim.Token)
}
