package nodeexec

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"reflect"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
)

// Executor writes through Client and observes through an uncached APIReader.
// New does not install a controller, CRD, daemon, or privileged node agent.
type Executor struct {
	client    client.Client
	reader    client.Reader
	namespace string
}

func New(writer client.Client, reader client.Reader, namespace string) *Executor {
	return &Executor{client: writer, reader: reader, namespace: namespace}
}

type receipt struct {
	Version     int                    `json:"version"`
	Name        string                 `json:"name"`
	OwnerUID    types.UID              `json:"ownerUID"`
	Node        corev1.ObjectReference `json:"node"`
	Revision    string                 `json:"revision"`
	Fingerprint string                 `json:"fingerprint"`
	Sequence    uint64                 `json:"sequence"`
	JobName     string                 `json:"jobName"`
	JobUID      types.UID              `json:"jobUID,omitempty"`
	PodUID      types.UID              `json:"podUID,omitempty"`
	PodName     string                 `json:"podName,omitempty"`
	Dispatched  bool                   `json:"dispatched"`
	Phase       Phase                  `json:"phase"`
	Reason      string                 `json:"reason,omitempty"`
}

func (r receipt) result(namespace string) Result {
	result := Result{Phase: r.Phase, Revision: r.Revision, Reason: r.Reason,
		Job: corev1.ObjectReference{APIVersion: "batch/v1", Kind: "Job", Namespace: namespace, Name: r.JobName, UID: r.JobUID},
		Pod: corev1.ObjectReference{APIVersion: "v1", Kind: "Pod", Namespace: namespace, Name: r.PodName, UID: r.PodUID}}
	if !result.Done() && result.Phase != Unknown {
		result.Result.RequeueAfter = pollInterval
	}
	return result
}

// Reconcile launches or observes one logical operation without blocking for its
// command to finish. Keep the owner and receipt until external effects are resolved.
// Unknown is sticky and never automatically retries or starts a newer revision.
func (e *Executor) Reconcile(ctx context.Context, owner client.Object, operation Operation) (Result, error) {
	if e.client == nil || e.reader == nil || e.namespace == "" || owner == nil || owner.GetUID() == "" || owner.GetName() == "" {
		return Result{}, errors.New("executor requires a client, APIReader, namespace, and persisted owner")
	}
	if owner.GetNamespace() != "" && owner.GetNamespace() != e.namespace {
		return Result{}, errors.New("execution namespace must match a namespaced owner's namespace")
	}
	template, err := normalize(operation)
	if err != nil {
		return Result{}, err
	}
	operation.Node = corev1.ObjectReference{Name: operation.Node.Name, UID: operation.Node.UID}
	gvk, err := apiutil.GVKForObject(owner, e.client.Scheme())
	if err != nil {
		return Result{}, err
	}
	ref := metav1.OwnerReference{APIVersion: gvk.GroupVersion().String(), Kind: gvk.Kind, Name: owner.GetName(), UID: owner.GetUID(), Controller: ptr.To(true), BlockOwnerDeletion: ptr.To(true)}
	live, err := e.liveOwner(ctx, owner)
	if err != nil {
		return Result{}, err
	}
	key := client.ObjectKey{Namespace: e.namespace, Name: ReceiptName(owner.GetUID(), operation.Node.UID, operation.Name)}
	fingerprint := hash(struct {
		Revision string
		Node     corev1.ObjectReference
		Template corev1.PodTemplateSpec
	}{operation.Revision, operation.Node, template})
	desired := receipt{Version: 1, Name: operation.Name, OwnerUID: owner.GetUID(), Node: operation.Node, Revision: operation.Revision, Fingerprint: fingerprint,
		Sequence: 1, JobName: jobName(key.Name, fingerprint, 1), Phase: Pending}
	var cm corev1.ConfigMap
	if err = e.reader.Get(ctx, key, &cm); apierrors.IsNotFound(err) {
		if live.GetDeletionTimestamp() != nil {
			return Result{}, errors.New("deleting owner cannot authorize a new operation")
		}
		if err := e.checkNode(ctx, operation.Node); err != nil {
			return Result{}, err
		}
		data, _ := json.Marshal(desired)
		cm = corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace, OwnerReferences: []metav1.OwnerReference{ref}, Labels: map[string]string{recordLabel: key.Name}}, Data: map[string]string{"receipt.json": string(data)}}
		if err = e.client.Create(ctx, &cm); err != nil && !apierrors.IsAlreadyExists(err) {
			return Result{}, err
		}
		return desired.result(e.namespace), nil
	} else if err != nil {
		return Result{}, err
	}
	r, err := decodeReceipt(&cm, desired, ref)
	if err != nil {
		return Result{}, err
	}
	if cm.DeletionTimestamp != nil {
		if r.Dispatched && !r.result(e.namespace).Done() {
			return e.unknown(ctx, &cm, r, "ExecutionReceiptDeleting")
		}
		return Result{}, errors.New("deleting execution receipt cannot authorize dispatch")
	}
	if r.Revision == desired.Revision && r.Fingerprint != desired.Fingerprint {
		return Result{}, errors.New("operation template changed without changing its revision")
	}
	if r.Phase == Unknown {
		return r.result(e.namespace), nil
	}
	if r.Revision != desired.Revision {
		if r.Phase == Succeeded || r.Phase == Failed {
			if live.GetDeletionTimestamp() != nil {
				return r.result(e.namespace), nil
			}
			if err := e.checkNode(ctx, desired.Node); err != nil {
				return Result{}, err
			}
			desired.Sequence = r.Sequence + 1
			if desired.Sequence == 0 {
				return Result{}, errors.New("execution sequence exhausted")
			}
			desired.JobName = jobName(cm.Name, desired.Fingerprint, desired.Sequence)
			return e.save(ctx, &cm, desired)
		}
		if !r.Dispatched {
			if r.JobUID != "" {
				var job batchv1.Job
				err := e.reader.Get(ctx, client.ObjectKey{Namespace: e.namespace, Name: r.JobName}, &job)
				if err != nil && !apierrors.IsNotFound(err) {
					return Result{}, err
				}
				if err == nil {
					if job.UID != r.JobUID || job.Spec.Suspend == nil || !*job.Spec.Suspend {
						return e.unknown(ctx, &cm, r, "SupersededExecutionMayHaveStarted")
					}
					if err = e.client.Delete(ctx, &job, client.Preconditions{UID: &job.UID}); err != nil && !apierrors.IsNotFound(err) {
						return Result{}, err
					}
				}
			}
			r.Phase, r.Reason = Failed, "SupersededBeforeDispatch"
			return e.save(ctx, &cm, r)
		}
		return e.observe(ctx, owner, &cm, r, ref, nil, false)
	}
	if r.Phase == Succeeded || r.Phase == Failed {
		return r.result(e.namespace), nil
	}
	if live.GetDeletionTimestamp() != nil && !r.Dispatched {
		r.Phase, r.Reason = Failed, "OwnerDeletingBeforeDispatch"
		return e.save(ctx, &cm, r)
	}
	return e.observe(ctx, owner, &cm, r, ref, &template, live.GetDeletionTimestamp() == nil)
}

func (e *Executor) liveOwner(ctx context.Context, owner client.Object) (client.Object, error) {
	live := owner.DeepCopyObject().(client.Object)
	if err := e.reader.Get(ctx, client.ObjectKeyFromObject(owner), live); err != nil {
		return nil, err
	}
	if live.GetUID() != owner.GetUID() || live.GetGeneration() != owner.GetGeneration() {
		return nil, errors.New("owner identity or desired generation changed")
	}
	return live, nil
}

func (e *Executor) checkNode(ctx context.Context, expected corev1.ObjectReference) error {
	var node corev1.Node
	if err := e.reader.Get(ctx, client.ObjectKey{Name: expected.Name}, &node); err != nil {
		return err
	}
	if node.UID != expected.UID || node.DeletionTimestamp != nil {
		return errors.New("target node identity changed")
	}
	return nil
}

func decodeReceipt(cm *corev1.ConfigMap, desired receipt, owner metav1.OwnerReference) (receipt, error) {
	var r receipt
	decoder := json.NewDecoder(bytes.NewBufferString(cm.Data["receipt.json"]))
	decoder.DisallowUnknownFields()
	if !sameOwner(cm, owner) || len(cm.Data["receipt.json"]) > 8192 || decoder.Decode(&r) != nil || decoder.Decode(new(any)) != io.EOF || r.Version != 1 || r.Name != desired.Name || r.OwnerUID != desired.OwnerUID || r.Node.Name != desired.Node.Name || r.Node.UID != desired.Node.UID || r.Revision == "" || len(r.Revision) > 256 || !digest(r.Fingerprint) || r.Sequence == 0 || r.JobName != jobName(cm.Name, r.Fingerprint, r.Sequence) {
		return r, errors.New("execution receipt identity or contents are invalid")
	}
	if r.Dispatched && r.JobUID == "" || (r.PodUID == "") != (r.PodName == "") || r.PodUID != "" && !r.Dispatched || (r.Phase == Running || r.Phase == Succeeded) && r.PodUID == "" {
		return r, errors.New("execution receipt contains inconsistent dispatch evidence")
	}
	switch r.Phase {
	case Pending, Running, Succeeded, Failed, Unknown:
	default:
		return r, errors.New("invalid execution receipt phase")
	}
	return r, nil
}

func sameOwner(object metav1.Object, expected metav1.OwnerReference) bool {
	owner := metav1.GetControllerOf(object)
	return owner != nil && owner.UID == expected.UID && owner.Name == expected.Name && owner.Kind == expected.Kind && owner.APIVersion == expected.APIVersion
}

func (e *Executor) save(ctx context.Context, cm *corev1.ConfigMap, r receipt) (Result, error) {
	before := cm.DeepCopy()
	data, _ := json.Marshal(r)
	if cm.Data == nil {
		cm.Data = map[string]string{}
	}
	cm.Data["receipt.json"] = string(data)
	if reflect.DeepEqual(before.Data, cm.Data) {
		return r.result(e.namespace), nil
	}
	if err := e.client.Patch(ctx, cm, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
		if apierrors.IsConflict(err) {
			return Result{Phase: Pending, Result: ctrl.Result{RequeueAfter: pollInterval}}, nil
		}
		return Result{}, err
	}
	return r.result(e.namespace), nil
}

func (e *Executor) unknown(ctx context.Context, cm *corev1.ConfigMap, r receipt, reason string) (Result, error) {
	r.Phase, r.Reason = Unknown, reason
	return e.save(ctx, cm, r)
}

func (e *Executor) observe(ctx context.Context, owner client.Object, cm *corev1.ConfigMap, r receipt, ref metav1.OwnerReference, template *corev1.PodTemplateSpec, authorize bool) (Result, error) {
	var job batchv1.Job
	key := client.ObjectKey{Namespace: e.namespace, Name: r.JobName}
	if err := e.reader.Get(ctx, key, &job); apierrors.IsNotFound(err) {
		if r.Dispatched || r.JobUID != "" {
			return e.unknown(ctx, cm, r, "IssuedJobMissing")
		}
		if template == nil || !authorize {
			return r.result(e.namespace), nil
		}
		if _, err := e.liveOwner(ctx, owner); err != nil {
			return Result{}, err
		}
		if err := e.checkNode(ctx, r.Node); err != nil {
			return Result{}, err
		}
		job = *jobFor(e.namespace, cm.Name, r, *template.DeepCopy(), ref)
		if err := e.client.Create(ctx, &job); err != nil && !apierrors.IsAlreadyExists(err) {
			return Result{}, err
		}
		return r.result(e.namespace), nil
	} else if err != nil {
		return Result{}, err
	}
	if !sameOwner(&job, ref) || job.Annotations[fingerprintAnnotation] != r.Fingerprint || job.Labels[recordLabel] != cm.Name || job.UID == "" || job.DeletionTimestamp != nil || r.JobUID != "" && job.UID != r.JobUID {
		return e.unknown(ctx, cm, r, "JobIdentityChanged")
	}
	if !r.Dispatched && (job.Spec.Suspend == nil || !*job.Spec.Suspend) {
		return e.unknown(ctx, cm, r, "JobStartedWithoutReceipt")
	}
	if r.JobUID == "" {
		r.JobUID = job.UID
		return e.save(ctx, cm, r)
	}
	if job.Spec.Suspend != nil && *job.Spec.Suspend {
		if !authorize || template == nil {
			if r.Dispatched {
				return e.unknown(ctx, cm, r, "AuthorizationChangedDuringDispatch")
			}
			return r.result(e.namespace), nil
		}
		live, err := e.liveOwner(ctx, owner)
		if err != nil {
			return Result{}, err
		}
		if live.GetDeletionTimestamp() != nil {
			return e.unknown(ctx, cm, r, "OwnerDeletingDuringDispatch")
		}
		if err := e.checkNode(ctx, r.Node); err != nil {
			return Result{}, err
		}
		if !r.Dispatched {
			r.Dispatched = true
			return e.save(ctx, cm, r)
		}
		before := job.DeepCopy()
		job.Spec.Suspend = ptr.To(false)
		if err := e.client.Patch(ctx, &job, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
			return Result{}, err
		}
		return r.result(e.namespace), nil
	}
	if err := e.checkNode(ctx, r.Node); err != nil {
		if apierrors.IsNotFound(err) || err.Error() == "target node identity changed" {
			return e.unknown(ctx, cm, r, "TargetNodeChangedAfterDispatch")
		}
		return Result{}, err
	}
	var pods corev1.PodList
	if err := e.reader.List(ctx, &pods, client.InNamespace(e.namespace), client.MatchingLabels{"batch.kubernetes.io/controller-uid": string(job.UID)}); err != nil {
		return Result{}, err
	}
	if len(pods.Items) > 1 {
		return e.unknown(ctx, cm, r, "MultipleExecutionPods")
	}
	if len(pods.Items) == 0 {
		if r.PodUID != "" || job.Status.Succeeded > 0 || job.Status.Failed > 0 {
			return e.unknown(ctx, cm, r, "ExecutionPodMissing")
		}
		return r.result(e.namespace), nil
	}
	pod := &pods.Items[0]
	podOwner := metav1.GetControllerOf(pod)
	if podOwner == nil || podOwner.APIVersion != "batch/v1" || podOwner.Kind != "Job" || podOwner.Name != job.Name || podOwner.UID != job.UID || pod.UID == "" || r.PodUID != "" && r.PodUID != pod.UID || pod.Spec.NodeName != "" && pod.Spec.NodeName != r.Node.Name {
		return e.unknown(ctx, cm, r, "PodIdentityChanged")
	}
	if r.PodUID == "" {
		r.PodUID, r.PodName = pod.UID, pod.Name
		return e.save(ctx, cm, r)
	}
	if pod.Spec.NodeName == "" {
		return r.result(e.namespace), nil
	}
	if pod.Status.Phase == corev1.PodSucceeded {
		r.Phase, r.Reason = Succeeded, "ProcessCompleted"
	} else if pod.Status.Phase == corev1.PodFailed {
		r.Phase, r.Reason = Failed, "ProcessFailed"
	} else if pod.DeletionTimestamp != nil {
		return e.unknown(ctx, cm, r, "ExecutionPodDeleting")
	} else if pod.Status.Phase == corev1.PodRunning {
		r.Phase = Running
	}
	return e.save(ctx, cm, r)
}

// ReceiptName lets callers inspect retained execution records with native clients.
func ReceiptName(ownerUID, nodeUID types.UID, operationName string) string {
	return "execution-" + hash([]string{string(ownerUID), string(nodeUID), operationName})[:32]
}
