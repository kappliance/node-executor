package nodeexec

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestDispatchClaimAPIErrorPreservesLaunchBoundary(t *testing.T) {
	for reason, code := range map[metav1.StatusReason]int32{
		metav1.StatusReasonConflict:              409,
		metav1.StatusReasonInvalid:               422,
		metav1.StatusReasonForbidden:             403,
		metav1.StatusReasonUnauthorized:          401,
		metav1.StatusReasonBadRequest:            400,
		metav1.StatusReasonNotFound:              404,
		metav1.StatusReasonMethodNotAllowed:      405,
		metav1.StatusReasonRequestEntityTooLarge: 413,
		metav1.StatusReasonTooManyRequests:       429,
		metav1.StatusReasonTimeout:               504,
		metav1.StatusReasonServerTimeout:         500,
		metav1.StatusReasonInternalError:         500,
	} {
		for _, queued := range []bool{false, true} {
			t.Run(string(reason)+map[bool]string{false: "/retry", true: "/queued-evidence"}[queued], func(t *testing.T) {
				f := setup(t)
				job := dispatchClaimReady(t, f)
				cm, original := f.receipt(t)
				attempts := 0
				writer := interceptor.NewClient(f.client.(client.WithWatch), interceptor.Funcs{
					Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
						if _, ok := obj.(*batchv1.Job); ok {
							attempts++
							if queued {
								result, err := f.executor.unknown(ctx, &cm, original, "RejectionUncertainty")
								assertDispatchClaimPending(t, result, err)
							}
							return &apierrors.StatusError{ErrStatus: metav1.Status{Status: metav1.StatusFailure, Reason: reason, Code: code}}
						}
						return c.Patch(ctx, obj, patch, opts...)
					},
				})
				if _, err := New(writer, f.client, job.Namespace).Reconcile(context.Background(), f.owner, f.op); err == nil || attempts != 1 {
					t.Fatalf("definitive rejection was not exercised: %v, attempts=%d", err, attempts)
				}
				retained, _ := f.receipt(t)
				if reason == metav1.StatusReasonTimeout || reason == metav1.StatusReasonServerTimeout || reason == metav1.StatusReasonInternalError {
					if retained.Data["dispatch.json"] == "" || retained.Data["unknown.json"] != "" || (retained.Data["pending-unknown.json"] != "") != queued {
						t.Fatalf("ambiguous API failure dropped its boundary: %v", retained.Data)
					}
					for range 2 {
						result, err := f.executor.Reconcile(context.Background(), f.owner, f.op)
						assertDispatchClaimPending(t, result, err)
					}
					if !ptr.Deref(f.job(t).Spec.Suspend, false) {
						t.Fatal("ambiguous API failure retried launch")
					}
					return
				}
				if retained.Data["dispatch.json"] != "" || retained.Data["pending-unknown.json"] != "" {
					t.Fatalf("definitive rejection abandoned its claim: %v", retained.Data)
				}
				result := f.reconcile(t)
				if queued {
					if result.Phase != Unknown || result.Reason != "RejectionUncertainty" || !ptr.Deref(f.job(t).Spec.Suspend, false) {
						t.Fatalf("queued uncertainty was not published before another launch: %+v", result)
					}
				} else if ptr.Deref(f.job(t).Spec.Suspend, true) || retained.Data["unknown.json"] != "" {
					t.Fatalf("restored permissions did not permit previously rejected launch: %+v", result)
				}
			})
		}
	}
}

func dispatchClaimReady(t *testing.T, f fixture) *batchv1.Job {
	t.Helper()
	for range 8 {
		f.reconcile(t)
		cm, r := f.receipt(t)
		if !r.Dispatched || r.JobUID == "" {
			continue
		}
		var job batchv1.Job
		if err := f.client.Get(context.Background(), client.ObjectKey{Namespace: cm.Namespace, Name: r.JobName}, &job); err != nil {
			t.Fatal(err)
		}
		if !ptr.Deref(job.Spec.Suspend, false) || cm.Data["dispatch.json"] != "" {
			t.Fatal("fixture passed the unclaimed, suspended dispatch boundary")
		}
		return &job
	}
	t.Fatal("fixture never reached the dispatch boundary")
	return nil
}

func dispatchClaimPredecessor(t *testing.T) (fixture, corev1.ConfigMap, receipt, *batchv1.Job) {
	t.Helper()
	f := setup(t)
	job := f.start(t)
	pod := f.pod(t, job, corev1.PodSucceeded)
	f.reconcile(t)
	if result := f.reconcile(t); result.Phase != Succeeded {
		t.Fatalf("predecessor did not finish: %+v", result)
	}
	cm, original := f.receipt(t)
	original.ObservedPods = []corev1.ObjectReference{
		{APIVersion: "v1", Kind: "Pod", Namespace: pod.Namespace, Name: pod.Name, UID: pod.UID},
		{APIVersion: "v1", Kind: "Pod", Namespace: pod.Namespace, Name: pod.Name + "-other", UID: "other-pod"},
	}
	f.op.Revision = "v2"
	return f, cm, original, dispatchClaimReady(t, f)
}

func assertDispatchClaimPending(t *testing.T, result Result, err error) {
	t.Helper()
	if err != nil || result.Phase != Pending || result.Reason != "DispatchClaimUnresolved" || result.Result.RequeueAfter <= 0 {
		t.Fatalf("unsettled launch was not retained: %+v, %v", result, err)
	}
}

func assertDispatchClaimSnapshot(t *testing.T, data string, original receipt) {
	t.Helper()
	want := original
	want.Phase, want.Reason = Unknown, "MultipleExecutionPods"
	var got receipt
	if err := json.Unmarshal([]byte(data), &got); err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("original uncertainty snapshot changed: got=%+v want=%+v err=%v", got, want, err)
	}
}

func TestDispatchClaimAcquisitionLostReply(t *testing.T) {
	for _, accepted := range []bool{false, true} {
		t.Run(map[bool]string{false: "before-acceptance", true: "after-acceptance"}[accepted], func(t *testing.T) {
			ctx := context.Background()
			f := setup(t)
			job := dispatchClaimReady(t, f)
			before, _ := f.receipt(t)
			lostReply, jobPatches := false, 0
			var writer client.WithWatch
			writer = interceptor.NewClient(f.client.(client.WithWatch), interceptor.Funcs{
				Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
					if _, ok := obj.(*corev1.ConfigMap); ok && !lostReply {
						data, err := patch.Data(obj)
						if err != nil {
							t.Fatal(err)
						}
						var operations []struct {
							Op    string `json:"op"`
							Path  string `json:"path"`
							Value any    `json:"value"`
						}
						if patch.Type() != types.JSONPatchType || json.Unmarshal(data, &operations) != nil {
							t.Fatalf("claim acquisition was not a JSON patch: %s", data)
						}
						preconditions := map[string]any{}
						for _, operation := range operations {
							if operation.Op == "test" {
								preconditions[operation.Path] = operation.Value
							}
						}
						if preconditions["/metadata/uid"] != string(before.UID) || preconditions["/metadata/resourceVersion"] != before.ResourceVersion {
							t.Fatalf("claim acquisition omitted receipt UID/RV guards: %s", data)
						}
						lostReply = true
						if accepted {
							if err := c.Patch(ctx, obj, patch, opts...); err != nil {
								return err
							}
						}
						return errors.New("claim acquisition reply lost")
					}
					if _, ok := obj.(*batchv1.Job); ok {
						jobPatches++
						if jobPatches > 1 {
							t.Fatal("another invocation attempted to launch the claimed Job")
						}
						cm, _ := f.receipt(t)
						if cm.Data["dispatch.json"] == "" {
							t.Fatal("Job mutation preceded durable claim acquisition")
						}
						result, err := New(writer, f.client, job.Namespace).Reconcile(ctx, f.owner, f.op)
						assertDispatchClaimPending(t, result, err)
					}
					return c.Patch(ctx, obj, patch, opts...)
				},
			})
			_, err := New(writer, f.client, job.Namespace).Reconcile(ctx, f.owner, f.op)
			if !lostReply {
				t.Fatal("claim acquisition fault was not exercised")
			}
			if err := f.client.Get(ctx, client.ObjectKeyFromObject(job), job); err != nil {
				t.Fatal(err)
			}
			cm, _ := f.receipt(t)
			if accepted {
				if err != nil || jobPatches != 1 || ptr.Deref(job.Spec.Suspend, true) || cm.Data["dispatch.json"] != "" {
					t.Fatalf("accepted acquisition was not recovered exactly once: patches=%d result=%+v err=%v", jobPatches, job.Spec.Suspend, err)
				}
				for range 2 {
					if _, err := New(writer, f.client, job.Namespace).Reconcile(ctx, f.owner, f.op); err != nil {
						t.Fatal(err)
					}
				}
				if jobPatches != 1 {
					t.Fatalf("accepted claim replayed a Job patch: %d", jobPatches)
				}
			} else if jobPatches != 0 || !ptr.Deref(job.Spec.Suspend, false) || cm.Data["dispatch.json"] != "" {
				t.Fatalf("unaccepted acquisition authorized launch: patches=%d claim=%q", jobPatches, cm.Data["dispatch.json"])
			}
		})
	}
}

func TestDispatchClaimAmbiguousLaunchRetainsOriginalPendingEvidence(t *testing.T) {
	ctx := context.Background()
	f, stale, original, job := dispatchClaimPredecessor(t)
	jobPatches := 0
	var queued string
	writer := interceptor.NewClient(f.client.(client.WithWatch), interceptor.Funcs{
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			if _, ok := obj.(*batchv1.Job); ok {
				jobPatches++
				if jobPatches > 1 {
					t.Fatal("ambiguous launch was retried")
				}
				result, err := New(f.client, f.client, job.Namespace).unknown(ctx, &stale, original, "MultipleExecutionPods")
				assertDispatchClaimPending(t, result, err)
				cm, _ := f.receipt(t)
				queued = cm.Data["pending-unknown.json"]
				assertDispatchClaimSnapshot(t, queued, original)
				if cm.Data["unknown.json"] != "" || cm.Data["dispatch.json"] == "" {
					t.Fatal("held claim was bypassed while queuing uncertainty")
				}
				return errors.New("Job patch was not accepted; transport outcome is ambiguous")
			}
			return c.Patch(ctx, obj, patch, opts...)
		},
	})
	if _, err := New(writer, f.client, job.Namespace).Reconcile(ctx, f.owner, f.op); err == nil || jobPatches != 1 {
		t.Fatalf("unaccepted Job fault was not exercised: patches=%d err=%v", jobPatches, err)
	}
	held, _ := f.receipt(t)
	later := original
	later.JobUID, later.PodUID, later.PodName = "later-job", "later-pod", "later-pod"
	later.ObservedPods = nil
	result, err := New(f.client, f.client, job.Namespace).unknown(ctx, &stale, later, "IssuedJobMissing")
	assertDispatchClaimPending(t, result, err)
	for _, revision := range []string{"v2", "v3", "v1"} {
		for _, cancel := range []bool{false, true} {
			f.op.Revision, f.op.Cancel = revision, cancel
			result, err := New(writer, f.client, job.Namespace).Reconcile(ctx, f.owner, f.op)
			assertDispatchClaimPending(t, result, err)
			cm, _ := f.receipt(t)
			if cm.Data["dispatch.json"] != held.Data["dispatch.json"] || cm.Data["pending-unknown.json"] != queued || cm.Data["unknown.json"] != "" || cm.Data["receipt.json"] != held.Data["receipt.json"] {
				t.Fatalf("restart/revision/cancellation changed ambiguous evidence: revision=%s cancel=%v data=%v", revision, cancel, cm.Data)
			}
		}
	}
	if err := f.client.Get(ctx, client.ObjectKeyFromObject(job), job); err != nil || !ptr.Deref(job.Spec.Suspend, false) || jobPatches != 1 {
		t.Fatalf("unaccepted ambiguous launch escaped its claim: patches=%d err=%v", jobPatches, err)
	}
}

func TestDispatchClaimAcceptedLostJobReplyRecoversObservation(t *testing.T) {
	ctx := context.Background()
	f := setup(t)
	job := dispatchClaimReady(t, f)
	jobPatches := 0
	writer := interceptor.NewClient(f.client.(client.WithWatch), interceptor.Funcs{
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			if _, ok := obj.(*batchv1.Job); ok {
				jobPatches++
				if jobPatches > 1 {
					t.Fatal("accepted Job release was patched again")
				}
				if err := c.Patch(ctx, obj, patch, opts...); err != nil {
					return err
				}
				return errors.New("reply lost after accepted Job patch")
			}
			return c.Patch(ctx, obj, patch, opts...)
		},
	})
	_, _ = New(writer, f.client, job.Namespace).Reconcile(ctx, f.owner, f.op)
	if err := f.client.Get(ctx, client.ObjectKeyFromObject(job), job); err != nil || ptr.Deref(job.Spec.Suspend, true) || jobPatches != 1 {
		t.Fatalf("accepted lost Job reply was not exercised: patches=%d err=%v", jobPatches, err)
	}
	pod := f.pod(t, job, corev1.PodSucceeded)
	var result Result
	for range 4 {
		var err error
		result, err = New(writer, f.client, job.Namespace).Reconcile(ctx, f.owner, f.op)
		if err != nil {
			t.Fatal(err)
		}
		if result.Phase == Succeeded {
			break
		}
	}
	cm, _ := f.receipt(t)
	if result.Phase != Succeeded || result.Pod.UID != pod.UID || jobPatches != 1 || cm.Data["dispatch.json"] != "" {
		t.Fatalf("restart did not finalize and observe accepted launch: result=%+v patches=%d claim=%q", result, jobPatches, cm.Data["dispatch.json"])
	}
}

func TestDispatchClaimFinalizationLostReplyKeepsOriginalUnknown(t *testing.T) {
	ctx := context.Background()
	f, stale, original, job := dispatchClaimPredecessor(t)
	jobPatches, lostReply := 0, false
	var queued string
	writer := interceptor.NewClient(f.client.(client.WithWatch), interceptor.Funcs{
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			if _, ok := obj.(*batchv1.Job); ok {
				jobPatches++
				if jobPatches > 1 {
					t.Fatal("finalization failure replayed Job release")
				}
				result, err := New(f.client, f.client, job.Namespace).unknown(ctx, &stale, original, "MultipleExecutionPods")
				assertDispatchClaimPending(t, result, err)
				cm, _ := f.receipt(t)
				queued = cm.Data["pending-unknown.json"]
				assertDispatchClaimSnapshot(t, queued, original)
				if cm.Data["dispatch.json"] == "" || cm.Data["unknown.json"] != "" {
					t.Fatal("uncertainty bypassed an in-flight claim")
				}
			}
			if err := c.Patch(ctx, obj, patch, opts...); err != nil {
				return err
			}
			if _, ok := obj.(*corev1.ConfigMap); ok && jobPatches == 1 && !lostReply {
				cm, _ := f.receipt(t)
				if cm.Data["unknown.json"] != queued || cm.Data["pending-unknown.json"] != "" || cm.Data["dispatch.json"] != "" {
					t.Fatalf("finalization was not atomic: %v", cm.Data)
				}
				lostReply = true
				return errors.New("reply lost after accepted claim finalization")
			}
			return nil
		},
	})
	_, _ = New(writer, f.client, job.Namespace).Reconcile(ctx, f.owner, f.op)
	if !lostReply || jobPatches != 1 {
		t.Fatalf("finalization fault was not exercised: lost=%v patches=%d", lostReply, jobPatches)
	}
	for _, revision := range []string{"v2", "v3", "v1"} {
		f.op.Revision, f.op.Cancel = revision, true
		result, err := New(writer, f.client, job.Namespace).Reconcile(ctx, f.owner, f.op)
		want := original
		want.Phase, want.Reason = Unknown, "MultipleExecutionPods"
		if err != nil || !reflect.DeepEqual(result, want.result(job.Namespace)) {
			t.Fatalf("finalization readback lost original uncertainty: %+v, %v", result, err)
		}
	}
	later := original
	later.JobUID, later.ObservedPods = "another-job", nil
	result, err := New(writer, f.client, job.Namespace).unknown(ctx, &stale, later, "IssuedJobMissing")
	want := original
	want.Phase, want.Reason = Unknown, "MultipleExecutionPods"
	if err != nil || !reflect.DeepEqual(result, want.result(job.Namespace)) {
		t.Fatalf("later evidence replaced permanent uncertainty: %+v, %v", result, err)
	}
	cm, _ := f.receipt(t)
	assertDispatchClaimSnapshot(t, cm.Data["unknown.json"], original)
	if cm.Data["unknown.json"] != queued || cm.Data["dispatch.json"] != "" || cm.Data["pending-unknown.json"] != "" || jobPatches != 1 {
		t.Fatal("finalized uncertainty was changed or dispatch was replayed")
	}
}

func TestDispatchClaimMalformedAndForeignIdentityFailClosed(t *testing.T) {
	for _, invalid := range []string{"malformed", "missing-token", "receipt-uid", "sequence", "fingerprint", "job-uid"} {
		t.Run(invalid, func(t *testing.T) {
			ctx := context.Background()
			f := setup(t)
			job := dispatchClaimReady(t, f)
			cm, r := f.receipt(t)
			claim := dispatchClaim{Token: "claim-token", ReceiptUID: cm.UID, Sequence: r.Sequence, Fingerprint: r.Fingerprint, JobUID: job.UID}
			switch invalid {
			case "missing-token":
				claim.Token = ""
			case "receipt-uid":
				claim.ReceiptUID = "foreign-receipt"
			case "sequence":
				claim.Sequence++
			case "fingerprint":
				claim.Fingerprint = hash("foreign-template")
			case "job-uid":
				claim.JobUID = "foreign-job"
			}
			data, err := json.Marshal(claim)
			if err != nil {
				t.Fatal(err)
			}
			cm.Data["dispatch.json"] = string(data)
			if invalid == "malformed" {
				cm.Data["dispatch.json"] = "{"
			}
			if err := f.client.Update(ctx, &cm); err != nil {
				t.Fatal(err)
			}
			jobPatches := 0
			writer := interceptor.NewClient(f.client.(client.WithWatch), interceptor.Funcs{
				Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
					if _, ok := obj.(*batchv1.Job); ok {
						jobPatches++
					}
					return c.Patch(ctx, obj, patch, opts...)
				},
			})
			if _, err := New(writer, f.client, job.Namespace).Reconcile(ctx, f.owner, f.op); err == nil {
				t.Fatal("invalid claim identity was accepted")
			}
			after, _ := f.receipt(t)
			if err := f.client.Get(ctx, client.ObjectKeyFromObject(job), job); err != nil || !ptr.Deref(job.Spec.Suspend, false) || jobPatches != 0 || !reflect.DeepEqual(after.Data, cm.Data) {
				t.Fatalf("invalid claim changed dispatch state: patches=%d err=%v", jobPatches, err)
			}
		})
	}
}
