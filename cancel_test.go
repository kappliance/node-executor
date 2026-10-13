package nodeexec

import (
	"context"
	"errors"
	"reflect"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func (f fixture) receipt(t *testing.T) (corev1.ConfigMap, receipt) {
	t.Helper()
	var cm corev1.ConfigMap
	if err := f.client.Get(context.Background(), client.ObjectKey{Namespace: "operations", Name: ReceiptName(f.owner.UID, f.op.Node.UID, f.op.Name)}, &cm); err != nil {
		t.Fatal(err)
	}
	r, err := decodeReceipt(&cm, receipt{Name: f.op.Name, OwnerUID: f.owner.UID, Node: f.op.Node}, *metav1.GetControllerOf(&cm))
	if err != nil {
		t.Fatal(err)
	}
	return cm, r
}

func TestCancellationBeforeDispatch(t *testing.T) {
	for steps, stage := range []string{"initial", "receipt-only", "unbound-job", "bound-job"} {
		t.Run(stage, func(t *testing.T) {
			f := setup(t)
			for range steps {
				f.reconcile(t)
			}
			var original receipt
			if steps > 0 {
				_, original = f.receipt(t)
			}
			var job *batchv1.Job
			if steps >= 2 {
				job = f.job(t)
			}
			f.op.Cancel = true
			r := f.reconcile(t)
			if r.Phase != Failed || r.Reason != "CancelledBeforeDispatch" || r.Revision != "v1" || !r.Done() || r.Result.RequeueAfter != 0 {
				t.Fatalf("cancellation did not terminate undispatched work: %+v", r)
			}
			_, cancelled := f.receipt(t)
			if cancelled.Dispatched || steps > 0 && (cancelled.Fingerprint != original.Fingerprint || cancelled.Sequence != original.Sequence || cancelled.JobName != original.JobName) {
				t.Fatalf("cancellation changed execution identity: %+v", cancelled)
			}
			if job != nil {
				if r.Job.UID != job.UID || !reflect.DeepEqual(job, f.job(t)) {
					t.Fatalf("cancellation mutated the suspended Job or lost its UID: %+v", r)
				}
			} else {
				var jobs batchv1.JobList
				if err := f.client.List(context.Background(), &jobs); err != nil || len(jobs.Items) != 0 {
					t.Fatalf("cancellation created a Job: %d, %v", len(jobs.Items), err)
				}
			}
			// Neither renewed authorization nor a cancelled newer revision replays work.
			f.op.Cancel = false
			if again := f.reconcile(t); !reflect.DeepEqual(again, r) {
				t.Fatalf("same-revision authorization replaced cancellation: %+v", again)
			}
			f.op.Cancel, f.op.Revision = true, "v2"
			f.op.Template.Spec.Containers[0].Args = []string{"changed"}
			if again := f.reconcile(t); !reflect.DeepEqual(again, r) {
				t.Fatalf("cancelled new revision replaced the terminal receipt: %+v", again)
			}
		})
	}
}

func TestCancellationAllowsChangedTemplateAndRevision(t *testing.T) {
	for _, revision := range []string{"v1", "v2"} {
		t.Run(revision, func(t *testing.T) {
			f := setup(t)
			for range 3 {
				f.reconcile(t)
			}
			before, original := f.receipt(t)
			f.op.Cancel, f.op.Revision = true, revision
			f.op.Template.Spec.Containers[0].Args = []string{"new-desired-command"}
			r := f.reconcile(t)
			if r.Phase != Failed || r.Reason != "CancelledBeforeDispatch" || r.Revision != original.Revision || r.Job.UID != original.JobUID {
				t.Fatalf("cancellation rejected or replaced old work: %+v", r)
			}
			after, retained := f.receipt(t)
			if retained.Fingerprint != original.Fingerprint || retained.Sequence != original.Sequence || after.UID != before.UID {
				t.Fatalf("cancellation mutated execution identity: %+v", retained)
			}
		})
	}
}

func TestCancellationAfterInterruptedUnsuspend(t *testing.T) {
	for _, accepted := range []bool{false, true} {
		t.Run(map[bool]string{false: "not-accepted", true: "accepted"}[accepted], func(t *testing.T) {
			f := setup(t)
			for range 4 {
				f.reconcile(t)
			}
			job := f.job(t)
			patches := 0
			writer := interceptor.NewClient(f.client.(client.WithWatch), interceptor.Funcs{
				Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
					if _, ok := obj.(*batchv1.Job); ok {
						patches++
						if accepted {
							if err := c.Patch(ctx, obj, patch, opts...); err != nil {
								return err
							}
						}
						return errors.New("interrupted unsuspend request")
					}
					return c.Patch(ctx, obj, patch, opts...)
				},
			})
			f.executor = New(writer, f.client, "operations")
			if _, err := f.executor.Reconcile(context.Background(), f.owner, f.op); err == nil || patches != 1 {
				t.Fatalf("unsuspend interruption was not exercised: %v, patches=%d", err, patches)
			}
			f.op.Cancel = true
			r := f.reconcile(t)
			if accepted {
				if r.Phase != Pending || r.Job.UID != job.UID {
					t.Fatalf("accepted dispatch was cancelled: %+v", r)
				}
				pod := f.pod(t, job, corev1.PodSucceeded)
				f.reconcile(t)
				if r = f.reconcile(t); r.Phase != Succeeded || r.Pod.UID != pod.UID {
					t.Fatalf("terminal report after withdrawal was lost: %+v", r)
				}
			} else {
				if r.Phase != Unknown || r.Reason != "AuthorizationCancelledDuringDispatch" || r.Job.UID != job.UID {
					t.Fatalf("ambiguous dispatch was not retained: %+v", r)
				}
				f.op.Cancel, f.op.Revision = false, "v2"
				if again := f.reconcile(t); !reflect.DeepEqual(again, r) {
					t.Fatalf("ambiguous cancellation was not sticky: %+v", again)
				}
			}
			if patches != 1 || ptr.Deref(f.job(t).Spec.Suspend, false) == accepted {
				t.Fatal("cancellation patched Job suspension")
			}
		})
	}
}

func TestCancellationBeforeUnsuspendIsStickyUnknown(t *testing.T) {
	f := setup(t)
	for range 4 {
		f.reconcile(t)
	}
	job := f.job(t)
	f.op.Cancel = true
	r := f.reconcile(t)
	if r.Phase != Unknown || r.Reason != "AuthorizationCancelledDuringDispatch" || r.Job.UID != job.UID || r.Result.RequeueAfter != 0 {
		t.Fatalf("dispatched suspended Job lost uncertainty: %+v", r)
	}
	if !reflect.DeepEqual(job, f.job(t)) {
		t.Fatal("cancelled reconciliation released or modified Job")
	}
	f.op.Revision = "v2"
	f.op.Template.Spec.Containers[0].Args = []string{"changed"}
	if again := f.reconcile(t); !reflect.DeepEqual(again, r) {
		t.Fatalf("uncertainty evidence was replaced: %+v", again)
	}
}

func TestCancellationObservesRunningAndRetainsTerminalEvidence(t *testing.T) {
	for _, phase := range []corev1.PodPhase{corev1.PodSucceeded, corev1.PodFailed} {
		t.Run(string(phase), func(t *testing.T) {
			f := setup(t)
			job := f.start(t)
			pod := f.pod(t, job, corev1.PodRunning)
			f.reconcile(t)
			before := f.reconcile(t)
			f.op.Cancel, f.op.Revision = true, "v2"
			f.op.Template.Spec.Containers[0].Args = []string{"changed"}
			if r := f.reconcile(t); !reflect.DeepEqual(r, before) || r.Phase != Running {
				t.Fatalf("running evidence changed after withdrawal: %+v", r)
			}
			if !reflect.DeepEqual(job, f.job(t)) {
				t.Fatal("cancellation mutated running Job")
			}
			pod.Status.Phase = phase
			if err := f.client.Status().Update(context.Background(), pod); err != nil {
				t.Fatal(err)
			}
			terminal := f.reconcile(t)
			want := Succeeded
			if phase == corev1.PodFailed {
				want = Failed
			}
			if terminal.Phase != want || terminal.Revision != "v1" || terminal.Job.UID != job.UID || terminal.Pod.UID != pod.UID || !terminal.Done() {
				t.Fatalf("terminal report after withdrawal was lost: %+v", terminal)
			}
			cm, _ := f.receipt(t)
			for _, object := range []client.Object{job, pod} {
				if err := f.client.Delete(context.Background(), object); err != nil {
					t.Fatal(err)
				}
			}
			for _, revision := range []string{"v1", "v2"} {
				f.op.Revision = revision
				if r := f.reconcile(t); !reflect.DeepEqual(r, terminal) {
					t.Fatalf("terminal evidence changed after cleanup: %+v", r)
				}
			}
			after, _ := f.receipt(t)
			if !reflect.DeepEqual(cm, after) {
				t.Fatal("cancellation rewrote terminal receipt")
			}
		})
	}
}

func TestCancellationChecksUndispatchedJobEvidence(t *testing.T) {
	for _, scenario := range []string{"identity", "fingerprint", "unsuspended", "missing", "start", "active", "terminating", "uncounted"} {
		t.Run(scenario, func(t *testing.T) {
			f := setup(t)
			for range 2 {
				f.reconcile(t)
			}
			job := f.job(t)
			want := "ObservedExecutionSuspended"
			switch scenario {
			case "identity", "fingerprint", "unsuspended":
				want = "JobIdentityChanged"
				if scenario == "identity" {
					job.OwnerReferences[0].UID = "other-owner"
				} else if scenario == "fingerprint" {
					job.Annotations[fingerprintAnnotation] = "changed"
				} else {
					job.Spec.Suspend = ptr.To(false)
					want = "JobStartedWithoutReceipt"
				}
				if err := f.client.Update(context.Background(), job); err != nil {
					t.Fatal(err)
				}
			case "missing":
				f.reconcile(t)
				if err := f.client.Delete(context.Background(), job); err != nil {
					t.Fatal(err)
				}
				want = "IssuedJobMissing"
			default:
				switch scenario {
				case "start":
					job.Status.StartTime = ptr.To(metav1.Now())
				case "active":
					job.Status.Active = 1
				case "terminating":
					job.Status.Terminating = ptr.To[int32](1)
				case "uncounted":
					job.Status.UncountedTerminatedPods = &batchv1.UncountedTerminatedPods{Succeeded: []types.UID{"observed-pod"}}
				}
				if err := f.client.Status().Update(context.Background(), job); err != nil {
					t.Fatal(err)
				}
			}
			f.op.Cancel = true
			r := f.reconcile(t)
			if r.Phase != Unknown || r.Reason != want || want == "ObservedExecutionSuspended" && r.Job.UID != job.UID {
				t.Fatalf("unsafe Job was declared cancelled before dispatch: %+v", r)
			}
			if scenario != "missing" && !reflect.DeepEqual(job, f.job(t)) {
				t.Fatal("cancellation changed ambiguous Job")
			}
			f.op.Revision = "v2"
			if again := f.reconcile(t); !reflect.DeepEqual(again, r) {
				t.Fatalf("uncertainty was replaced: %+v", again)
			}
		})
	}
}

func TestCancellationCannotOverwriteConcurrentDispatch(t *testing.T) {
	f := setup(t)
	for range 3 {
		f.reconcile(t)
	}
	competed := false
	writer := interceptor.NewClient(f.client.(client.WithWatch), interceptor.Funcs{
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			if _, ok := obj.(*corev1.ConfigMap); ok && !competed {
				competed = true
				f.reconcile(t)
			}
			return c.Patch(ctx, obj, patch, opts...)
		},
	})
	cancel := f.op
	cancel.Cancel = true
	r, err := New(writer, f.client, "operations").Reconcile(context.Background(), f.owner, cancel)
	if err != nil || !competed || r.Phase != Pending {
		t.Fatalf("stale cancellation was committed: %+v, %v", r, err)
	}
	_, retained := f.receipt(t)
	if !retained.Dispatched || retained.Phase != Pending {
		t.Fatalf("dispatch marker was overwritten: %+v", retained)
	}
	f.op.Cancel = true
	if r := f.reconcile(t); r.Phase != Unknown || r.Reason != "AuthorizationCancelledDuringDispatch" {
		t.Fatalf("concurrent dispatch lost uncertainty: %+v", r)
	}
	if !ptr.Deref(f.job(t).Spec.Suspend, false) {
		t.Fatal("cancellation released concurrent dispatch")
	}
}

func TestInitialCancellationReadsConcurrentlyCreatedReceipt(t *testing.T) {
	f := setup(t)
	competed := false
	var job *batchv1.Job
	writer := interceptor.NewClient(f.client.(client.WithWatch), interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if _, ok := obj.(*corev1.ConfigMap); ok && !competed {
				competed = true
				job = f.start(t)
			}
			return c.Create(ctx, obj, opts...)
		},
	})
	cancel := f.op
	cancel.Cancel = true
	r, err := New(writer, f.client, "operations").Reconcile(context.Background(), f.owner, cancel)
	if err != nil || !competed || r.Phase != Pending || r.Job.UID != job.UID || r.Reason != "" {
		t.Fatalf("creation race reported a false safe cancellation: %+v, %v", r, err)
	}
	if !reflect.DeepEqual(job, f.job(t)) {
		t.Fatal("cancellation mutated concurrent running Job")
	}
}

func TestCancellationPreservesExistingUnknownEvidence(t *testing.T) {
	f := setup(t)
	job := f.start(t)
	pod := f.pod(t, job, corev1.PodRunning)
	extra := pod.DeepCopy()
	extra.Name, extra.UID, extra.ResourceVersion = pod.Name+"-other", "other-pod", ""
	if err := f.client.Create(context.Background(), extra); err != nil {
		t.Fatal(err)
	}
	unknown := f.reconcile(t)
	if unknown.Phase != Unknown || len(unknown.ObservedPods) != 2 {
		t.Fatalf("uncertainty evidence was not exercised: %+v", unknown)
	}
	f.op.Cancel, f.op.Revision = true, "v2"
	f.op.Template.Spec.Containers[0].Args = []string{"changed"}
	if err := f.client.Delete(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	if r := f.reconcile(t); !reflect.DeepEqual(r, unknown) {
		t.Fatalf("cancellation replaced existing uncertainty: %+v", r)
	}
}
