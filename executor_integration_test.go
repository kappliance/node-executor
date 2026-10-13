//go:build integration

package nodeexec

import (
	"context"
	"os"
	"sync"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

// Envtest runs the real API server's validation, UID allocation, and CAS writes.
// It has no scheduler, Job controller, kubelet, or host activation.
func TestAPIDispatchAndReceiptConflict(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Fatal("set KUBEBUILDER_ASSETS to Kubernetes envtest binaries")
	}
	environment := &envtest.Environment{}
	config, err := environment.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := environment.Stop(); err != nil {
			t.Error(err)
		}
	})
	c, err := client.NewWithWatch(config, client.Options{Scheme: scheme(t)})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "operations", Labels: map[string]string{"pod-security.kubernetes.io/enforce": "restricted", "pod-security.kubernetes.io/enforce-version": "v1.35"}}}
	if err := c.Create(ctx, ns); err != nil {
		t.Fatal(err)
	}
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker"}}
	if err := c.Create(ctx, node); err != nil {
		t.Fatal(err)
	}
	owner := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "owner", Namespace: ns.Name, Finalizers: []string{"example.com/retained-effects"}},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "owner"}},
			Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "owner"}}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "owner", Image: image}}}},
		},
	}
	if err := c.Create(ctx, owner); err != nil {
		t.Fatal(err)
	}
	f := fixture{client: c, executor: New(c, c, ns.Name), owner: owner, op: Operation{Name: "probe", Revision: "v1", Node: corev1.ObjectReference{Name: node.Name, UID: node.UID}, Template: Command(image, "/probe")}}
	f.reconcile(t)
	key := client.ObjectKey{Namespace: ns.Name, Name: ReceiptName(owner.UID, node.UID, f.op.Name)}
	var first, stale corev1.ConfigMap
	if err := c.Get(ctx, key, &first); err != nil {
		t.Fatal(err)
	}
	first.DeepCopyInto(&stale)
	r, err := decodeReceipt(&first, receipt{Name: f.op.Name, OwnerUID: owner.UID, Node: f.op.Node}, *metav1.GetControllerOf(&first))
	if err != nil {
		t.Fatal(err)
	}
	r.Reason = "CurrentWriter"
	if _, err := f.executor.save(ctx, &first, r); err != nil {
		t.Fatal(err)
	}
	r.Reason = "StaleWriter"
	if _, err := f.executor.save(ctx, &stale, r); err != nil {
		t.Fatal(err)
	}
	var current corev1.ConfigMap
	if err := c.Get(ctx, key, &current); err != nil {
		t.Fatal(err)
	}
	r, err = decodeReceipt(&current, receipt{Name: f.op.Name, OwnerUID: owner.UID, Node: f.op.Node}, *metav1.GetControllerOf(&current))
	if err != nil || r.Reason != "CurrentWriter" {
		t.Fatalf("stale writer replaced receipt: %+v, %v", r, err)
	}

	f.reconcile(t)
	suspended := f.job(t).DeepCopy()
	for range 6 {
		var wg sync.WaitGroup
		for range 8 {
			wg.Go(func() { _, _ = f.executor.Reconcile(ctx, f.owner, f.op) })
		}
		wg.Wait()
	}
	job := f.job(t)
	if err := c.Delete(ctx, suspended, client.Preconditions{UID: &suspended.UID, ResourceVersion: &suspended.ResourceVersion}); !apierrors.IsConflict(err) {
		t.Fatalf("stale suspended-Job deletion was accepted: %v", err)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(job), job); err != nil {
		t.Fatal(err)
	}
	if job.Spec.Suspend == nil || *job.Spec.Suspend || *job.Spec.BackoffLimit != 0 || *job.Spec.PodReplacementPolicy != batchv1.Failed {
		t.Fatalf("unexpected job: %+v", job.Spec)
	}
	if err := c.Get(ctx, key, &current); err != nil {
		t.Fatal(err)
	}
	r, err = decodeReceipt(&current, receipt{Name: f.op.Name, OwnerUID: owner.UID, Node: f.op.Node}, *metav1.GetControllerOf(&current))
	if err != nil || !r.Dispatched || r.JobUID != job.UID {
		t.Fatalf("job released without durable identity: %+v, %v", r, err)
	}

	// Directly created Pod status is simulated; admission and resource writes are real.
	pod := f.pod(t, job, corev1.PodPending)
	pod.Status.Phase = corev1.PodSucceeded
	if err := c.Status().Update(ctx, pod); err != nil {
		t.Fatal(err)
	}
	f.reconcile(t)
	var beforeCompletion corev1.ConfigMap
	if err := c.Get(ctx, key, &beforeCompletion); err != nil {
		t.Fatal(err)
	}
	observed, err := decodeReceipt(&beforeCompletion, receipt{Name: f.op.Name, OwnerUID: owner.UID, Node: f.op.Node}, *metav1.GetControllerOf(&beforeCompletion))
	if err != nil {
		t.Fatal(err)
	}
	if result := f.reconcile(t); result.Phase != Succeeded {
		t.Fatalf("%+v", result)
	}
	if err := c.Delete(ctx, job); err != nil {
		t.Fatal(err)
	}
	if result := f.reconcile(t); result.Phase != Succeeded {
		t.Fatalf("cleanup replayed operation: %+v", result)
	}
	if result, err := f.executor.unknown(ctx, &beforeCompletion, observed, "LateUncertainty"); err != nil || result.Phase != Unknown {
		t.Fatalf("stale observation was discarded: %+v, %v", result, err)
	}
	f.op.Revision = "v2"
	if result := f.reconcile(t); result.Phase != Unknown || result.Revision != "v1" {
		t.Fatalf("uncertainty was not authoritative: %+v", result)
	}

	// A distinct operation exercises suspended execution evidence before UID binding.
	f.op.Name = "suspension-check"
	var pending Result
	for range 2 {
		pending = f.reconcile(t)
	}
	suspendedExecution := &batchv1.Job{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: pending.Job.Namespace, Name: pending.Job.Name}, suspendedExecution); err != nil {
		t.Fatal(err)
	}
	suspendedExecution.Status.Terminating = ptr.To[int32](1)
	if err := c.Status().Update(ctx, suspendedExecution); err != nil {
		t.Fatal(err)
	}
	f.op.Revision = "v3"
	if result := f.reconcile(t); result.Phase != Unknown || result.Job.UID != suspendedExecution.UID {
		t.Fatalf("executed suspended Job was superseded: %+v", result)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(suspendedExecution), suspendedExecution); err != nil || !*suspendedExecution.Spec.Suspend {
		t.Fatalf("suspended predecessor was removed or released: %v", err)
	}

	t.Run("cancellation", func(t *testing.T) {
		for steps, stage := range []string{"initial", "receipt-only", "unbound-job", "bound-job", "dispatch-marked"} {
			t.Run(stage, func(t *testing.T) {
				cancelled := f
				cancelled.op.Name, cancelled.op.Revision = "cancel-"+stage, "v1"
				var before Result
				for range steps {
					before = cancelled.reconcile(t)
				}
				var job batchv1.Job
				if steps >= 2 {
					if err := c.Get(ctx, client.ObjectKey{Namespace: ns.Name, Name: before.Job.Name}, &job); err != nil {
						t.Fatal(err)
					}
				}
				cancelled.op.Cancel = true
				result := cancelled.reconcile(t)
				want, reason := Failed, "CancelledBeforeDispatch"
				if steps == 4 {
					want, reason = Unknown, "AuthorizationCancelledDuringDispatch"
				}
				if result.Phase != want || result.Reason != reason || result.Revision != "v1" {
					t.Fatalf("unexpected cancellation: %+v", result)
				}
				var after batchv1.Job
				jobErr := c.Get(ctx, client.ObjectKey{Namespace: ns.Name, Name: result.Job.Name}, &after)
				if steps < 2 {
					if !apierrors.IsNotFound(jobErr) {
						t.Fatalf("cancelled operation created a Job: %v", jobErr)
					}
				} else if jobErr != nil || after.UID != job.UID || after.ResourceVersion != job.ResourceVersion || !ptr.Deref(after.Spec.Suspend, false) || result.Job.UID != job.UID {
					t.Fatalf("cancellation modified Job or lost identity: %+v, %v", result, jobErr)
				}
				cancelled.op.Revision = "v2"
				cancelled.op.Template = Command(image, "/different-command")
				if result := cancelled.reconcile(t); result.Phase != want || result.Reason != reason || result.Revision != "v1" {
					t.Fatalf("cancelled revision replaced original receipt: %+v", result)
				}
			})
		}
	})
	t.Run("late-job-cancellation", func(t *testing.T) {
		for _, cancel := range []bool{false, true} {
			t.Run(map[bool]string{false: "normal", true: "cancel"}[cancel], func(t *testing.T) {
				cancelled := f
				cancelled.op.Name, cancelled.op.Revision = "late-job-"+map[bool]string{false: "normal", true: "cancel"}[cancel], "v1"
				cancelled.reconcile(t)
				job := cancelled.createJobAfterCancellation(t)
				_, before := cancelled.receipt(t)
				cancelled.op.Cancel, cancelled.op.Revision = cancel, "v2"
				cancelled.op.Template = Command(image, "/new-command")
				result := cancelled.reconcile(t)
				_, bound := cancelled.receipt(t)
				var after batchv1.Job
				jobErr := c.Get(ctx, client.ObjectKeyFromObject(job), &after)
				if result.Phase != Failed || result.Reason != "CancelledBeforeDispatch" || result.Revision != "v1" || result.Job.UID != job.UID || bound.JobUID != job.UID || bound.Fingerprint != before.Fingerprint || bound.Sequence != before.Sequence || jobErr != nil || after.ResourceVersion != job.ResourceVersion || !ptr.Deref(after.Spec.Suspend, false) {
					t.Fatalf("late Job was not bound without replay or replacement: %+v, %v", result, jobErr)
				}
				if err := c.Delete(ctx, job, client.Preconditions{UID: &result.Job.UID}); err != nil {
					t.Fatal(err)
				}
				cancelled.op.Cancel = true
				if again := cancelled.reconcile(t); again.Phase != Failed || again.Reason != result.Reason || again.Job.UID != result.Job.UID || again.Revision != "v1" {
					t.Fatalf("cleanup replaced terminal cancellation: %+v", again)
				}
			})
		}
	})
}
