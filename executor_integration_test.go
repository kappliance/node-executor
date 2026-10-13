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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	c, err := client.New(config, client.Options{Scheme: scheme(t)})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "operations"}}
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

	for range 6 {
		var wg sync.WaitGroup
		for range 8 {
			wg.Go(func() { _, _ = f.executor.Reconcile(ctx, f.owner, f.op) })
		}
		wg.Wait()
	}
	job := f.job(t)
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
	if result := f.reconcile(t); result.Phase != Succeeded {
		t.Fatalf("%+v", result)
	}
	if err := c.Delete(ctx, job); err != nil {
		t.Fatal(err)
	}
	if result := f.reconcile(t); result.Phase != Succeeded {
		t.Fatalf("cleanup replayed operation: %+v", result)
	}
}
