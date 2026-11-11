package nodeexec

import (
	"context"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestUnknownFencesInFlightSuccessorRelease(t *testing.T) {
	testUnknownFencesInFlightSuccessorRelease(t, setup(t))
}

func TestUnknownWinsBeforeDispatchClaim(t *testing.T) {
	testUnknownWinsBeforeDispatchClaim(t, setup(t))
}

func testUnknownWinsBeforeDispatchClaim(t *testing.T, f fixture) {
	t.Helper()
	for range 4 {
		f.reconcile(t)
	}
	cm, evidence := f.receipt(t)
	interleaved := false
	reader := interceptor.NewClient(f.client.(client.WithWatch), interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*corev1.Node); ok && !interleaved {
				interleaved = true
				if result, err := f.executor.unknown(ctx, &cm, evidence, "PrecedingUncertainty"); err != nil || result.Phase != Unknown {
					t.Fatalf("uncertainty was not committed: %+v, %v", result, err)
				}
			}
			return c.Get(ctx, key, obj, opts...)
		},
	})
	result, err := New(f.client, reader, cm.Namespace).Reconcile(context.Background(), f.owner, f.op)
	if !interleaved || err != nil || result.Phase != Unknown || result.Reason != "PrecedingUncertainty" {
		t.Fatalf("in-flight dispatch ignored uncertainty: %+v, %v", result, err)
	}
	if !ptr.Deref(f.job(t).Spec.Suspend, false) {
		t.Fatal("uncertainty-first dispatch released Job")
	}
}

func testUnknownFencesInFlightSuccessorRelease(t *testing.T, f fixture) {
	t.Helper()
	ctx := context.Background()
	firstJob := f.start(t)
	pod := f.pod(t, firstJob, corev1.PodPending)
	pod.Status.Phase = corev1.PodSucceeded
	if err := f.client.Status().Update(ctx, pod); err != nil {
		t.Fatal(err)
	}
	f.reconcile(t)
	stale, original := f.receipt(t)
	extra := pod.DeepCopy()
	extra.Name, extra.UID, extra.ResourceVersion = pod.Name+"-other", "", ""
	if err := f.client.Create(ctx, extra); err != nil {
		t.Fatal(err)
	}
	for _, observed := range []*corev1.Pod{pod, extra} {
		original.ObservedPods = append(original.ObservedPods, corev1.ObjectReference{APIVersion: "v1", Kind: "Pod", Namespace: observed.Namespace, Name: observed.Name, UID: observed.UID})
	}
	if err := f.client.Delete(ctx, extra, client.GracePeriodSeconds(0)); err != nil {
		t.Fatal(err)
	}
	if result := f.reconcile(t); result.Phase != Succeeded {
		t.Fatalf("competing success was not persisted: %+v", result)
	}
	f.op.Revision = "v2"
	for range 4 {
		f.reconcile(t)
	}
	current, dispatched := f.receipt(t)
	if dispatched.Revision != "v2" || !dispatched.Dispatched || dispatched.JobUID == "" {
		t.Fatalf("successor is not dispatch-marked: %+v", dispatched)
	}
	var successor batchv1.Job
	if err := f.client.Get(ctx, client.ObjectKey{Namespace: current.Namespace, Name: dispatched.JobName}, &successor); err != nil || !ptr.Deref(successor.Spec.Suspend, false) {
		t.Fatalf("successor was not suspended: %v", err)
	}
	unknownWriter := New(f.client, f.client, current.Namespace)
	var uncertainty Result
	interleaved := false
	writer := interceptor.NewClient(f.client.(client.WithWatch), interceptor.Funcs{
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			if _, ok := obj.(*batchv1.Job); ok {
				interleaved = true
				var err error
				uncertainty, err = unknownWriter.unknown(ctx, &stale, original, "MultipleExecutionPods")
				if err != nil {
					return err
				}
			}
			return c.Patch(ctx, obj, patch, opts...)
		},
	})
	_, releaseErr := New(writer, f.client, current.Namespace).Reconcile(ctx, f.owner, f.op)
	if !interleaved {
		t.Fatal("final Job mutation boundary was not exercised")
	}
	if err := f.client.Get(ctx, client.ObjectKeyFromObject(&successor), &successor); err != nil {
		t.Fatal(err)
	}
	if uncertainty.Phase == Unknown && !ptr.Deref(successor.Spec.Suspend, false) {
		t.Fatalf("successor released after durable uncertainty: %+v; release error: %v", uncertainty, releaseErr)
	}
	// A launch which already held the serialization boundary may finish first.
	result, err := unknownWriter.unknown(ctx, &stale, original, "MultipleExecutionPods")
	if err != nil || result.Phase != Unknown || result.Revision != "v1" || len(result.ObservedPods) != 2 {
		t.Fatalf("original uncertainty was lost: %+v, %v", result, err)
	}
	for _, revision := range []string{"v1", "v2", "v3"} {
		f.op.Revision = revision
		if result := f.reconcile(t); result.Phase != Unknown || result.Revision != "v1" || len(result.ObservedPods) != 2 {
			t.Fatalf("uncertainty did not block revision %s: %+v", revision, result)
		}
	}
}
