package nodeexec

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

const image = "registry.example.com/component@sha256:1111111111111111111111111111111111111111111111111111111111111111"

type fixture struct {
	client   client.Client
	executor *Executor
	owner    *appsv1.Deployment
	op       Operation
}

func scheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, batchv1.AddToScheme, appsv1.AddToScheme} {
		if err := add(s); err != nil {
			t.Fatal(err)
		}
	}
	return s
}

func setup(t *testing.T) fixture {
	t.Helper()
	owner := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "owner", Namespace: "operations", UID: "owner-1", Generation: 1, Finalizers: []string{"example.com/retained-effects"}}}
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker", UID: "node-1"}}
	c := fake.NewClientBuilder().WithScheme(scheme(t)).WithStatusSubresource(&batchv1.Job{}, &corev1.Pod{}).WithObjects(owner, node).WithInterceptorFuncs(interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if obj.GetUID() == "" {
				obj.SetUID(types.UID("uid-" + obj.GetName()))
			}
			return c.Create(ctx, obj, opts...)
		},
	}).Build()
	return fixture{client: c, executor: New(c, c, "operations"), owner: owner, op: Operation{Name: "probe", Revision: "v1", Node: corev1.ObjectReference{Name: node.Name, UID: node.UID}, Template: Command(image, "/probe")}}
}

func (f fixture) reconcile(t *testing.T) Result {
	t.Helper()
	r, err := f.executor.Reconcile(context.Background(), f.owner, f.op)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func (f fixture) job(t *testing.T) *batchv1.Job {
	t.Helper()
	var jobs batchv1.JobList
	if err := f.client.List(context.Background(), &jobs); err != nil || len(jobs.Items) != 1 {
		t.Fatalf("jobs=%d err=%v", len(jobs.Items), err)
	}
	return &jobs.Items[0]
}

func (f fixture) start(t *testing.T) *batchv1.Job {
	t.Helper()
	for range 8 {
		f.reconcile(t)
		var jobs batchv1.JobList
		if err := f.client.List(context.Background(), &jobs); err != nil {
			t.Fatal(err)
		}
		if len(jobs.Items) == 1 && jobs.Items[0].Spec.Suspend != nil && !*jobs.Items[0].Spec.Suspend {
			return &jobs.Items[0]
		}
	}
	t.Fatal("operation never started")
	return nil
}

func (f fixture) pod(t *testing.T, job *batchv1.Job, phase corev1.PodPhase) *corev1.Pod {
	t.Helper()
	p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: job.Name + "-pod", Namespace: job.Namespace, Labels: map[string]string{"batch.kubernetes.io/controller-uid": string(job.UID)}, OwnerReferences: []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "Job", Name: job.Name, UID: job.UID, Controller: ptr.To(true)}}}, Spec: *job.Spec.Template.Spec.DeepCopy(), Status: corev1.PodStatus{Phase: phase}}
	p.Spec.NodeName = f.op.Node.Name
	if err := f.client.Create(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestDispatchPersistsIdentityBeforeStart(t *testing.T) {
	f := setup(t)
	r := f.reconcile(t)
	if r.Phase != Pending || r.Result.RequeueAfter <= 0 {
		t.Fatalf("%+v", r)
	}
	f.reconcile(t)
	job := f.job(t)
	if !*job.Spec.Suspend || *job.Spec.BackoffLimit != 0 {
		t.Fatal("job must be suspended without retries")
	}
	f.reconcile(t)
	var cm corev1.ConfigMap
	if err := f.client.Get(context.Background(), client.ObjectKey{Namespace: "operations", Name: ReceiptName(f.owner.UID, f.op.Node.UID, f.op.Name)}, &cm); err != nil {
		t.Fatal(err)
	}
	rcp, err := decodeReceipt(&cm, receipt{Name: f.op.Name, OwnerUID: f.owner.UID, Node: f.op.Node}, *metav1.GetControllerOf(&cm))
	if err != nil || rcp.JobUID != job.UID || rcp.Dispatched {
		t.Fatalf("receipt=%+v err=%v", rcp, err)
	}
	f.reconcile(t)
	if !*f.job(t).Spec.Suspend {
		t.Fatal("dispatch marker must precede unsuspend")
	}
	f.reconcile(t)
	if *f.job(t).Spec.Suspend {
		t.Fatal("job was not released")
	}
}

func TestCompletedCommandRetainsReceiptAfterCleanup(t *testing.T) {
	f := setup(t)
	job := f.start(t)
	pod := f.pod(t, job, corev1.PodSucceeded)
	f.reconcile(t)
	r := f.reconcile(t)
	if r.Phase != Succeeded || !r.Done() || r.Pod.UID != pod.UID || r.Result.RequeueAfter != 0 {
		t.Fatalf("%+v", r)
	}
	if err := f.client.Delete(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	if err := f.client.Delete(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	if r := f.reconcile(t); r.Phase != Succeeded {
		t.Fatalf("retained outcome: %+v", r)
	}
	var jobs batchv1.JobList
	_ = f.client.List(context.Background(), &jobs)
	if len(jobs.Items) != 0 {
		t.Fatal("completed operation was replayed")
	}
}

func TestLostIssuedJobBlocksReplayAndNewRevision(t *testing.T) {
	f := setup(t)
	job := f.start(t)
	if err := f.client.Delete(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	if r := f.reconcile(t); r.Phase != Unknown || r.Reason != "IssuedJobMissing" {
		t.Fatalf("%+v", r)
	}
	f.op.Revision = "v2"
	for range 3 {
		if r := f.reconcile(t); r.Phase != Unknown || r.Revision != "v1" {
			t.Fatalf("%+v", r)
		}
	}
	var jobs batchv1.JobList
	_ = f.client.List(context.Background(), &jobs)
	if len(jobs.Items) != 0 {
		t.Fatal("unknown operation was replayed")
	}
}

func TestChangedRevisionWaitsForPreviousProcess(t *testing.T) {
	f := setup(t)
	job := f.start(t)
	pod := f.pod(t, job, corev1.PodRunning)
	f.reconcile(t)
	f.op.Revision = "v2"
	r := f.reconcile(t)
	if r.Revision != "v1" || r.Phase != Running {
		t.Fatalf("%+v", r)
	}
	pod.Status.Phase = corev1.PodSucceeded
	if err := f.client.Status().Update(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	if r := f.reconcile(t); r.Revision != "v1" || r.Phase != Succeeded {
		t.Fatalf("%+v", r)
	}
	if r := f.reconcile(t); r.Revision != "v2" || r.Phase != Pending {
		t.Fatalf("%+v", r)
	}
}

func TestChangingTemplateRequiresNewRevision(t *testing.T) {
	f := setup(t)
	f.reconcile(t)
	f.op.Template.Spec.Containers[0].Args = []string{"changed"}
	if _, err := f.executor.Reconcile(context.Background(), f.owner, f.op); err == nil {
		t.Fatal("unchanged revision accepted a different template")
	}
}

func TestNodeReplacementAndStaleOwnerNeverStart(t *testing.T) {
	t.Run("node", func(t *testing.T) {
		f := setup(t)
		f.reconcile(t)
		f.reconcile(t)
		var node corev1.Node
		_ = f.client.Get(context.Background(), client.ObjectKey{Name: "worker"}, &node)
		node.UID = "node-replacement"
		if err := f.client.Update(context.Background(), &node); err != nil {
			t.Fatal(err)
		}
		f.reconcile(t)
		if _, err := f.executor.Reconcile(context.Background(), f.owner, f.op); err == nil {
			t.Fatal("replacement accepted")
		}
		if !*f.job(t).Spec.Suspend {
			t.Fatal("job started on a replacement")
		}
	})
	t.Run("owner", func(t *testing.T) {
		f := setup(t)
		f.reconcile(t)
		f.reconcile(t)
		live := f.owner.DeepCopy()
		_ = f.client.Get(context.Background(), client.ObjectKeyFromObject(live), live)
		live.Generation++
		if err := f.client.Update(context.Background(), live); err != nil {
			t.Fatal(err)
		}
		if _, err := f.executor.Reconcile(context.Background(), f.owner, f.op); err == nil {
			t.Fatal("stale owner accepted")
		}
		if !*f.job(t).Spec.Suspend {
			t.Fatal("stale owner started a job")
		}
	})
}

func TestReplacementAfterDispatchIsUnknown(t *testing.T) {
	f := setup(t)
	f.start(t)
	var node corev1.Node
	_ = f.client.Get(context.Background(), client.ObjectKey{Name: "worker"}, &node)
	node.UID = "replacement"
	_ = f.client.Update(context.Background(), &node)
	if r := f.reconcile(t); r.Phase != Unknown {
		t.Fatalf("%+v", r)
	}
}

func TestPodReplacementAndMultiplePodsAreUnknown(t *testing.T) {
	for _, multiple := range []bool{false, true} {
		t.Run(map[bool]string{false: "replacement", true: "multiple"}[multiple], func(t *testing.T) {
			f := setup(t)
			job := f.start(t)
			pod := f.pod(t, job, corev1.PodRunning)
			f.reconcile(t)
			if multiple {
				other := pod.DeepCopy()
				other.Name += "-second"
				other.UID = "second"
				other.ResourceVersion = ""
				if err := f.client.Create(context.Background(), other); err != nil {
					t.Fatal(err)
				}
			} else {
				pod.UID = "replacement"
				if err := f.client.Update(context.Background(), pod); err != nil {
					t.Fatal(err)
				}
			}
			if r := f.reconcile(t); r.Phase != Unknown {
				t.Fatalf("%+v", r)
			}
		})
	}
}

func TestAmbiguousUnsuspendReplyDoesNotCreateAnotherJob(t *testing.T) {
	f := setup(t)
	var once sync.Once
	f.executor.client = interceptor.NewClient(f.client.(client.WithWatch), interceptor.Funcs{
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			err := c.Patch(ctx, obj, patch, opts...)
			if err != nil {
				return err
			}
			if _, ok := obj.(*batchv1.Job); ok {
				once.Do(func() { err = errors.New("reply lost after accepted patch") })
			}
			return err
		},
	})
	var failed bool
	for range 7 {
		_, err := f.executor.Reconcile(context.Background(), f.owner, f.op)
		failed = failed || err != nil
	}
	if !failed || *f.job(t).Spec.Suspend {
		t.Fatal("lost reply was not exercised")
	}
	if r := f.reconcile(t); r.Phase == Unknown {
		t.Fatalf("accepted job became unknown: %+v", r)
	}
}

func TestConcurrentReconcileCreatesOneJob(t *testing.T) {
	f := setup(t)
	for range 5 {
		var wg sync.WaitGroup
		for range 10 {
			wg.Add(1)
			go func() { defer wg.Done(); _, _ = f.executor.Reconcile(context.Background(), f.owner, f.op) }()
		}
		wg.Wait()
	}
	job := f.job(t)
	if *job.Spec.Suspend {
		t.Fatal("concurrent reconciliation did not start the job")
	}
}

func TestDeletingOwnerCannotDispatch(t *testing.T) {
	f := setup(t)
	f.reconcile(t)
	f.reconcile(t)
	if err := f.client.Delete(context.Background(), f.owner); err != nil {
		t.Fatal(err)
	}
	if r := f.reconcile(t); r.Phase != Failed {
		t.Fatalf("%+v", r)
	}
	if !*f.job(t).Spec.Suspend {
		t.Fatal("deleting owner dispatched an operation")
	}
}

func TestDeletingReceiptCannotDispatch(t *testing.T) {
	for _, dispatched := range []bool{false, true} {
		t.Run(map[bool]string{false: "before", true: "after"}[dispatched], func(t *testing.T) {
			f := setup(t)
			if dispatched {
				f.start(t)
			} else {
				f.reconcile(t)
				f.reconcile(t)
			}
			var cm corev1.ConfigMap
			if err := f.client.Get(context.Background(), client.ObjectKey{Namespace: "operations", Name: ReceiptName(f.owner.UID, f.op.Node.UID, f.op.Name)}, &cm); err != nil {
				t.Fatal(err)
			}
			cm.Finalizers = []string{"example.com/retain"}
			if err := f.client.Update(context.Background(), &cm); err != nil {
				t.Fatal(err)
			}
			if err := f.client.Delete(context.Background(), &cm); err != nil {
				t.Fatal(err)
			}
			r, err := f.executor.Reconcile(context.Background(), f.owner, f.op)
			if dispatched {
				if err != nil || r.Phase != Unknown {
					t.Fatalf("result=%+v err=%v", r, err)
				}
			} else if err == nil || !*f.job(t).Spec.Suspend {
				t.Fatal("deleting receipt authorized a job")
			}
		})
	}
}

func TestInvalidReceiptFailsClosed(t *testing.T) {
	for _, invalid := range []string{"unknown-field", "trailing-json", "inconsistent-evidence", "invalid-owner"} {
		t.Run(invalid, func(t *testing.T) {
			f := setup(t)
			f.reconcile(t)
			var cm corev1.ConfigMap
			if err := f.client.Get(context.Background(), client.ObjectKey{Namespace: "operations", Name: ReceiptName(f.owner.UID, f.op.Node.UID, f.op.Name)}, &cm); err != nil {
				t.Fatal(err)
			}
			switch invalid {
			case "unknown-field":
				cm.Data["receipt.json"] = cm.Data["receipt.json"][:len(cm.Data["receipt.json"])-1] + `,"extra":true}`
			case "trailing-json":
				cm.Data["receipt.json"] += "{}"
			case "inconsistent-evidence":
				var r receipt
				if err := json.Unmarshal([]byte(cm.Data["receipt.json"]), &r); err != nil {
					t.Fatal(err)
				}
				r.Dispatched = true
				data, err := json.Marshal(r)
				if err != nil {
					t.Fatal(err)
				}
				cm.Data["receipt.json"] = string(data)
			case "invalid-owner":
				cm.OwnerReferences[0].UID = "other"
			}
			if err := f.client.Update(context.Background(), &cm); err != nil {
				t.Fatal(err)
			}
			if _, err := f.executor.Reconcile(context.Background(), f.owner, f.op); err == nil {
				t.Fatal("invalid receipt authorized dispatch")
			}
			var jobs batchv1.JobList
			if err := f.client.List(context.Background(), &jobs); err != nil || len(jobs.Items) != 0 {
				t.Fatalf("jobs=%d err=%v", len(jobs.Items), err)
			}
		})
	}
}

func TestRollbackRevisionGetsNewExecutionIdentity(t *testing.T) {
	f := setup(t)
	var previous *batchv1.Job
	for _, revision := range []string{"v1", "v2", "v1"} {
		f.op.Revision = revision
		var job batchv1.Job
		for range 8 {
			r := f.reconcile(t)
			if r.Revision != revision || r.Job.UID == "" {
				continue
			}
			if err := f.client.Get(context.Background(), client.ObjectKey{Namespace: r.Job.Namespace, Name: r.Job.Name}, &job); err != nil {
				t.Fatal(err)
			}
			if job.Spec.Suspend != nil && !*job.Spec.Suspend {
				break
			}
		}
		if job.UID == "" || job.Spec.Suspend == nil || *job.Spec.Suspend || previous != nil && previous.Name == job.Name {
			t.Fatalf("revision %s reused execution: %+v", revision, job)
		}
		f.pod(t, &job, corev1.PodSucceeded)
		f.reconcile(t)
		if r := f.reconcile(t); r.Phase != Succeeded {
			t.Fatalf("%+v", r)
		}
		previous = job.DeepCopy()
	}
}

func TestNodeReferenceIgnoresObservedResourceVersion(t *testing.T) {
	f := setup(t)
	f.op.Node.APIVersion, f.op.Node.Kind, f.op.Node.ResourceVersion = "v1", "Node", "123"
	f.reconcile(t)
	f.op.Node.ResourceVersion = "124"
	f.reconcile(t)
}
