// Package controller demonstrates integrating Node Executor into a reconciler.
package controller

import (
	"context"
	"time"

	nodeexec "github.com/kappliance/node-executor"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ReconcileWork assumes the caller has retained its owner's finalizer and chosen
// the node, authorization, revision, image, and explicit execution permissions.
func ReconcileWork(ctx context.Context, executor *nodeexec.Executor, owner client.Object, operation nodeexec.Operation) (ctrl.Result, nodeexec.Result, error) {
	run, err := executor.Reconcile(ctx, owner, operation)
	if err != nil {
		return ctrl.Result{}, run, err
	}
	if run.Phase == nodeexec.Unknown {
		// The caller persists a condition and resolves the external effects.
		return ctrl.Result{}, run, nil
	}
	if run.Revision != operation.Revision {
		if run.Done() {
			return ctrl.Result{RequeueAfter: time.Second}, run, nil
		}
		return run.Result, run, nil
	}
	// The caller maps the transport result to status and verifies host state.
	return run.Result, run, nil
}
