package nodeexec

import (
	"time"

	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
)

// Operation identifies one logical operation. Change Revision for new desired work;
// keep Name stable so an unresolved predecessor blocks a newer revision.
type Operation struct {
	Name     string
	Revision string
	Node     corev1.ObjectReference
	Template corev1.PodTemplateSpec
}

type Phase string

const (
	Pending   Phase = "Pending"
	Running   Phase = "Running"
	Succeeded Phase = "Succeeded"
	Failed    Phase = "Failed"
	Unknown   Phase = "Unknown"
)

// Result describes transport execution, not proof of external host commit.
// Revision may be an unresolved predecessor of the requested revision.
type Result struct {
	Phase    Phase
	Revision string
	Reason   string
	Job      corev1.ObjectReference
	Pod      corev1.ObjectReference
	Result   ctrl.Result
}

func (r Result) Done() bool { return r.Phase == Succeeded || r.Phase == Failed }

const pollInterval = 5 * time.Second
