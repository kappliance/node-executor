package nodeexec

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"
)

func TestInstallOnlyMountsTargetDirectory(t *testing.T) {
	pod, err := Install(Binary{Image: image, HelperImage: image, Source: "/payload/tool", Destination: "/opt/tools/tool", SHA256: strings.Repeat("a", 64)})
	if err != nil {
		t.Fatal(err)
	}
	if len(pod.Spec.InitContainers) != 1 || pod.Spec.InitContainers[0].Image != image || pod.Spec.Containers[0].Command[0] != "/tools/node-executor" {
		t.Fatal("helper must execute inside the source image")
	}
	if len(pod.Spec.InitContainers[0].VolumeMounts) != 1 || pod.Spec.InitContainers[0].VolumeMounts[0].Name != "tools" {
		t.Fatal("helper preparation has host access")
	}
	for _, volume := range pod.Spec.Volumes {
		if volume.HostPath != nil && (volume.HostPath.Path != "/opt/tools" || *volume.HostPath.Type != corev1.HostPathDirectory) {
			t.Fatalf("unexpected host mount: %+v", volume)
		}
	}
	if *pod.Spec.AutomountServiceAccountToken || *pod.Spec.Containers[0].SecurityContext.AllowPrivilegeEscalation {
		t.Fatal("unexpected authority")
	}
}

func TestContainerRestartOverridesAreRejected(t *testing.T) {
	f := setup(t)
	f.op.Template.Spec.Containers[0].RestartPolicy = ptr.To(corev1.ContainerRestartPolicyAlways)
	if _, err := normalize(f.op); err == nil {
		t.Fatal("per-container retries accepted")
	}
	f.op.Template.Spec.Containers[0].RestartPolicy = nil
	f.op.Template.Spec.InitContainers = []corev1.Container{{Name: "sidecar", Image: image, RestartPolicy: ptr.To(corev1.ContainerRestartPolicyAlways)}}
	if _, err := normalize(f.op); err == nil {
		t.Fatal("restarting sidecar accepted")
	}
}

func TestSuppliedEmptyAffinityTermsAreRejected(t *testing.T) {
	for _, terms := range [][]corev1.NodeSelectorTerm{nil, {{}}, {{}, {MatchExpressions: []corev1.NodeSelectorRequirement{{Key: "pool", Operator: corev1.NodeSelectorOpIn, Values: []string{"approved"}}}}}} {
		f := setup(t)
		f.op.Template.Spec.Affinity = &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{NodeSelectorTerms: terms}}}
		if _, err := normalize(f.op); err == nil {
			t.Fatal("empty affinity broadened caller's node selection")
		}
	}
}

func TestInvalidInstallInputs(t *testing.T) {
	valid := Binary{Image: image, HelperImage: image, Source: "/payload/tool", Destination: "/opt/tools/tool", SHA256: strings.Repeat("a", 64)}
	for _, mutate := range []func(*Binary){func(b *Binary) { b.Image = "repo:latest" }, func(b *Binary) { b.HelperImage = "repo:latest" }, func(b *Binary) { b.SHA256 = "bad" }, func(b *Binary) { b.PreviousSHA256 = "bad" }, func(b *Binary) { b.Destination = "/opt/../etc/tool" }, func(b *Binary) { b.Source = "../tool" }} {
		b := valid
		mutate(&b)
		if _, err := Install(b); err == nil {
			t.Fatalf("accepted %+v", b)
		}
	}
}

func TestTargetConstraintIntersectsEveryAffinityTerm(t *testing.T) {
	f := setup(t)
	f.op.Template.Spec.Affinity = &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{{Key: "pool", Operator: corev1.NodeSelectorOpIn, Values: []string{"one"}}}}, {MatchExpressions: []corev1.NodeSelectorRequirement{{Key: "pool", Operator: corev1.NodeSelectorOpIn, Values: []string{"two"}}}}}}}}
	pod, err := normalize(f.op)
	if err != nil {
		t.Fatal(err)
	}
	for _, term := range pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms {
		if len(term.MatchFields) != 1 || term.MatchFields[0].Values[0] != "worker" || len(term.MatchExpressions) != 1 {
			t.Fatalf("target constraint lost: %+v", term)
		}
	}
	if len(f.op.Template.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0].MatchFields) != 0 {
		t.Fatal("caller template was modified")
	}
}
