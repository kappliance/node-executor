package nodeexec

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/utils/ptr"
)

const recordLabel = "node-executor.kappliance.io/record"
const fingerprintAnnotation = "node-executor.kappliance.io/fingerprint"

// Command runs an argv array directly. Edit the returned native template to add
// volumes, environment, resources, or explicitly approved host privileges.
func Command(image string, argv ...string) corev1.PodTemplateSpec {
	return corev1.PodTemplateSpec{Spec: corev1.PodSpec{
		RestartPolicy:                corev1.RestartPolicyNever,
		AutomountServiceAccountToken: ptr.To(false),
		Containers: []corev1.Container{{Name: "execute", Image: image, Command: argv,
			SecurityContext: &corev1.SecurityContext{
				RunAsUser: ptr.To[int64](65532), RunAsNonRoot: ptr.To(true),
				AllowPrivilegeEscalation: ptr.To(false), ReadOnlyRootFilesystem: ptr.To(true),
				Capabilities:   &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
				SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
			},
		}},
	}}
}

// Binary describes one binary in an immutable container image. HelperImage is
// the image built from this project's Dockerfile. Both images must be digest-pinned.
type Binary struct {
	Image          string
	HelperImage    string
	Source         string
	Destination    string
	SHA256         string
	PreviousSHA256 string
}

// Install constructs a template that works with source images without a shell.
// The target directory must already exist; it is the only host path mounted.
func Install(binary Binary) (corev1.PodTemplateSpec, error) {
	if !pinnedImage(binary.Image) || !pinnedImage(binary.HelperImage) {
		return corev1.PodTemplateSpec{}, errors.New("source and helper images must be digest-pinned")
	}
	if !digest(binary.SHA256) || (binary.PreviousSHA256 != "" && !digest(binary.PreviousSHA256)) {
		return corev1.PodTemplateSpec{}, errors.New("binary digests must be lowercase SHA-256 hex")
	}
	if !filepath.IsAbs(binary.Source) || filepath.Clean(binary.Source) != binary.Source || binary.Source == "/" || strings.ContainsAny(binary.Source, "\x00\\") || binary.Source == "/tools" || strings.HasPrefix(binary.Source, "/tools/") || binary.Source == "/target" || strings.HasPrefix(binary.Source, "/target/") {
		return corev1.PodTemplateSpec{}, errors.New("source must be a clean absolute image path")
	}
	if !filepath.IsAbs(binary.Destination) || filepath.Clean(binary.Destination) != binary.Destination || filepath.Dir(binary.Destination) == "/" || filepath.Base(binary.Destination) == ".node-executor.lock" || strings.ContainsAny(binary.Destination, "\x00\\") {
		return corev1.PodTemplateSpec{}, errors.New("destination must be a clean absolute host file path")
	}
	args := []string{"install", "--source=" + binary.Source, "--directory=/target", "--name=" + filepath.Base(binary.Destination), "--sha256=" + binary.SHA256}
	if binary.PreviousSHA256 != "" {
		args = append(args, "--previous-sha256="+binary.PreviousSHA256)
	}
	pod := Command(binary.Image, "/tools/node-executor")
	pod.Spec.Containers[0].Args = args
	pod.Spec.Containers[0].SecurityContext.RunAsUser = ptr.To[int64](0)
	pod.Spec.Containers[0].SecurityContext.RunAsNonRoot = ptr.To(false)
	pod.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{{Name: "tools", MountPath: "/tools", ReadOnly: true}, {Name: "target", MountPath: "/target"}}
	pod.Spec.Volumes = []corev1.Volume{
		{Name: "tools", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
		{Name: "target", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: filepath.Dir(binary.Destination), Type: ptr.To(corev1.HostPathDirectory)}}},
	}
	helper := pod.Spec.Containers[0].DeepCopy()
	helper.Name, helper.Image = "prepare-helper", binary.HelperImage
	helper.Command = []string{"/node-executor"}
	helper.Args = []string{"copy", "--source=/node-executor", "--directory=/tools", "--name=node-executor"}
	helper.VolumeMounts = []corev1.VolumeMount{{Name: "tools", MountPath: "/tools"}}
	pod.Spec.InitContainers = []corev1.Container{*helper}
	return pod, nil
}

func pinnedImage(image string) bool {
	parts := strings.Split(image, "@sha256:")
	return len(parts) == 2 && parts[0] != "" && !strings.ContainsAny(parts[0], " \t\r\n@") && digest(parts[1])
}

func digest(value string) bool {
	if len(value) != 64 || value != strings.ToLower(value) {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func hash(value any) string {
	data, _ := json.Marshal(value)
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

func normalize(operation Operation) (corev1.PodTemplateSpec, error) {
	if len(validation.IsDNS1123Label(operation.Name)) != 0 || operation.Revision == "" || len(operation.Revision) > 256 || operation.Node.Name == "" || operation.Node.UID == "" {
		return corev1.PodTemplateSpec{}, errors.New("operation requires a DNS label name, bounded revision, and node name/UID")
	}
	if operation.Node.Namespace != "" || operation.Node.Kind != "" && operation.Node.Kind != "Node" || operation.Node.APIVersion != "" && operation.Node.APIVersion != "v1" {
		return corev1.PodTemplateSpec{}, errors.New("target must reference a cluster-scoped v1 Node")
	}
	template := operation.Template.DeepCopy()
	if len(template.Spec.Containers) == 0 || template.Spec.NodeName != "" || template.Spec.RestartPolicy != "" && template.Spec.RestartPolicy != corev1.RestartPolicyNever {
		return corev1.PodTemplateSpec{}, errors.New("template requires containers, no nodeName, and restartPolicy Never")
	}
	if template.Name != "" || template.GenerateName != "" || template.Namespace != "" || template.UID != "" || len(template.OwnerReferences) != 0 || len(template.Finalizers) != 0 {
		return corev1.PodTemplateSpec{}, errors.New("template metadata must not supply resource identity or ownership")
	}
	for _, container := range append(append([]corev1.Container{}, template.Spec.InitContainers...), template.Spec.Containers...) {
		if container.RestartPolicy != nil && *container.RestartPolicy != corev1.ContainerRestartPolicyNever || len(container.RestartPolicyRules) != 0 {
			return corev1.PodTemplateSpec{}, fmt.Errorf("container %q must not restart", container.Name)
		}
		if !pinnedImage(container.Image) {
			return corev1.PodTemplateSpec{}, fmt.Errorf("container %q image must be digest-pinned", container.Name)
		}
	}
	template.Spec.RestartPolicy = corev1.RestartPolicyNever
	if template.Spec.AutomountServiceAccountToken == nil {
		template.Spec.AutomountServiceAccountToken = ptr.To(false)
	}
	if template.Spec.Affinity == nil {
		template.Spec.Affinity = &corev1.Affinity{}
	}
	if template.Spec.Affinity.NodeAffinity == nil {
		template.Spec.Affinity.NodeAffinity = &corev1.NodeAffinity{}
	}
	selector := template.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
	if selector == nil {
		selector = &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{}}}
		template.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution = selector
	} else {
		if len(selector.NodeSelectorTerms) == 0 {
			return corev1.PodTemplateSpec{}, errors.New("required node affinity must contain nonempty terms")
		}
		for _, term := range selector.NodeSelectorTerms {
			if len(term.MatchExpressions) == 0 && len(term.MatchFields) == 0 {
				return corev1.PodTemplateSpec{}, errors.New("required node affinity must contain nonempty terms")
			}
		}
	}
	for index := range selector.NodeSelectorTerms {
		selector.NodeSelectorTerms[index].MatchFields = append(selector.NodeSelectorTerms[index].MatchFields, corev1.NodeSelectorRequirement{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{operation.Node.Name}})
	}
	template.Spec.Tolerations = append(template.Spec.Tolerations, corev1.Toleration{Key: "node.kubernetes.io/unschedulable", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule})
	return *template, nil
}

func jobName(recordName, fingerprint string, sequence uint64) string {
	return "execute-" + hash(struct {
		Record, Fingerprint string
		Sequence            uint64
	}{recordName, fingerprint, sequence})[:40]
}

func jobFor(namespace, recordName string, record receipt, template corev1.PodTemplateSpec, owner metav1.OwnerReference) *batchv1.Job {
	if template.Labels == nil {
		template.Labels = map[string]string{}
	}
	if template.Annotations == nil {
		template.Annotations = map[string]string{}
	}
	template.Labels[recordLabel] = recordName
	template.Annotations[fingerprintAnnotation] = record.Fingerprint
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: record.JobName, Namespace: namespace,
			Labels: map[string]string{recordLabel: recordName}, Annotations: map[string]string{fingerprintAnnotation: record.Fingerprint}, OwnerReferences: []metav1.OwnerReference{owner}},
		Spec: batchv1.JobSpec{Suspend: ptr.To(true), Parallelism: ptr.To[int32](1), Completions: ptr.To[int32](1), BackoffLimit: ptr.To[int32](0), PodReplacementPolicy: ptr.To(batchv1.Failed), Template: template},
	}
}
