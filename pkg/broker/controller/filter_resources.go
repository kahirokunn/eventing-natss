/*
Copyright 2026 The Knative Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"fmt"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/utils/ptr"

	"knative.dev/eventing-natss/pkg/broker/controller/resources"
)

// mergeFilterDeployment overlays the fields owned by this controller onto the
// stored Deployment. Kubernetes and admission-owned defaults remain untouched,
// so a steady-state reconcile does not issue a PUT merely to clear them.
func mergeFilterDeployment(existing, expected *appsv1.Deployment) (*appsv1.Deployment, error) {
	if !equality.Semantic.DeepEqual(existing.Spec.Selector, expected.Spec.Selector) {
		return nil, fmt.Errorf("filter deployment selector is immutable: existing=%v expected=%v", existing.Spec.Selector, expected.Spec.Selector)
	}

	merged := existing.DeepCopy()
	merged.Labels = mergeStringMap(merged.Labels, expected.Labels)
	merged.Annotations = mergeStringMap(merged.Annotations, expected.Annotations)
	merged.Spec.Replicas = copyInt32Pointer(expected.Spec.Replicas)
	merged.Spec.Template.Labels = mergeStringMap(merged.Spec.Template.Labels, expected.Spec.Template.Labels)
	merged.Spec.Template.Annotations = mergeStringMap(merged.Spec.Template.Annotations, expected.Spec.Template.Annotations)

	existingPod := &merged.Spec.Template.Spec
	expectedPod := &expected.Spec.Template.Spec
	existingPod.ServiceAccountName = expectedPod.ServiceAccountName
	existingPod.TerminationGracePeriodSeconds = copyInt64Pointer(expectedPod.TerminationGracePeriodSeconds)
	existingPod.NodeSelector = copyStringMap(expectedPod.NodeSelector)
	if expectedPod.Affinity == nil {
		existingPod.Affinity = nil
	} else {
		existingPod.Affinity = expectedPod.Affinity.DeepCopy()
	}
	expectedContainer, err := namedContainer(expectedPod.Containers, resources.FilterContainerName)
	if err != nil {
		return nil, fmt.Errorf("invalid expected filter deployment: %w", err)
	}
	existingIndex, err := namedContainerIndex(existingPod.Containers, resources.FilterContainerName)
	if err != nil {
		return nil, fmt.Errorf("invalid existing filter deployment: %w", err)
	}
	if existingIndex == -1 {
		existingPod.Containers = append(existingPod.Containers, *expectedContainer.DeepCopy())
	} else {
		mergeFilterContainer(&existingPod.Containers[existingIndex], expectedContainer)
	}

	return merged, nil
}

func mergeFilterContainer(existing, expected *corev1.Container) {
	existing.Image = expected.Image
	if expected.ImagePullPolicy != "" {
		existing.ImagePullPolicy = expected.ImagePullPolicy
	} else {
		existing.ImagePullPolicy = defaultImagePullPolicy(expected.Image)
	}
	existing.Env = mergeEnvVars(existing.Env, expected.Env)
	existing.Resources = normalizedResourceRequirements(expected.Resources)
	existing.SecurityContext = mergeFilterSecurityContext(existing.SecurityContext, expected.SecurityContext)
	existing.Ports = mergeContainerPorts(existing.Ports, expected.Ports)
	existing.LivenessProbe = mergeProbe(existing.LivenessProbe, expected.LivenessProbe)
	existing.ReadinessProbe = mergeProbe(existing.ReadinessProbe, expected.ReadinessProbe)
}

func mergeFilterSecurityContext(existing, expected *corev1.SecurityContext) *corev1.SecurityContext {
	if expected == nil {
		if existing == nil {
			return nil
		}
		return existing.DeepCopy()
	}
	if existing == nil {
		return expected.DeepCopy()
	}
	merged := existing.DeepCopy()
	merged.AllowPrivilegeEscalation = copyBoolPointer(expected.AllowPrivilegeEscalation)
	merged.ReadOnlyRootFilesystem = copyBoolPointer(expected.ReadOnlyRootFilesystem)
	merged.RunAsNonRoot = copyBoolPointer(expected.RunAsNonRoot)
	if expected.Capabilities == nil {
		merged.Capabilities = nil
	} else {
		merged.Capabilities = expected.Capabilities.DeepCopy()
	}
	if expected.SeccompProfile == nil {
		merged.SeccompProfile = nil
	} else {
		merged.SeccompProfile = expected.SeccompProfile.DeepCopy()
	}
	return merged
}

func copyBoolPointer(value *bool) *bool {
	if value == nil {
		return nil
	}
	return ptr.To(*value)
}

func mergeFilterService(existing, expected *corev1.Service) (*corev1.Service, error) {
	merged := existing.DeepCopy()
	merged.Labels = mergeStringMap(merged.Labels, expected.Labels)
	merged.Spec.Selector = copyStringMap(expected.Spec.Selector)
	ports, err := mergeServicePorts(merged.Spec.Ports, expected.Spec.Ports)
	if err != nil {
		return nil, err
	}
	merged.Spec.Ports = ports
	return merged, nil
}

func mergeStringMap(existing, expected map[string]string) map[string]string {
	merged := copyStringMap(existing)
	if merged == nil && len(expected) > 0 {
		merged = make(map[string]string, len(expected))
	}
	for key, value := range expected {
		merged[key] = value
	}
	return merged
}

func copyStringMap(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	result := make(map[string]string, len(values))
	for key, value := range values {
		result[key] = value
	}
	return result
}

func copyInt32Pointer(value *int32) *int32 {
	if value == nil {
		return nil
	}
	return ptr.To(*value)
}

func copyInt64Pointer(value *int64) *int64 {
	if value == nil {
		return nil
	}
	return ptr.To(*value)
}

func namedContainer(containers []corev1.Container, name string) (*corev1.Container, error) {
	index, err := namedContainerIndex(containers, name)
	if err != nil {
		return nil, err
	}
	if index == -1 {
		return nil, fmt.Errorf("container %q is missing", name)
	}
	return &containers[index], nil
}

func namedContainerIndex(containers []corev1.Container, name string) (int, error) {
	found := -1
	for index := range containers {
		if containers[index].Name != name {
			continue
		}
		if found != -1 {
			return -1, fmt.Errorf("container %q appears more than once", name)
		}
		found = index
	}
	return found, nil
}

func mergeEnvVars(existing, expected []corev1.EnvVar) []corev1.EnvVar {
	expectedByName := make(map[string]corev1.EnvVar, len(expected))
	expectedOrder := make([]string, 0, len(expected))
	for _, variable := range expected {
		expectedByName[variable.Name] = variable
		expectedOrder = append(expectedOrder, variable.Name)
	}

	storedOrder := make([]string, 0, len(expected))
	storedCounts := make(map[string]int, len(expected))
	for _, variable := range existing {
		if _, owned := expectedByName[variable.Name]; owned {
			storedOrder = append(storedOrder, variable.Name)
			storedCounts[variable.Name]++
		}
	}

	orderMatches := len(storedOrder) == len(expectedOrder)
	if orderMatches {
		for index := range expectedOrder {
			if storedOrder[index] != expectedOrder[index] || storedCounts[expectedOrder[index]] != 1 {
				orderMatches = false
				break
			}
		}
	}

	if orderMatches {
		merged := make([]corev1.EnvVar, 0, len(existing))
		for _, stored := range existing {
			desired, owned := expectedByName[stored.Name]
			if owned {
				stored = defaultEnvVar(desired)
			}
			merged = append(merged, stored)
		}
		return merged
	}

	// Repair missing, duplicate, or reordered owned entries once. A mutating
	// webhook may reinsert unknown entries anywhere; once the owned subsequence
	// is correct the branch above preserves those positions on later reconciles.
	merged := make([]corev1.EnvVar, 0, len(expected)+len(existing))
	for _, desired := range expected {
		merged = append(merged, defaultEnvVar(desired))
	}
	for _, stored := range existing {
		if _, owned := expectedByName[stored.Name]; !owned {
			merged = append(merged, stored)
		}
	}
	return merged
}

func defaultEnvVar(desired corev1.EnvVar) corev1.EnvVar {
	merged := *desired.DeepCopy()
	if merged.ValueFrom == nil {
		return merged
	}
	if merged.ValueFrom.FieldRef != nil && merged.ValueFrom.FieldRef.APIVersion == "" {
		merged.ValueFrom.FieldRef.APIVersion = "v1"
	}
	if merged.ValueFrom.FileKeyRef != nil && merged.ValueFrom.FileKeyRef.Optional == nil {
		merged.ValueFrom.FileKeyRef.Optional = ptr.To(false)
	}
	return merged
}

func mergeContainerPorts(existing, expected []corev1.ContainerPort) []corev1.ContainerPort {
	merged := append([]corev1.ContainerPort(nil), existing...)
	for _, desired := range expected {
		found := false
		for index := range merged {
			if merged[index].Name != desired.Name {
				continue
			}
			merged[index].Protocol = desired.Protocol
			merged[index].ContainerPort = desired.ContainerPort
			found = true
			break
		}
		if !found {
			merged = append(merged, desired)
		}
	}
	return merged
}

func mergeServicePorts(existing, expected []corev1.ServicePort) ([]corev1.ServicePort, error) {
	merged := append([]corev1.ServicePort(nil), existing...)
	for _, desired := range expected {
		match := -1
		for index := range merged {
			if merged[index].Name == desired.Name {
				if match != -1 {
					return nil, fmt.Errorf("service port %q appears more than once", desired.Name)
				}
				match = index
			}
		}
		if match == -1 && len(merged) == 1 {
			match = 0
		}
		if match == -1 {
			for index := range merged {
				if merged[index].Protocol == desired.Protocol && merged[index].Port == desired.Port {
					if match != -1 {
						return nil, fmt.Errorf("service port %q cannot be repaired unambiguously", desired.Name)
					}
					match = index
				}
			}
		}
		if match == -1 {
			if len(merged) > 0 {
				return nil, fmt.Errorf("service port %q cannot be matched to an existing port", desired.Name)
			}
			merged = append(merged, desired)
			continue
		}
		merged[match].Name = desired.Name
		merged[match].Protocol = desired.Protocol
		merged[match].Port = desired.Port
		merged[match].TargetPort = desired.TargetPort
	}
	return merged, nil
}

func mergeProbe(existing, expected *corev1.Probe) *corev1.Probe {
	if expected == nil {
		return nil
	}
	if existing == nil {
		return expected.DeepCopy()
	}
	merged := existing.DeepCopy()
	merged.ProbeHandler = mergeProbeHandler(existing.ProbeHandler, expected.ProbeHandler)
	merged.InitialDelaySeconds = expected.InitialDelaySeconds
	merged.PeriodSeconds = expected.PeriodSeconds
	return merged
}

func mergeProbeHandler(existing, expected corev1.ProbeHandler) corev1.ProbeHandler {
	if existing.HTTPGet == nil || expected.HTTPGet == nil {
		return *expected.DeepCopy()
	}
	merged := *existing.DeepCopy()
	merged.Exec = nil
	merged.TCPSocket = nil
	merged.GRPC = nil
	merged.HTTPGet.Path = expected.HTTPGet.Path
	merged.HTTPGet.Port = expected.HTTPGet.Port
	merged.HTTPGet.Scheme = expected.HTTPGet.Scheme
	if merged.HTTPGet.Scheme == "" {
		merged.HTTPGet.Scheme = corev1.URISchemeHTTP
	}
	return merged
}

func defaultImagePullPolicy(image string) corev1.PullPolicy {
	imageName, digest, _ := strings.Cut(image, "@")
	lastSlash := strings.LastIndex(imageName, "/")
	lastColon := strings.LastIndex(imageName, ":")
	if lastColon > lastSlash {
		if imageName[lastColon+1:] == "latest" {
			return corev1.PullAlways
		}
		return corev1.PullIfNotPresent
	}
	if digest == "" {
		return corev1.PullAlways
	}
	return corev1.PullIfNotPresent
}

func normalizedResourceRequirements(resources corev1.ResourceRequirements) corev1.ResourceRequirements {
	normalized := *resources.DeepCopy()
	normalizeResourceList(normalized.Limits)
	normalizeResourceList(normalized.Requests)
	return normalized
}

func normalizeResourceList(resources corev1.ResourceList) {
	for name, quantity := range resources {
		quantity.RoundUp(-3)
		resources[name] = quantity
	}
}
