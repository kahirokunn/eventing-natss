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

package resources

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/validation"
	eventingv1 "knative.dev/eventing/pkg/apis/eventing/v1"
	"knative.dev/pkg/ptr"
	"knative.dev/pkg/system"

	brokerautoscaler "knative.dev/eventing-natss/pkg/broker/autoscaler"
	brokerconfig "knative.dev/eventing-natss/pkg/broker/config"
)

func TestLongBrokerGeneratedResourceNamesFitDNSLabelLimit(t *testing.T) {
	broker := &eventingv1.Broker{ObjectMeta: metav1.ObjectMeta{
		Name:      strings.Repeat("b", 63),
		Namespace: "test-namespace",
		UID:       "test-uid",
	}}
	filterName := FilterName(broker.Name)
	deployment := MakeFilterDeployment(&FilterArgs{Broker: broker})
	service := MakeFilterService(broker)
	scaledObject, err := brokerautoscaler.MakeScaledObject(
		broker,
		nil,
		filterName,
		brokerautoscaler.Settings{Enabled: true, MinScale: 0, MaxScale: 2, PollingInterval: 10, CooldownPeriod: 30},
		brokerautoscaler.MonitoringConfig{Endpoint: "nats.nats-io.svc:8222", Account: "$G"},
	)
	if err != nil {
		t.Fatal(err)
	}
	hpaName, found, err := unstructured.NestedString(
		scaledObject.Object,
		"spec", "advanced", "horizontalPodAutoscalerConfig", "name",
	)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("generated HPA name is missing")
	}

	for kind, name := range map[string]string{
		"Deployment":   deployment.Name,
		"Service":      service.Name,
		"ScaledObject": scaledObject.GetName(),
		"HPA":          hpaName,
	} {
		if len(name) > 63 {
			t.Errorf("%s name has %d characters, want at most 63: %q", kind, len(name), name)
		}
		if errs := validation.IsDNS1035Label(name); len(errs) > 0 {
			t.Errorf("%s name %q is not a DNS-1035 label: %v", kind, name, errs)
		}
	}
	if deployment.Name != filterName || service.Name != filterName {
		t.Fatalf("filter resource names differ: FilterName=%q Deployment=%q Service=%q", filterName, deployment.Name, service.Name)
	}
	targetName, found, err := unstructured.NestedString(scaledObject.Object, "spec", "scaleTargetRef", "name")
	if err != nil || !found {
		t.Fatalf("scaled target name is missing: found=%v err=%v", found, err)
	}
	if targetName != filterName {
		t.Fatalf("scale target = %q, want %q", targetName, filterName)
	}
}

func TestMakeFilterDeployment(t *testing.T) {
	broker := &eventingv1.Broker{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-broker",
			Namespace: "test-namespace",
			UID:       "test-uid",
		},
	}

	tests := []struct {
		name          string
		args          *FilterArgs
		wantName      string
		wantNamespace string
		wantReplicas  int32
		wantImage     string
		wantLabels    map[string]string
	}{
		{
			name: "basic deployment",
			args: &FilterArgs{
				Broker:             broker,
				Image:              "gcr.io/test/filter:latest",
				ServiceAccountName: "test-sa",
				StreamName:         "TEST_STREAM",
				NatsURL:            "nats://nats:4222",
			},
			wantName:      "test-broker-broker-filter",
			wantNamespace: "test-namespace",
			wantReplicas:  1,
			wantImage:     "gcr.io/test/filter:latest",
			wantLabels: map[string]string{
				BrokerLabelKey: "test-broker",
				RoleLabelKey:   FilterRoleLabelValue,
			},
		},
		{
			name: "deployment with template",
			args: &FilterArgs{
				Broker:             broker,
				Image:              "gcr.io/test/filter:latest",
				ServiceAccountName: "test-sa",
				StreamName:         "TEST_STREAM",
				NatsURL:            "nats://nats:4222",
				Template: &brokerconfig.DeploymentTemplate{
					Replicas: ptr.Int32(5),
					Labels: map[string]string{
						"custom": "label",
					},
					Annotations: map[string]string{
						"custom": "annotation",
					},
				},
			},
			wantName:      "test-broker-broker-filter",
			wantNamespace: "test-namespace",
			wantReplicas:  5,
			wantImage:     "gcr.io/test/filter:latest",
			wantLabels: map[string]string{
				BrokerLabelKey: "test-broker",
				RoleLabelKey:   FilterRoleLabelValue,
				"custom":       "label",
			},
		},
		{
			name: "deployment with pod labels and annotations",
			args: &FilterArgs{
				Broker:             broker,
				Image:              "gcr.io/test/filter:latest",
				ServiceAccountName: "test-sa",
				StreamName:         "TEST_STREAM",
				NatsURL:            "nats://nats:4222",
				Template: &brokerconfig.DeploymentTemplate{
					PodLabels: map[string]string{
						"pod-label": "value",
					},
					PodAnnotations: map[string]string{
						"pod-annotation": "value",
					},
				},
			},
			wantName:      "test-broker-broker-filter",
			wantNamespace: "test-namespace",
			wantReplicas:  1,
			wantImage:     "gcr.io/test/filter:latest",
			wantLabels: map[string]string{
				BrokerLabelKey: "test-broker",
				RoleLabelKey:   FilterRoleLabelValue,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deployment := MakeFilterDeployment(tt.args)

			if deployment.Name != tt.wantName {
				t.Errorf("Name = %v, want %v", deployment.Name, tt.wantName)
			}

			if deployment.Namespace != tt.wantNamespace {
				t.Errorf("Namespace = %v, want %v", deployment.Namespace, tt.wantNamespace)
			}

			if *deployment.Spec.Replicas != tt.wantReplicas {
				t.Errorf("Replicas = %v, want %v", *deployment.Spec.Replicas, tt.wantReplicas)
			}

			if len(deployment.Spec.Template.Spec.Containers) != 1 {
				t.Fatalf("Expected 1 container, got %d", len(deployment.Spec.Template.Spec.Containers))
			}

			container := deployment.Spec.Template.Spec.Containers[0]
			if container.Image != tt.wantImage {
				t.Errorf("Image = %v, want %v", container.Image, tt.wantImage)
			}

			if container.Name != FilterContainerName {
				t.Errorf("Container name = %v, want %v", container.Name, FilterContainerName)
			}

			// Check labels
			for k, v := range tt.wantLabels {
				if deployment.Labels[k] != v {
					t.Errorf("Label %s = %v, want %v", k, deployment.Labels[k], v)
				}
			}

			// Verify owner reference is set
			if len(deployment.OwnerReferences) != 1 {
				t.Errorf("Expected 1 owner reference, got %d", len(deployment.OwnerReferences))
			}

			// Verify ports
			if len(container.Ports) != 2 {
				t.Errorf("Expected 2 ports, got %d", len(container.Ports))
			}

			// Verify probes are set
			if container.LivenessProbe == nil {
				t.Error("LivenessProbe should not be nil")
			}
			if container.ReadinessProbe == nil {
				t.Error("ReadinessProbe should not be nil")
			}
		})
	}
}

func TestMakeFilterDeploymentTerminationGracePeriod(t *testing.T) {
	broker := &eventingv1.Broker{ObjectMeta: metav1.ObjectMeta{
		Name:      "test-broker",
		Namespace: "test-namespace",
		UID:       "test-uid",
	}}

	deployment := MakeFilterDeployment(&FilterArgs{Broker: broker})
	got := deployment.Spec.Template.Spec.TerminationGracePeriodSeconds
	if got == nil {
		t.Fatal("terminationGracePeriodSeconds is nil, want 45")
	}
	if *got != 45 {
		t.Errorf("terminationGracePeriodSeconds = %d, want 45", *got)
	}
}

func TestMakeFilterDeploymentDefaultsResourcesAndSecurity(t *testing.T) {
	broker := &eventingv1.Broker{ObjectMeta: metav1.ObjectMeta{
		Name: "test-broker", Namespace: "test-namespace", UID: "test-uid",
	}}
	deployment := MakeFilterDeployment(&FilterArgs{
		Broker: broker, Image: "filter:latest", ServiceAccountName: "filter-sa",
		StreamName: "TEST_STREAM", NatsURL: "nats://nats:4222",
	})

	filter := deployment.Spec.Template.Spec.Containers[0]
	assertDefaultFilterResources(t, filter.Resources)
	assertDefaultFilterSecurityContext(t, filter.SecurityContext)
}

func TestMakeFilterDeploymentExplicitResourcesAreExact(t *testing.T) {
	broker := &eventingv1.Broker{ObjectMeta: metav1.ObjectMeta{
		Name: "test-broker", Namespace: "test-namespace", UID: "test-uid",
	}}
	want := corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("225m")},
		Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("384Mi")},
	}
	deployment := MakeFilterDeployment(&FilterArgs{
		Broker: broker, Image: "filter:latest", ServiceAccountName: "filter-sa",
		StreamName: "TEST_STREAM", NatsURL: "nats://nats:4222",
		Template: &brokerconfig.DeploymentTemplate{Resources: want},
	})

	got := deployment.Spec.Template.Spec.Containers[0].Resources
	if !apiequality.Semantic.DeepEqual(got, want) {
		t.Errorf("explicit resources = %#v, want exact template %#v", got, want)
	}
}

func TestMakeFilterDeploymentEmptyResourcesUseSafeDefaults(t *testing.T) {
	broker := &eventingv1.Broker{ObjectMeta: metav1.ObjectMeta{
		Name: "test-broker", Namespace: "test-namespace", UID: "test-uid",
	}}
	deployment := MakeFilterDeployment(&FilterArgs{
		Broker: broker, Image: "filter:latest", ServiceAccountName: "filter-sa",
		StreamName: "TEST_STREAM", NatsURL: "nats://nats:4222",
		Template: &brokerconfig.DeploymentTemplate{
			Annotations: map[string]string{"custom.example/enabled": "true"},
			Resources:   corev1.ResourceRequirements{},
		},
	})

	assertDefaultFilterResources(t, deployment.Spec.Template.Spec.Containers[0].Resources)
}

func assertDefaultFilterResources(t *testing.T, got corev1.ResourceRequirements) {
	t.Helper()
	want := corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("100m"),
			corev1.ResourceMemory: resource.MustParse("64Mi"),
		},
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("500m"),
			corev1.ResourceMemory: resource.MustParse("256Mi"),
		},
	}
	if !apiequality.Semantic.DeepEqual(got, want) {
		t.Errorf("default filter resources = %#v, want %#v", got, want)
	}
}

func assertDefaultFilterSecurityContext(t *testing.T, got *corev1.SecurityContext) {
	t.Helper()
	if got == nil {
		t.Fatal("filter securityContext is nil")
	}
	if got.RunAsNonRoot == nil || !*got.RunAsNonRoot {
		t.Errorf("runAsNonRoot = %v, want true", got.RunAsNonRoot)
	}
	if got.AllowPrivilegeEscalation == nil || *got.AllowPrivilegeEscalation {
		t.Errorf("allowPrivilegeEscalation = %v, want false", got.AllowPrivilegeEscalation)
	}
	if got.ReadOnlyRootFilesystem == nil || !*got.ReadOnlyRootFilesystem {
		t.Errorf("readOnlyRootFilesystem = %v, want true", got.ReadOnlyRootFilesystem)
	}
	if got.Capabilities == nil || len(got.Capabilities.Add) != 0 || !apiequality.Semantic.DeepEqual(got.Capabilities.Drop, []corev1.Capability{"ALL"}) {
		t.Errorf("capabilities = %#v, want drop ALL with no additions", got.Capabilities)
	}
	if got.SeccompProfile == nil || got.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault || got.SeccompProfile.LocalhostProfile != nil {
		t.Errorf("seccompProfile = %#v, want RuntimeDefault", got.SeccompProfile)
	}
}

func TestMakeFilterDeploymentRequiredLabelsCannotBeOverridden(t *testing.T) {
	broker := &eventingv1.Broker{ObjectMeta: metav1.ObjectMeta{
		Name: "test-broker", Namespace: "test-namespace", UID: "test-uid",
	}}
	deployment := MakeFilterDeployment(&FilterArgs{
		Broker: broker,
		Template: &brokerconfig.DeploymentTemplate{
			Labels: map[string]string{
				BrokerLabelKey:            "wrong-broker",
				RoleLabelKey:              "wrong-role",
				"custom-deployment-label": "kept",
			},
			PodLabels: map[string]string{
				BrokerLabelKey:     "wrong-broker",
				RoleLabelKey:       "wrong-role",
				"custom-pod-label": "kept",
			},
		},
	})
	required := FilterLabels(broker.Name)

	for key, want := range required {
		if got := deployment.Labels[key]; got != want {
			t.Errorf("Deployment label %q = %q, want required value %q", key, got, want)
		}
		if got := deployment.Spec.Selector.MatchLabels[key]; got != want {
			t.Errorf("selector label %q = %q, want required value %q", key, got, want)
		}
		if got := deployment.Spec.Template.Labels[key]; got != want {
			t.Errorf("Pod label %q = %q, want required value %q", key, got, want)
		}
		if deployment.Spec.Template.Labels[key] != deployment.Spec.Selector.MatchLabels[key] {
			t.Errorf("Pod label %q does not match selector: Pod=%q selector=%q", key, deployment.Spec.Template.Labels[key], deployment.Spec.Selector.MatchLabels[key])
		}
	}
	if got := deployment.Labels["custom-deployment-label"]; got != "kept" {
		t.Errorf("custom Deployment label = %q, want kept", got)
	}
	if got := deployment.Spec.Template.Labels["custom-pod-label"]; got != "kept" {
		t.Errorf("custom Pod label = %q, want kept", got)
	}
}

func TestMakeFilterDeploymentWithResources(t *testing.T) {
	broker := &eventingv1.Broker{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-broker",
			Namespace: "test-namespace",
			UID:       "test-uid",
		},
	}

	args := &FilterArgs{
		Broker:             broker,
		Image:              "gcr.io/test/filter:latest",
		ServiceAccountName: "test-sa",
		StreamName:         "TEST_STREAM",
		NatsURL:            "nats://nats:4222",
		Template: &brokerconfig.DeploymentTemplate{
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("200m"),
					corev1.ResourceMemory: resource.MustParse("256Mi"),
				},
				Limits: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("1"),
					corev1.ResourceMemory: resource.MustParse("1Gi"),
				},
			},
		},
	}

	deployment := MakeFilterDeployment(args)
	container := deployment.Spec.Template.Spec.Containers[0]

	if container.Resources.Requests.Cpu().String() != "200m" {
		t.Errorf("CPU request = %v, want 200m", container.Resources.Requests.Cpu().String())
	}

	if container.Resources.Requests.Memory().String() != "256Mi" {
		t.Errorf("Memory request = %v, want 256Mi", container.Resources.Requests.Memory().String())
	}

	if container.Resources.Limits.Cpu().String() != "1" {
		t.Errorf("CPU limit = %v, want 1", container.Resources.Limits.Cpu().String())
	}
}

func TestMakeFilterDeploymentWithNodeSelector(t *testing.T) {
	broker := &eventingv1.Broker{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-broker",
			Namespace: "test-namespace",
			UID:       "test-uid",
		},
	}

	args := &FilterArgs{
		Broker:             broker,
		Image:              "gcr.io/test/filter:latest",
		ServiceAccountName: "test-sa",
		StreamName:         "TEST_STREAM",
		NatsURL:            "nats://nats:4222",
		Template: &brokerconfig.DeploymentTemplate{
			NodeSelector: map[string]string{
				"disktype": "ssd",
			},
		},
	}

	deployment := MakeFilterDeployment(args)

	if deployment.Spec.Template.Spec.NodeSelector["disktype"] != "ssd" {
		t.Errorf("NodeSelector disktype = %v, want ssd", deployment.Spec.Template.Spec.NodeSelector["disktype"])
	}
}

func TestMakeFilterService(t *testing.T) {
	broker := &eventingv1.Broker{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-broker",
			Namespace: "test-namespace",
			UID:       "test-uid",
		},
	}

	service := MakeFilterService(broker)

	if service.Name != "test-broker-broker-filter" {
		t.Errorf("Name = %v, want test-broker-broker-filter", service.Name)
	}

	if service.Namespace != "test-namespace" {
		t.Errorf("Namespace = %v, want test-namespace", service.Namespace)
	}

	// Check labels
	if service.Labels[BrokerLabelKey] != "test-broker" {
		t.Errorf("Label %s = %v, want test-broker", BrokerLabelKey, service.Labels[BrokerLabelKey])
	}

	if service.Labels[RoleLabelKey] != FilterRoleLabelValue {
		t.Errorf("Label %s = %v, want %s", RoleLabelKey, service.Labels[RoleLabelKey], FilterRoleLabelValue)
	}

	// Check selector
	if service.Spec.Selector[BrokerLabelKey] != "test-broker" {
		t.Errorf("Selector %s = %v, want test-broker", BrokerLabelKey, service.Spec.Selector[BrokerLabelKey])
	}

	// Check ports
	if len(service.Spec.Ports) != 1 {
		t.Fatalf("Expected 1 port, got %d", len(service.Spec.Ports))
	}

	port := service.Spec.Ports[0]
	if port.Port != 80 {
		t.Errorf("Port = %v, want 80", port.Port)
	}

	if port.TargetPort.IntVal != FilterPortNumber {
		t.Errorf("TargetPort = %v, want %d", port.TargetPort.IntVal, FilterPortNumber)
	}

	// Verify owner reference is set
	if len(service.OwnerReferences) != 1 {
		t.Errorf("Expected 1 owner reference, got %d", len(service.OwnerReferences))
	}
}

func TestMakeFilterEnvVars(t *testing.T) {
	broker := &eventingv1.Broker{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-broker",
			Namespace: "test-namespace",
		},
	}

	args := &FilterArgs{
		Broker:             broker,
		Image:              "gcr.io/test/filter:latest",
		ServiceAccountName: "test-sa",
		StreamName:         "TEST_STREAM",
		NatsURL:            "nats://nats:4222",
		NatsConfigJSON:     `{"url":"tls://nats:4222","auth":{"credentialFile":{"secret":{"name":"credentials"}}}}`,
	}

	deployment := MakeFilterDeployment(args)
	container := deployment.Spec.Template.Spec.Containers[0]
	envVars := container.Env

	// Build a map for easy lookup
	envMap := make(map[string]string)
	for _, env := range envVars {
		if env.Value != "" {
			envMap[env.Name] = env.Value
		}
	}

	// Check required environment variables
	if envMap["BROKER_NAME"] != "test-broker" {
		t.Errorf("BROKER_NAME = %v, want test-broker", envMap["BROKER_NAME"])
	}
	if envMap["NAMESPACE"] != "test-namespace" {
		t.Errorf("NAMESPACE = %v, want test-namespace", envMap["NAMESPACE"])
	}

	if envMap["BROKER_NAMESPACE"] != "test-namespace" {
		t.Errorf("BROKER_NAMESPACE = %v, want test-namespace", envMap["BROKER_NAMESPACE"])
	}

	if envMap["STREAM_NAME"] != "TEST_STREAM" {
		t.Errorf("STREAM_NAME = %v, want TEST_STREAM", envMap["STREAM_NAME"])
	}

	if envMap["NATS_URL"] != "nats://nats:4222" {
		t.Errorf("NATS_URL = %v, want nats://nats:4222", envMap["NATS_URL"])
	}

	if envMap["NATS_CONFIG"] != args.NatsConfigJSON {
		t.Errorf("NATS_CONFIG = %v, want controller snapshot", envMap["NATS_CONFIG"])
	}

	if envMap["CONTAINER_NAME"] != FilterContainerName {
		t.Errorf("CONTAINER_NAME = %v, want %s", envMap["CONTAINER_NAME"], FilterContainerName)
	}

	// Filter should have CONFIG_LEADERELECTION_NAME
	if envMap["CONFIG_LEADERELECTION_NAME"] != "config-leader-election" {
		t.Errorf("CONFIG_LEADERELECTION_NAME = %v, want config-leader-election", envMap["CONFIG_LEADERELECTION_NAME"])
	}
}

func TestMakeFilterEnvRequiredValuesCannotBeOverridden(t *testing.T) {
	broker := &eventingv1.Broker{ObjectMeta: metav1.ObjectMeta{
		Name: "test-broker", Namespace: "test-namespace",
	}}
	requiredValues := map[string]string{
		system.NamespaceEnvKey:       system.Namespace(),
		"NAMESPACE":                  broker.Namespace,
		"BROKER_NAME":                broker.Name,
		"BROKER_NAMESPACE":           broker.Namespace,
		"STREAM_NAME":                "TEST_STREAM",
		"NATS_URL":                   "nats://nats:4222",
		"NATS_CONFIG":                `{"url":"tls://nats:4222"}`,
		"METRICS_DOMAIN":             "knative.dev/eventing",
		"CONFIG_LOGGING_NAME":        "config-logging",
		"CONFIG_LEADERELECTION_NAME": "config-leader-election",
		"CONTAINER_NAME":             FilterContainerName,
	}
	templateEnv := make([]corev1.EnvVar, 0, len(requiredValues)+2)
	for name := range requiredValues {
		templateEnv = append(templateEnv, corev1.EnvVar{Name: name, Value: "user-override"})
	}
	templateEnv = append(templateEnv,
		corev1.EnvVar{Name: "POD_NAME", Value: "user-override"},
		corev1.EnvVar{Name: "CUSTOM_ENV", Value: "$(BROKER_NAME)-suffix"},
	)
	deployment := MakeFilterDeployment(&FilterArgs{
		Broker:         broker,
		StreamName:     "TEST_STREAM",
		NatsURL:        "nats://nats:4222",
		NatsConfigJSON: requiredValues["NATS_CONFIG"],
		Template:       &brokerconfig.DeploymentTemplate{Env: templateEnv},
	})

	counts := make(map[string]int)
	byName := make(map[string]corev1.EnvVar)
	indices := make(map[string]int)
	for index, env := range deployment.Spec.Template.Spec.Containers[0].Env {
		counts[env.Name]++
		byName[env.Name] = env
		indices[env.Name] = index
	}
	for name, want := range requiredValues {
		if counts[name] != 1 {
			t.Errorf("environment variable %q appears %d times, want exactly 1", name, counts[name])
		}
		if got := byName[name]; got.Value != want || got.ValueFrom != nil {
			t.Errorf("environment variable %q = %#v, want controller value %q", name, got, want)
		}
	}
	if counts["POD_NAME"] != 1 {
		t.Errorf("environment variable POD_NAME appears %d times, want exactly 1", counts["POD_NAME"])
	}
	podName := byName["POD_NAME"]
	if podName.Value != "" || podName.ValueFrom == nil || podName.ValueFrom.FieldRef == nil || podName.ValueFrom.FieldRef.FieldPath != "metadata.name" {
		t.Errorf("POD_NAME = %#v, want controller metadata.name field reference", podName)
	}
	if counts["CUSTOM_ENV"] != 1 || byName["CUSTOM_ENV"].Value != "$(BROKER_NAME)-suffix" {
		t.Errorf("CUSTOM_ENV = %#v count=%d, want retained custom value", byName["CUSTOM_ENV"], counts["CUSTOM_ENV"])
	}
	if indices["CUSTOM_ENV"] <= indices["BROKER_NAME"] {
		t.Errorf("CUSTOM_ENV index = %d, want after BROKER_NAME index %d so Kubernetes can expand $(BROKER_NAME)", indices["CUSTOM_ENV"], indices["BROKER_NAME"])
	}
}
