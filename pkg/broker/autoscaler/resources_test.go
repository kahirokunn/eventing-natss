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

package autoscaler

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"

	eventingv1 "knative.dev/eventing/pkg/apis/eventing/v1"

	brokerutils "knative.dev/eventing-natss/pkg/broker/utils"
)

func TestMakeScaledObject(t *testing.T) {
	broker := &eventingv1.Broker{ObjectMeta: metav1.ObjectMeta{
		Namespace: "shop", Name: "orders", UID: types.UID("broker-uid"),
		Annotations: map[string]string{LagThresholdAnnotation: "20"},
	}}
	triggerB := &eventingv1.Trigger{
		ObjectMeta: metav1.ObjectMeta{Namespace: "shop", Name: "b", UID: types.UID("bbbb-bbbb")},
		Spec:       eventingv1.TriggerSpec{Broker: "orders"},
	}
	triggerA := &eventingv1.Trigger{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "shop", Name: "a", UID: types.UID("aaaa-aaaa"),
			Annotations: map[string]string{LagThresholdAnnotation: "5", ActivationLagThresholdAnnotation: "1"},
		},
		Spec: eventingv1.TriggerSpec{Broker: "orders"},
	}
	other := &eventingv1.Trigger{
		ObjectMeta: metav1.ObjectMeta{Namespace: "shop", Name: "other", UID: types.UID("other")},
		Spec:       eventingv1.TriggerSpec{Broker: "another-broker"},
	}

	object, err := MakeScaledObject(
		broker,
		[]*eventingv1.Trigger{triggerB, other, triggerA},
		"orders-broker-filter",
		Settings{Enabled: true, MinScale: 0, MaxScale: 20, PollingInterval: 10, CooldownPeriod: 30},
		MonitoringConfig{Endpoint: "nats.nats-io.svc:8222", Account: "$G"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if object.GetName() != "orders-broker-filter" || object.GetNamespace() != "shop" {
		t.Fatalf("unexpected metadata: %s/%s", object.GetNamespace(), object.GetName())
	}
	if !metav1.IsControlledBy(object, broker) {
		t.Fatal("ScaledObject should be controlled by the Broker")
	}

	targetName, _, _ := unstructured.NestedString(object.Object, "spec", "scaleTargetRef", "name")
	if targetName != "orders-broker-filter" {
		t.Fatalf("scale target = %q", targetName)
	}
	scaleTriggers, _, err := unstructured.NestedSlice(object.Object, "spec", "triggers")
	if err != nil {
		t.Fatal(err)
	}
	if len(scaleTriggers) != 2 {
		t.Fatalf("len(triggers) = %d, want 2", len(scaleTriggers))
	}

	first := scaleTriggers[0].(map[string]interface{})
	metadata := first["metadata"].(map[string]interface{})
	if metadata["consumer"] != brokerutils.TriggerConsumerName(string(triggerA.UID)) {
		t.Fatalf("first consumer = %v, want Trigger a consumer", metadata["consumer"])
	}
	if metadata["lagThreshold"] != "5" || metadata["activationLagThreshold"] != "1" {
		t.Fatalf("Trigger override was not applied: %v", metadata)
	}
	second := scaleTriggers[1].(map[string]interface{})["metadata"].(map[string]interface{})
	if second["lagThreshold"] != "20" || second["activationLagThreshold"] != "0" {
		t.Fatalf("Broker/default thresholds were not applied: %v", second)
	}
}

func TestScaledObjectName(t *testing.T) {
	short := "orders-broker-filter"
	if got := ScaledObjectName(short); got != short {
		t.Fatalf("ScaledObjectName(%q) = %q, want unchanged", short, got)
	}

	long := "a-very-long-broker-name-that-reaches-the-kubernetes-resource-name-limit-broker-filter"
	got := ScaledObjectName(long)
	if len(got) > 63 {
		t.Fatalf("len(ScaledObjectName(%q)) = %d, want at most 63 (%q)", long, len(got), got)
	}
	if got == ScaledObjectName(long+"-different") {
		t.Fatalf("different target names produced the same ScaledObject name %q", got)
	}
}
