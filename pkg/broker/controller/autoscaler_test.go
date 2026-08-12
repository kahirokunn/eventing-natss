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
	"context"
	"errors"
	"testing"

	apierrs "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"

	eventingv1 "knative.dev/eventing/pkg/apis/eventing/v1"

	"knative.dev/eventing-natss/pkg/broker/autoscaler"
)

func autoscaledBroker() *eventingv1.Broker {
	return &eventingv1.Broker{ObjectMeta: metav1.ObjectMeta{
		Namespace: testNamespace,
		Name:      testBrokerName,
		UID:       types.UID("broker-uid"),
		Annotations: map[string]string{
			autoscaler.ClassAnnotation: autoscaler.KEDAClass,
		},
	}}
}

func expectedScaledObject(t *testing.T, broker *eventingv1.Broker) *unstructured.Unstructured {
	t.Helper()
	trigger := testTrigger(testNamespace, "trigger", testBrokerName)
	trigger.UID = types.UID("trigger-uid")
	object, err := autoscaler.MakeScaledObject(
		broker,
		[]*eventingv1.Trigger{trigger},
		"test-broker-broker-filter",
		autoscaler.Settings{Enabled: true, MinScale: 0, MaxScale: 50, PollingInterval: 10, CooldownPeriod: 30},
		autoscaler.MonitoringConfig{Endpoint: "nats.nats-io.svc:8222", Account: "$G"},
	)
	if err != nil {
		t.Fatal(err)
	}
	return object
}

func TestReconcileScaledObject(t *testing.T) {
	ctx := context.Background()
	broker := autoscaledBroker()
	expected := expectedScaledObject(t, broker)
	client := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
	r := &Reconciler{dynamicClient: client}

	if err := r.reconcileScaledObject(ctx, expected, broker); err != nil {
		t.Fatalf("create: %v", err)
	}
	created, err := client.Resource(autoscaler.ScaledObjectGVR).Namespace(testNamespace).Get(ctx, expected.GetName(), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !metav1.IsControlledBy(created, broker) {
		t.Fatal("created ScaledObject is not controlled by Broker")
	}

	updatedExpected := expected.DeepCopy()
	if err := unstructured.SetNestedField(updatedExpected.Object, int64(7), "spec", "maxReplicaCount"); err != nil {
		t.Fatal(err)
	}
	if err := r.reconcileScaledObject(ctx, updatedExpected, broker); err != nil {
		t.Fatalf("update: %v", err)
	}
	updated, err := client.Resource(autoscaler.ScaledObjectGVR).Namespace(testNamespace).Get(ctx, expected.GetName(), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	max, _, _ := unstructured.NestedInt64(updated.Object, "spec", "maxReplicaCount")
	if max != 7 {
		t.Fatalf("maxReplicaCount = %d, want 7", max)
	}
	if err := unstructured.SetNestedSlice(updated.Object, []interface{}{
		map[string]interface{}{
			"type":    "Ready",
			"status":  "False",
			"reason":  "KEDAScalerFailed",
			"message": "cannot reach NATS monitoring endpoint",
		},
	}, "status", "conditions"); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Resource(autoscaler.ScaledObjectGVR).Namespace(testNamespace).Update(ctx, updated, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := r.reconcileScaledObject(ctx, updatedExpected, broker); err == nil {
		t.Fatal("expected a not-ready ScaledObject error")
	}

	targetName, _, _ := unstructured.NestedString(expected.Object, "spec", "scaleTargetRef", "name")
	if err := r.deleteScaledObject(ctx, broker, targetName); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := client.Resource(autoscaler.ScaledObjectGVR).Namespace(testNamespace).Get(ctx, expected.GetName(), metav1.GetOptions{}); !apierrs.IsNotFound(err) {
		t.Fatalf("expected NotFound after deletion, got %v", err)
	}
}

func TestReconcileScaledObjectRejectsForeignOwner(t *testing.T) {
	broker := autoscaledBroker()
	existing := expectedScaledObject(t, broker)
	existing.SetOwnerReferences(nil)
	client := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), existing)
	r := &Reconciler{dynamicClient: client}

	err := r.reconcileScaledObject(context.Background(), expectedScaledObject(t, broker), broker)
	if !errors.Is(err, errScaledObjectNotOwned) {
		t.Fatalf("error = %v, want errScaledObjectNotOwned", err)
	}
}
