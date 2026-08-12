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
	"fmt"
	"reflect"
	"time"

	apierrs "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"knative.dev/pkg/controller"

	eventingv1 "knative.dev/eventing/pkg/apis/eventing/v1"

	"knative.dev/eventing-natss/pkg/broker/autoscaler"
)

var errScaledObjectNotOwned = errors.New("scaledobject is not owned by broker")

func (r *Reconciler) reconcileScaledObject(ctx context.Context, expected *unstructured.Unstructured, broker *eventingv1.Broker) error {
	if r.dynamicClient == nil {
		return fmt.Errorf("KEDA dynamic client is not configured")
	}

	resources := r.dynamicClient.Resource(autoscaler.ScaledObjectGVR).Namespace(expected.GetNamespace())
	existing, err := resources.Get(ctx, expected.GetName(), metav1.GetOptions{})
	if err != nil && !apierrs.IsNotFound(err) {
		return fmt.Errorf("failed to get ScaledObject %s/%s: %w", expected.GetNamespace(), expected.GetName(), err)
	}
	if apierrs.IsNotFound(err) {
		if _, err := resources.Create(ctx, expected, metav1.CreateOptions{}); err != nil {
			return fmt.Errorf("failed to create ScaledObject %s/%s: %w", expected.GetNamespace(), expected.GetName(), err)
		}
		return nil
	}
	if !metav1.IsControlledBy(existing, broker) {
		return fmt.Errorf("%w: %s/%s", errScaledObjectNotOwned, existing.GetNamespace(), existing.GetName())
	}
	if err := scaledObjectReadyError(existing); err != nil {
		return err
	}

	expectedSpec, _, err := unstructured.NestedMap(expected.Object, "spec")
	if err != nil {
		return fmt.Errorf("failed to read expected ScaledObject spec: %w", err)
	}
	existingSpec, _, err := unstructured.NestedMap(existing.Object, "spec")
	if err != nil {
		return fmt.Errorf("failed to read existing ScaledObject spec: %w", err)
	}
	if equalityDeepEqual(existingSpec, expectedSpec) {
		return nil
	}

	toUpdate := existing.DeepCopy()
	if err := unstructured.SetNestedMap(toUpdate.Object, expectedSpec, "spec"); err != nil {
		return fmt.Errorf("failed to set ScaledObject spec: %w", err)
	}
	labels := toUpdate.GetLabels()
	if labels == nil {
		labels = make(map[string]string)
	}
	for key, value := range expected.GetLabels() {
		labels[key] = value
	}
	toUpdate.SetLabels(labels)
	if _, err := resources.Update(ctx, toUpdate, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("failed to update ScaledObject %s/%s: %w", expected.GetNamespace(), expected.GetName(), err)
	}
	return nil
}

func scaledObjectReadyError(object *unstructured.Unstructured) error {
	conditions, found, err := unstructured.NestedSlice(object.Object, "status", "conditions")
	if err != nil {
		return fmt.Errorf("failed to read ScaledObject %s/%s conditions: %w", object.GetNamespace(), object.GetName(), err)
	}
	if !found {
		return nil
	}
	for _, rawCondition := range conditions {
		condition, ok := rawCondition.(map[string]interface{})
		if !ok || condition["type"] != "Ready" || condition["status"] != "False" {
			continue
		}
		reason, _ := condition["reason"].(string)
		message, _ := condition["message"].(string)
		if reason == "" {
			reason = "unknown reason"
		}
		if message == "" {
			message = "KEDA reported the scaler as not ready"
		}
		return fmt.Errorf("ScaledObject %s/%s is not ready (%s): %s", object.GetNamespace(), object.GetName(), reason, message)
	}
	return nil
}

func (r *Reconciler) deleteScaledObject(ctx context.Context, broker *eventingv1.Broker, targetName string) error {
	if r.dynamicClient == nil {
		return nil
	}

	name := autoscaler.ScaledObjectName(targetName)
	resources := r.dynamicClient.Resource(autoscaler.ScaledObjectGVR).Namespace(broker.Namespace)
	existing, err := resources.Get(ctx, name, metav1.GetOptions{})
	if apierrs.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to get ScaledObject %s/%s before deletion: %w", broker.Namespace, name, err)
	}
	if !metav1.IsControlledBy(existing, broker) {
		return fmt.Errorf("%w: %s/%s", errScaledObjectNotOwned, broker.Namespace, name)
	}
	if err := resources.Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !apierrs.IsNotFound(err) {
		return fmt.Errorf("failed to delete ScaledObject %s/%s: %w", broker.Namespace, name, err)
	}
	if _, err := resources.Get(ctx, name, metav1.GetOptions{}); apierrs.IsNotFound(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("failed to confirm ScaledObject deletion %s/%s: %w", broker.Namespace, name, err)
	}
	return controller.NewRequeueAfter(time.Second)
}

// equalityDeepEqual is kept behind a small function so unstructured KEDA
// comparisons have one normalization point if the API changes representation.
func equalityDeepEqual(left, right map[string]interface{}) bool {
	return reflect.DeepEqual(left, right)
}
