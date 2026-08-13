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
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	corev1 "k8s.io/api/core/v1"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"

	eventingv1 "knative.dev/eventing/pkg/apis/eventing/v1"

	"knative.dev/eventing-natss/pkg/broker/autoscaler"
	"knative.dev/eventing-natss/pkg/broker/controller/resources"
	brokerutils "knative.dev/eventing-natss/pkg/broker/utils"
)

var errScaledObjectNotOwned = errors.New("scaledobject is not owned by broker")

const (
	autoscalerSafetyQueryConcurrency = 8
	autoscalerSafetyOperationTimeout = 5 * time.Second
	autoscalerSafetyErrorSampleLimit = 3
)

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
	if existing.GetDeletionTimestamp() == nil {
		foreground := metav1.DeletePropagationForeground
		uid := existing.GetUID()
		options := metav1.DeleteOptions{
			PropagationPolicy: &foreground,
			Preconditions:     &metav1.Preconditions{UID: &uid},
		}
		if err := resources.Delete(ctx, name, options); err != nil && !apierrs.IsNotFound(err) {
			return fmt.Errorf("failed to delete ScaledObject %s/%s: %w", broker.Namespace, name, err)
		}
	}
	remaining, err := resources.Get(ctx, name, metav1.GetOptions{})
	if apierrs.IsNotFound(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("failed to confirm ScaledObject deletion %s/%s: %w", broker.Namespace, name, err)
	}
	if remaining.GetUID() != existing.GetUID() || !metav1.IsControlledBy(remaining, broker) {
		return fmt.Errorf("%w: %s/%s changed while being deleted", errScaledObjectNotOwned, broker.Namespace, name)
	}
	for _, finalizer := range remaining.GetFinalizers() {
		if finalizer == metav1.FinalizerDeleteDependents {
			return controller.NewRequeueAfter(time.Second)
		}
	}
	return controller.NewRequeueAfter(30 * time.Second)
}

// rejectForeignScaledObject checks whether the well-known ScaledObject name is
// already controlled by another resource. Callers use this before changing the
// target Deployment so a foreign autoscaler cannot be disrupted indirectly.
// Other lookup failures are left to deleteScaledObject: restoring a Broker's
// static capacity must not depend on KEDA's API being available.
func (r *Reconciler) rejectForeignScaledObject(ctx context.Context, broker *eventingv1.Broker, targetName string) error {
	if r.dynamicClient == nil {
		return nil
	}

	name := autoscaler.ScaledObjectName(targetName)
	object, err := r.dynamicClient.Resource(autoscaler.ScaledObjectGVR).Namespace(broker.Namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil
	}
	if !metav1.IsControlledBy(object, broker) {
		return fmt.Errorf("%w: %s/%s", errScaledObjectNotOwned, broker.Namespace, name)
	}
	return nil
}

// reconcileAutoscalerSafetyWakeup is a live fallback for a stopped KEDA
// control plane. KEDA's native fallback covers scaler errors, but it cannot
// run after the operator itself stops. In that case the Broker controller can
// still read JetStream over its data connection and wake a zero-replica filter
// when one of its consumers has actionable backlog.
func (r *Reconciler) reconcileAutoscalerSafetyWakeup(ctx context.Context, broker *eventingv1.Broker, triggers []*eventingv1.Trigger, streamName string, settings autoscaler.Settings) error {
	operationCtx, cancel := context.WithTimeout(ctx, autoscalerSafetyOperationTimeout)
	defer cancel()

	deployments := r.kubeClientSet.AppsV1().Deployments(broker.Namespace)
	targetName := resources.FilterName(broker.Name)
	scale, err := deployments.GetScale(operationCtx, targetName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get filter scale for autoscaler safety check: %w", err)
	}
	if scale.Spec.Replicas != 0 {
		return nil
	}

	if err := validateAutoscalerSafetyTriggers(operationCtx, triggers, broker.Annotations); err != nil {
		if parentErr := ctx.Err(); parentErr != nil {
			return parentErr
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			logging.FromContext(ctx).Warnw("Timed out validating Trigger thresholds for autoscaler safety wakeup", "error", err)
			return nil
		}
		return err
	}

	result, err := r.queryAutoscalerSafetyBacklog(operationCtx, streamName, triggers, broker.Annotations)
	if err != nil {
		if parentErr := ctx.Err(); parentErr != nil {
			return parentErr
		}
		// This check supplements KEDA and must not make a healthy Broker
		// unavailable when one consumer is transiently missing or NATS is slow.
		// Without a positive backlog observation, leave the scale unchanged and
		// retry on the normal autoscaler reconciliation interval.
		logging.FromContext(ctx).Warnw("Could not complete JetStream consumer lag checks for autoscaler safety wakeup", "error", err)
		return nil
	}
	if result == nil {
		return nil
	}

	// Refresh the scale immediately before writing. If KEDA recovered and
	// already activated the target, do not overwrite its desired count.
	scale, err = deployments.GetScale(operationCtx, targetName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to refresh filter scale before autoscaler safety wakeup: %w", err)
	}
	if scale.Spec.Replicas != 0 {
		return nil
	}
	scale.Spec.Replicas = int32(autoscaler.FallbackReplicaCount(settings.MinScale))
	if _, err := deployments.UpdateScale(operationCtx, targetName, scale, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("failed to wake filter from zero after detecting JetStream backlog: %w", err)
	}

	message := fmt.Sprintf("Woke filter from zero because Trigger %s has JetStream lag %d above activation threshold %d", result.trigger.Name, result.lag, result.activationThreshold)
	logging.FromContext(ctx).Warnw(message, "trigger", result.trigger.Name, "consumer", result.consumer)
	controller.GetEventRecorder(ctx).Event(broker, corev1.EventTypeWarning, ReasonAutoscalerSafetyWakeup, message)
	return nil
}

type autoscalerSafetyQuery struct {
	trigger             *eventingv1.Trigger
	consumer            string
	activationThreshold int64
}

type autoscalerSafetyResult struct {
	autoscalerSafetyQuery
	lag uint64
	err error
}

func validateAutoscalerSafetyTriggers(ctx context.Context, triggers []*eventingv1.Trigger, brokerAnnotations map[string]string) error {
	for _, trigger := range triggers {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, _, err := autoscaler.ResolveLagThresholds(trigger.Annotations, brokerAnnotations); err != nil {
			return fmt.Errorf("failed to resolve activation threshold for Trigger %s/%s: %w", trigger.Namespace, trigger.Name, err)
		}
	}
	return ctx.Err()
}

// queryAutoscalerSafetyBacklog bounds the time and concurrency spent checking
// one Broker. A positive result cancels the remaining JetStream requests, but
// all workers are joined before returning so a reconciliation never leaves
// query goroutines behind.
func (r *Reconciler) queryAutoscalerSafetyBacklog(ctx context.Context, streamName string, triggers []*eventingv1.Trigger, brokerAnnotations map[string]string) (*autoscalerSafetyResult, error) {
	if len(triggers) == 0 {
		return nil, nil
	}

	queryCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	workerCount := min(len(triggers), autoscalerSafetyQueryConcurrency)
	results := make(chan autoscalerSafetyResult, workerCount)
	var next atomic.Int64

	var workers sync.WaitGroup
	workers.Add(workerCount)
	for range workerCount {
		go func() {
			defer workers.Done()
			for {
				if queryCtx.Err() != nil {
					return
				}
				index := int(next.Add(1) - 1)
				if index >= len(triggers) {
					return
				}
				if queryCtx.Err() != nil {
					return
				}

				trigger := triggers[index]
				_, activationThreshold, err := autoscaler.ResolveLagThresholds(trigger.Annotations, brokerAnnotations)
				if err != nil {
					// Trigger objects from informer listers are immutable. This was
					// validated before workers started, so reaching this branch would
					// indicate a caller violated that contract.
					results <- autoscalerSafetyResult{err: fmt.Errorf("trigger %s/%s changed while checking autoscaler safety: %w", trigger.Namespace, trigger.Name, err)}
					continue
				}
				query := autoscalerSafetyQuery{
					trigger:             trigger,
					consumer:            brokerutils.TriggerConsumerName(string(trigger.UID)),
					activationThreshold: activationThreshold,
				}
				result := autoscalerSafetyResult{autoscalerSafetyQuery: query}
				info, err := r.js.ConsumerInfo(streamName, query.consumer, nats.Context(queryCtx))
				switch {
				case err != nil:
					result.err = fmt.Errorf("failed to read JetStream consumer %s for Trigger %s/%s: %w", query.consumer, query.trigger.Namespace, query.trigger.Name, err)
				case info == nil:
					result.err = fmt.Errorf("JetStream returned no consumer information for Trigger %s/%s consumer %s", query.trigger.Namespace, query.trigger.Name, query.consumer)
				default:
					result.lag = consumerLag(info)
					if result.lag > uint64(query.activationThreshold) {
						// Cancel here, before publishing the result, so this worker
						// cannot take another job while the coordinator is waking up.
						cancel()
					}
				}
				results <- result
			}
		}()
	}
	go func() {
		workers.Wait()
		close(results)
	}()

	var winner *autoscalerSafetyResult
	errorCount := 0
	errorSamples := make([]error, 0, autoscalerSafetyErrorSampleLimit)
	for result := range results {
		if result.err != nil {
			errorCount++
			if len(errorSamples) < autoscalerSafetyErrorSampleLimit {
				errorSamples = append(errorSamples, result.err)
			}
			continue
		}
		if winner == nil && result.lag > uint64(result.activationThreshold) {
			matched := result
			winner = &matched
		}
	}
	if winner != nil {
		return winner, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if errorCount > 0 {
		return nil, fmt.Errorf("%d JetStream consumer lag checks failed (showing %d): %w", errorCount, len(errorSamples), errors.Join(errorSamples...))
	}
	return nil, nil
}

// consumerLag matches KEDA's NATS JetStream scaler: undelivered messages and
// delivered-but-unacknowledged messages both require a running filter.
func consumerLag(info *nats.ConsumerInfo) uint64 {
	lag := info.NumPending
	if info.NumAckPending <= 0 {
		return lag
	}
	ackPending := uint64(info.NumAckPending)
	if ^uint64(0)-lag < ackPending {
		return ^uint64(0)
	}
	return lag + ackPending
}

// equalityDeepEqual is kept behind a small function so unstructured KEDA
// comparisons have one normalization point if the API changes representation.
func equalityDeepEqual(left, right map[string]interface{}) bool {
	return reflect.DeepEqual(left, right)
}
