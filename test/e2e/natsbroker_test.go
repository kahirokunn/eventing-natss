//go:build e2e
// +build e2e

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

package e2e

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	cetypes "github.com/cloudevents/sdk-go/v2/types"
	// For our e2e testing, we want this linked first so that our
	// system namespace environment variable is defaulted prior to
	// logstream initialization.
	authorizationv1 "k8s.io/api/authorization/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/ptr"
	_ "knative.dev/eventing-natss/test/defaultsystem"
	eventingv1 "knative.dev/eventing/pkg/apis/eventing/v1"
	eventingclient "knative.dev/eventing/pkg/client/injection/client"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	dynamicclient "knative.dev/pkg/injection/clients/dynamicclient"
	"knative.dev/pkg/system"
	_ "knative.dev/pkg/system/testing"
	"knative.dev/reconciler-test/pkg/environment"
	"knative.dev/reconciler-test/pkg/eventshub"
	"knative.dev/reconciler-test/pkg/feature"
	"knative.dev/reconciler-test/pkg/k8s"
	"knative.dev/reconciler-test/pkg/knative"

	brokerautoscaler "knative.dev/eventing-natss/pkg/broker/autoscaler"
	brokeroidc "knative.dev/eventing-natss/pkg/broker/oidc"
	brokerutils "knative.dev/eventing-natss/pkg/broker/utils"
	"knative.dev/eventing-natss/test/e2e/config/autoscaling"
	"knative.dev/eventing-natss/test/e2e/config/deadletter"
	"knative.dev/eventing-natss/test/e2e/config/filtering"
	"knative.dev/eventing-natss/test/e2e/config/natsbroker"
)

const e2eOIDCAudience = "https://eventing-natss-e2e.example"

// hasEventType returns an EventInfoMatcher that matches CloudEvents of the given type.
func hasEventType(eventType string) eventshub.EventInfoMatcher {
	return func(info eventshub.EventInfo) error {
		if info.Event == nil {
			return fmt.Errorf("event is nil")
		}
		if info.Event.Type() != eventType {
			return fmt.Errorf("event type %q, want %q", info.Event.Type(), eventType)
		}
		return nil
	}
}

func hasEventIndex(index int) eventshub.EventInfoMatcher {
	return func(info eventshub.EventInfo) error {
		if info.Event == nil {
			return fmt.Errorf("event is nil")
		}
		raw, ok := info.Event.Extensions()["index"]
		if !ok {
			return fmt.Errorf("event has no index extension")
		}
		got, err := cetypes.ToInteger(raw)
		if err != nil {
			return fmt.Errorf("event index %v is not an integer: %w", raw, err)
		}
		if got != int32(index) {
			return fmt.Errorf("event index %d, want %d", got, index)
		}
		return nil
	}
}

func assertLogicalEventBatch(ctx context.Context, t feature.T, store *eventshub.Store, eventType string, count int) {
	for index := 0; index < count; index++ {
		store.AssertAtLeast(ctx, t, 1, hasEventType(eventType), hasEventIndex(index))
	}
}

func TestNatsBrokerKEDAAutoscaling(t *testing.T) {
	ctx, env := global.Environment(
		environment.Managed(t),
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithObservabilityConfig,
		k8s.WithEventListener,
	)
	env.Test(ctx, t, namedRecorderFeature("autoscale-recorder-a", eventshub.ResponseWaitTime(time.Second), eventshub.OIDCReceiverAudience(e2eOIDCAudience)))
	env.Test(ctx, t, namedRecorderFeature("autoscale-recorder-b", eventshub.OIDCReceiverAudience(e2eOIDCAudience)))
	env.Test(ctx, t, NatsBrokerKEDAAutoscalingFeature())
}

func NatsBrokerKEDAAutoscalingFeature() *feature.Feature {
	const (
		eventCount       = 30
		outageEventCount = 5
		initialEventType = "knative.natsbroker.autoscaling.initial"
		outageEventType  = "knative.natsbroker.autoscaling.keda-outage"
	)
	f := new(feature.Feature)
	f.Setup("prepare autoscaled brokers and initial backlog", func(ctx context.Context, t feature.T) {
		autoscaling.InstallBrokersAndTriggers()(ctx, t)
		AllGoReady(ctx, t)
		waitForFilterReplicas("autoscale-broker-a-broker-filter", 0)(ctx, t)
		waitForFilterReplicas("autoscale-broker-b-broker-filter", 0)(ctx, t)
		autoscaling.InstallProducerWithEventType("autoscale-producer", eventCount, initialEventType)(ctx, t)
	})

	f.Alpha("NATS Broker filters scale safely from zero").Must("scale, fall back, recover, and preserve readiness", func(ctx context.Context, t feature.T) {
		waitForDeployment(ctx, t, "autoscale-broker-a-broker-filter", func(replicas int32) bool { return replicas >= 2 })
		assertFilterTokenRequestAuthority(ctx, t, "autoscale-broker-a", true)
		assertFilterReplicas(ctx, t, "autoscale-broker-b-broker-filter", 0)
		store := eventshub.StoreFromContext(ctx, "autoscale-recorder-a")
		assertLogicalEventBatch(ctx, t, store, initialEventType, eventCount)
		waitForFilterReplicas("autoscale-broker-a-broker-filter", 0)(ctx, t)

		verifyKEDANativeFallbackFromZero(ctx, t)

		withDeploymentsStopped(ctx, t, []deploymentRef{
			{namespace: "keda", name: "keda-operator"},
			{namespace: "keda", name: "keda-metrics-apiserver"},
		}, func() {
			// Capture a newly failed outage step before withDeploymentsStopped
			// restores KEDA. Successful runs avoid the extra API and log traffic.
			failedBefore := t.Failed()
			defer func() {
				if !shouldCaptureOutageSnapshot(failedBefore, t.Failed()) {
					return
				}
				t.Logf("KEDA outage workload snapshot before restore:\n%s", outageWorkloadStatusSnapshot(ctx, []string{
					environment.FromContext(ctx).Namespace(),
					"keda",
				}))
			}()
			autoscaling.InstallProducerWithEventType("autoscale-outage-producer", outageEventCount, outageEventType)(ctx, t)
			waitForDeployment(ctx, t, "autoscale-broker-a-broker-filter", func(replicas int32) bool { return replicas >= 1 })
			assertFilterReplicas(ctx, t, "autoscale-broker-b-broker-filter", 0)
			assertLogicalEventBatch(ctx, t, store, outageEventType, outageEventCount)
		})

		waitForFilterReplicas("autoscale-broker-a-broker-filter", 0)(ctx, t)
		waitForFilterReplicas("autoscale-broker-b-broker-filter", 0)(ctx, t)
		AllGoReady(ctx, t)

		// These scenarios mutate the same Broker A/B autoscaling resources as
		// the outage checks above. Keep them in this Must so reconciler-test
		// cannot execute them concurrently: outage recovery -> multi-trigger ->
		// opt-out/finalizer regression.
		verifyMultiTriggerScaledObject(ctx, t)
		verifyAutoscalerClassRemovalWithTerminatingScaledObject(ctx, t)
	})
	return f
}

func shouldCaptureOutageSnapshot(failedBefore, failedAfter bool) bool {
	return !failedBefore && failedAfter
}

func verifyMultiTriggerScaledObject(ctx context.Context, t feature.T) {
	const (
		brokerName       = "autoscale-broker-b"
		initialName      = "autoscale-trigger-b"
		additionalName   = "autoscale-trigger-b-extra"
		filterName       = "autoscale-broker-b-broker-filter"
		hpaName          = "keda-hpa-autoscale-broker-b-broker-filter"
		recorderName     = "autoscale-recorder-b"
		cleanupTimeout   = 2 * time.Minute
		reconcileTimeout = 3 * time.Minute
		scenarioTimeout  = 2 * time.Minute
	)
	scenarioCtx, cancelScenario := context.WithTimeout(ctx, scenarioTimeout)
	defer cancelScenario()
	ctx = scenarioCtx
	namespace := environment.FromContext(ctx).Namespace()
	triggers := eventingclient.Get(ctx).EventingV1().Triggers(namespace)
	broker, err := eventingclient.Get(ctx).EventingV1().Brokers(namespace).Get(ctx, brokerName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	initial, err := triggers.Get(ctx, initialName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := waitForTriggerUIDReady(ctx, namespace, initialName, initial.UID, reconcileTimeout); err != nil {
		t.Fatal(err)
	}
	if err := waitForAutoscalerTriggerShape(ctx, namespace, broker.UID, filterName, hpaName, []string{
		brokerutils.TriggerConsumerName(string(initial.UID)),
	}, reconcileTimeout); err != nil {
		t.Fatal(err)
	}
	additional, err := triggers.Create(ctx, &eventingv1.Trigger{
		ObjectMeta: metav1.ObjectMeta{
			Name:      additionalName,
			Namespace: namespace,
			Annotations: map[string]string{
				"natsjetstream.eventing.knative.dev/fetch-batch-size": "1",
				"natsjetstream.eventing.knative.dev/max-concurrency":  "1",
			},
		},
		Spec: eventingv1.TriggerSpec{
			Broker: brokerName,
			Subscriber: duckv1.Destination{
				Audience: ptr.To(e2eOIDCAudience),
				Ref: &duckv1.KReference{
					APIVersion: "v1",
					Kind:       "Service",
					Name:       recorderName,
				},
			},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	deleted := false
	defer func() {
		if deleted {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
		defer cancel()
		if err := deleteTriggerAndWait(cleanupCtx, namespace, additionalName, additional.UID); err != nil {
			t.Errorf("failed to clean up additional Trigger: %v", err)
		}
	}()

	if err := waitForTriggerUIDReady(ctx, namespace, additionalName, additional.UID, reconcileTimeout); err != nil {
		t.Fatal(err)
	}
	if err := waitForAutoscalerTriggerShape(ctx, namespace, broker.UID, filterName, hpaName, []string{
		brokerutils.TriggerConsumerName(string(initial.UID)),
		brokerutils.TriggerConsumerName(string(additional.UID)),
	}, reconcileTimeout); err != nil {
		t.Fatal(err)
	}
	if err := deleteTriggerAndWait(ctx, namespace, additionalName, additional.UID); err != nil {
		t.Fatal(err)
	}
	deleted = true
	if err := waitForTriggerUIDReady(ctx, namespace, initialName, initial.UID, reconcileTimeout); err != nil {
		t.Fatal(err)
	}
	if err := waitForAutoscalerTriggerShape(ctx, namespace, broker.UID, filterName, hpaName, []string{
		brokerutils.TriggerConsumerName(string(initial.UID)),
	}, reconcileTimeout); err != nil {
		t.Fatal(err)
	}
}

func verifyAutoscalerClassRemovalWithTerminatingScaledObject(ctx context.Context, t feature.T) {
	const (
		brokerName       = "autoscale-broker-a"
		triggerName      = "autoscale-trigger-a"
		filterName       = "autoscale-broker-a-broker-filter"
		hpaName          = "keda-hpa-autoscale-broker-a-broker-filter"
		holdFinalizer    = "e2e.eventing.knative.dev/hold-scaledobject"
		hpaHoldFinalizer = "e2e.eventing.knative.dev/hold-hpa"
		eventCount       = 3
		eventType        = "knative.natsbroker.autoscaling.class-removal"
		reconcileTimeout = 3 * time.Minute
		cleanupTimeout   = 3 * time.Minute
		cleanupPhase     = 45 * time.Second
		scenarioTimeout  = 4 * time.Minute
	)
	scenarioCtx, cancelScenario := context.WithTimeout(ctx, scenarioTimeout)
	defer cancelScenario()
	ctx = scenarioCtx
	namespace := environment.FromContext(ctx).Namespace()
	if err := waitForDeploymentReplicas(ctx, namespace, filterName, 0, false, reconcileTimeout); err != nil {
		t.Fatal(err)
	}
	broker, err := eventingclient.Get(ctx).EventingV1().Brokers(namespace).Get(ctx, brokerName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	originalClass, hadOriginalClass := broker.Annotations[brokerautoscaler.ClassAnnotation]
	if !hadOriginalClass || originalClass != brokerautoscaler.KEDAClass {
		t.Fatalf("Broker autoscaler class = %q, want %q", originalClass, brokerautoscaler.KEDAClass)
	}
	trigger, err := eventingclient.Get(ctx).EventingV1().Triggers(namespace).Get(ctx, triggerName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if err := waitForAutoscalerTriggerShape(ctx, namespace, broker.UID, filterName, hpaName, []string{
		brokerutils.TriggerConsumerName(string(trigger.UID)),
	}, reconcileTimeout); err != nil {
		t.Fatal(err)
	}
	scaledObjectUID, err := addScaledObjectFinalizer(ctx, namespace, filterName, holdFinalizer)
	if err != nil {
		t.Fatal(err)
	}
	scaledObjectHoldAdded := true
	hpaUID, err := addHPAFinalizer(ctx, namespace, hpaName, hpaHoldFinalizer)
	if err != nil {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
		defer cancel()
		if cleanupErr := removeScaledObjectFinalizer(cleanupCtx, namespace, filterName, scaledObjectUID, holdFinalizer); cleanupErr != nil {
			t.Errorf("failed to release ScaledObject hold finalizer after HPA setup failure: %v", cleanupErr)
		}
		t.Fatal(err)
	}
	hpaHoldAdded := true
	classRemoved := false
	classRestored := false
	defer func() {
		cleanupBase := context.WithoutCancel(ctx)
		if !classRemoved {
			if hpaHoldAdded {
				cleanupCtx, cancel := context.WithTimeout(cleanupBase, cleanupPhase)
				err := removeHPAFinalizer(cleanupCtx, namespace, hpaName, hpaUID, hpaHoldFinalizer)
				cancel()
				if err != nil {
					t.Errorf("failed to release HPA hold finalizer: %v", err)
				}
			}
			if scaledObjectHoldAdded {
				cleanupCtx, cancel := context.WithTimeout(cleanupBase, cleanupPhase)
				err := removeScaledObjectFinalizer(cleanupCtx, namespace, filterName, scaledObjectUID, holdFinalizer)
				cancel()
				if err != nil {
					t.Errorf("failed to release ScaledObject hold finalizer: %v", err)
				}
			}
			return
		}
		if hpaHoldAdded || scaledObjectHoldAdded {
			if err := releaseAutoscalerHolds(cleanupBase, cleanupPhase, namespace, hpaName, hpaUID, hpaHoldFinalizer, filterName, scaledObjectUID, holdFinalizer); err != nil {
				t.Errorf("failed to release autoscaler test holds in order: %v", err)
				return
			}
		}
		if classRemoved && !classRestored {
			cleanupCtx, cancel := context.WithTimeout(cleanupBase, cleanupPhase)
			defer cancel()
			if err := setBrokerAnnotation(cleanupCtx, namespace, brokerName, broker.UID, brokerautoscaler.ClassAnnotation, &originalClass); err != nil {
				t.Errorf("failed to restore Broker autoscaler class: %v", err)
			}
		}
	}()

	if err := setBrokerAnnotation(ctx, namespace, brokerName, broker.UID, brokerautoscaler.ClassAnnotation, nil); err != nil {
		t.Fatal(err)
	}
	classRemoved = true
	if err := waitForAutoscalersTerminating(ctx, namespace, filterName, scaledObjectUID, holdFinalizer, hpaName, hpaUID, hpaHoldFinalizer, reconcileTimeout); err != nil {
		t.Fatal(err)
	}
	if err := updateDeploymentScale(ctx, namespace, filterName, broker.UID, 0); err != nil {
		t.Fatal(err)
	}
	if err := waitForStaticCapacityWhileAutoscalersTerminating(ctx, namespace, filterName, scaledObjectUID, holdFinalizer, hpaName, hpaUID, hpaHoldFinalizer, reconcileTimeout); err != nil {
		t.Fatal(err)
	}
	if err := releaseAutoscalerHolds(ctx, cleanupPhase, namespace, hpaName, hpaUID, hpaHoldFinalizer, filterName, scaledObjectUID, holdFinalizer); err != nil {
		t.Fatal(err)
	}
	hpaHoldAdded = false
	scaledObjectHoldAdded = false
	if err := waitForBrokerReady(ctx, namespace, brokerName, reconcileTimeout); err != nil {
		t.Fatal(err)
	}
	if err := waitForDeploymentReplicas(ctx, namespace, filterName, 1, true, reconcileTimeout); err != nil {
		t.Fatal(err)
	}

	autoscaling.InstallProducerWithEventType("autoscale-class-removal-producer", eventCount, eventType)(ctx, t)
	assertLogicalEventBatch(ctx, t, eventshub.StoreFromContext(ctx, "autoscale-recorder-a"), eventType, eventCount)

	if err := setBrokerAnnotation(ctx, namespace, brokerName, broker.UID, brokerautoscaler.ClassAnnotation, &originalClass); err != nil {
		t.Fatal(err)
	}
	classRestored = true
	if err := waitForAutoscalerTriggerShape(ctx, namespace, broker.UID, filterName, hpaName, []string{
		brokerutils.TriggerConsumerName(string(trigger.UID)),
	}, reconcileTimeout); err != nil {
		t.Fatal(err)
	}
	if err := waitForBrokerReady(ctx, namespace, brokerName, reconcileTimeout); err != nil {
		t.Fatal(err)
	}
	if err := waitForDeploymentReplicas(ctx, namespace, filterName, 0, false, reconcileTimeout); err != nil {
		t.Fatal(err)
	}
}

func waitForTriggerUIDReady(ctx context.Context, namespace, name string, uid types.UID, timeout time.Duration) error {
	var last string
	err := wait.PollUntilContextTimeout(ctx, time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		trigger, err := eventingclient.Get(ctx).EventingV1().Triggers(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			last = err.Error()
			return false, nil
		}
		if trigger.UID != uid {
			return false, fmt.Errorf("Trigger UID changed from %q to %q", uid, trigger.UID)
		}
		last = fmt.Sprintf("generation=%d observedGeneration=%d ready=%t", trigger.Generation, trigger.Status.ObservedGeneration, trigger.Status.IsReady())
		return trigger.Status.ObservedGeneration == trigger.Generation && trigger.Status.IsReady(), nil
	})
	if err != nil {
		return fmt.Errorf("Trigger %s/%s UID %q did not become ready (%s): %w", namespace, name, uid, last, err)
	}
	return nil
}

func deleteTriggerAndWait(ctx context.Context, namespace, name string, uid types.UID) error {
	uidPrecondition := uid
	err := eventingclient.Get(ctx).EventingV1().Triggers(namespace).Delete(ctx, name, metav1.DeleteOptions{
		Preconditions: &metav1.Preconditions{UID: &uidPrecondition},
	})
	if err != nil && !apierrs.IsNotFound(err) {
		return fmt.Errorf("delete Trigger %s/%s: %w", namespace, name, err)
	}
	var lastErr error
	err = wait.PollUntilContextTimeout(ctx, time.Second, 2*time.Minute, true, func(ctx context.Context) (bool, error) {
		_, getErr := eventingclient.Get(ctx).EventingV1().Triggers(namespace).Get(ctx, name, metav1.GetOptions{})
		if apierrs.IsNotFound(getErr) {
			return true, nil
		}
		lastErr = getErr
		return false, nil
	})
	if err != nil {
		return fmt.Errorf("Trigger %s/%s was not deleted (last error: %v): %w", namespace, name, lastErr, err)
	}
	return nil
}

func waitForAutoscalerTriggerShape(ctx context.Context, namespace string, brokerUID types.UID, scaledObjectName, hpaName string, expected []string, timeout time.Duration) error {
	want := append([]string(nil), expected...)
	sort.Strings(want)
	var lastConsumers []string
	var lastMetrics []string
	var lastErr error
	err := wait.PollUntilContextTimeout(ctx, time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		object, err := dynamicclient.Get(ctx).Resource(brokerautoscaler.ScaledObjectGVR).Namespace(namespace).Get(ctx, scaledObjectName, metav1.GetOptions{})
		if err != nil {
			lastErr = err
			return false, nil
		}
		if object.GetDeletionTimestamp() != nil {
			lastErr = fmt.Errorf("ScaledObject is terminating")
			return false, nil
		}
		if err := requireControllerOwner(object.GetOwnerReferences(), "eventing.knative.dev/v1", "Broker", brokerUID); err != nil {
			lastErr = fmt.Errorf("ScaledObject owner: %w", err)
			return false, nil
		}
		if err := requireUnstructuredScaleTarget(object, scaledObjectName); err != nil {
			lastErr = err
			return false, nil
		}
		consumers, err := scaledObjectConsumers(object)
		if err != nil {
			lastErr = err
			return false, nil
		}
		sort.Strings(consumers)
		lastConsumers = consumers
		if !stringSlicesEqual(consumers, want) {
			lastErr = nil
			return false, nil
		}
		ready, err := scaledObjectReadyTrue(object)
		if err != nil {
			lastErr = err
			return false, nil
		}
		if !ready {
			lastErr = fmt.Errorf("ScaledObject Ready condition is not True")
			return false, nil
		}
		hpa, err := kubeclient.Get(ctx).AutoscalingV2().HorizontalPodAutoscalers(namespace).Get(ctx, hpaName, metav1.GetOptions{})
		if err != nil {
			lastErr = err
			return false, nil
		}
		if err := requireControllerOwner(hpa.OwnerReferences, "keda.sh/v1alpha1", "ScaledObject", object.GetUID()); err != nil {
			lastErr = fmt.Errorf("HPA owner: %w", err)
			return false, nil
		}
		if hpa.Spec.ScaleTargetRef.APIVersion != "apps/v1" || hpa.Spec.ScaleTargetRef.Kind != "Deployment" || hpa.Spec.ScaleTargetRef.Name != scaledObjectName {
			lastErr = fmt.Errorf("HPA scaleTargetRef = %s %s/%s, want apps/v1 Deployment/%s", hpa.Spec.ScaleTargetRef.APIVersion, hpa.Spec.ScaleTargetRef.Kind, hpa.Spec.ScaleTargetRef.Name, scaledObjectName)
			return false, nil
		}
		metrics, err := externalMetricNames(hpa)
		if err != nil {
			lastErr = err
			return false, nil
		}
		sort.Strings(metrics)
		lastMetrics = metrics
		lastErr = nil
		return len(metrics) == len(want), nil
	})
	if err != nil {
		return fmt.Errorf("autoscaler %s/%s consumers = %v, want %v; HPA %s metrics = %v (last error: %v): %w", namespace, scaledObjectName, lastConsumers, want, hpaName, lastMetrics, lastErr, err)
	}
	return nil
}

func requireControllerOwner(ownerReferences []metav1.OwnerReference, apiVersion, kind string, uid types.UID) error {
	for _, owner := range ownerReferences {
		if owner.Controller != nil && *owner.Controller {
			if owner.APIVersion != apiVersion || owner.Kind != kind || owner.UID != uid {
				return fmt.Errorf("controller owner = %s %s UID %q, want %s %s UID %q", owner.APIVersion, owner.Kind, owner.UID, apiVersion, kind, uid)
			}
			return nil
		}
	}
	return fmt.Errorf("controller owner is missing, want %s %s UID %q", apiVersion, kind, uid)
}

func requireUnstructuredScaleTarget(object *unstructured.Unstructured, name string) error {
	apiVersion, _, _ := unstructured.NestedString(object.Object, "spec", "scaleTargetRef", "apiVersion")
	kind, _, _ := unstructured.NestedString(object.Object, "spec", "scaleTargetRef", "kind")
	targetName, _, _ := unstructured.NestedString(object.Object, "spec", "scaleTargetRef", "name")
	if apiVersion != "apps/v1" || kind != "Deployment" || targetName != name {
		return fmt.Errorf("ScaledObject scaleTargetRef = %s %s/%s, want apps/v1 Deployment/%s", apiVersion, kind, targetName, name)
	}
	return nil
}

func scaledObjectReadyTrue(object *unstructured.Unstructured) (bool, error) {
	conditions, found, err := unstructured.NestedSlice(object.Object, "status", "conditions")
	if err != nil {
		return false, fmt.Errorf("read status.conditions: %w", err)
	}
	if !found {
		return false, nil
	}
	for index, rawCondition := range conditions {
		condition, ok := rawCondition.(map[string]interface{})
		if !ok {
			return false, fmt.Errorf("status.conditions[%d] has type %T", index, rawCondition)
		}
		if condition["type"] == "Ready" {
			return condition["status"] == "True", nil
		}
	}
	return false, nil
}

func externalMetricNames(hpa *autoscalingv2.HorizontalPodAutoscaler) ([]string, error) {
	names := make([]string, 0, len(hpa.Spec.Metrics))
	seen := make(map[string]struct{}, len(hpa.Spec.Metrics))
	for index, metric := range hpa.Spec.Metrics {
		if metric.Type != autoscalingv2.ExternalMetricSourceType || metric.External == nil {
			return nil, fmt.Errorf("HPA metric %d type = %q, want External", index, metric.Type)
		}
		name := metric.External.Metric.Name
		if name == "" {
			return nil, fmt.Errorf("HPA external metric %d has an empty name", index)
		}
		if _, duplicate := seen[name]; duplicate {
			return nil, fmt.Errorf("HPA external metric name %q is duplicated", name)
		}
		seen[name] = struct{}{}
		names = append(names, name)
	}
	return names, nil
}

func scaledObjectConsumers(object *unstructured.Unstructured) ([]string, error) {
	triggers, found, err := unstructured.NestedSlice(object.Object, "spec", "triggers")
	if err != nil {
		return nil, fmt.Errorf("read spec.triggers: %w", err)
	}
	if !found {
		return nil, fmt.Errorf("spec.triggers is missing")
	}
	consumers := make([]string, 0, len(triggers))
	for index, rawTrigger := range triggers {
		trigger, ok := rawTrigger.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("spec.triggers[%d] has type %T", index, rawTrigger)
		}
		metadata, ok := trigger["metadata"].(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("spec.triggers[%d].metadata has type %T", index, trigger["metadata"])
		}
		consumer, ok := metadata["consumer"].(string)
		if !ok || consumer == "" {
			return nil, fmt.Errorf("spec.triggers[%d].metadata.consumer is missing or not a string", index)
		}
		consumers = append(consumers, consumer)
	}
	return consumers, nil
}

func stringSlicesEqual(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func addScaledObjectFinalizer(ctx context.Context, namespace, name, finalizer string) (types.UID, error) {
	resources := dynamicclient.Get(ctx).Resource(brokerautoscaler.ScaledObjectGVR).Namespace(namespace)
	var uid types.UID
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		object, err := resources.Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if object.GetDeletionTimestamp() != nil {
			return fmt.Errorf("ScaledObject %s/%s is already terminating", namespace, name)
		}
		uid = object.GetUID()
		for _, existing := range object.GetFinalizers() {
			if existing == finalizer {
				return nil
			}
		}
		object = object.DeepCopy()
		object.SetFinalizers(append(object.GetFinalizers(), finalizer))
		updated, err := resources.Update(ctx, object, metav1.UpdateOptions{})
		if err == nil {
			uid = updated.GetUID()
		}
		return err
	})
	if err != nil {
		return "", fmt.Errorf("add finalizer to ScaledObject %s/%s: %w", namespace, name, err)
	}
	return uid, nil
}

func removeScaledObjectFinalizer(ctx context.Context, namespace, name string, uid types.UID, finalizer string) error {
	resources := dynamicclient.Get(ctx).Resource(brokerautoscaler.ScaledObjectGVR).Namespace(namespace)
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		object, err := resources.Get(ctx, name, metav1.GetOptions{})
		if apierrs.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if object.GetUID() != uid {
			return fmt.Errorf("ScaledObject UID changed from %q to %q", uid, object.GetUID())
		}
		finalizers := object.GetFinalizers()
		filtered := make([]string, 0, len(finalizers))
		found := false
		for _, existing := range finalizers {
			if existing == finalizer {
				found = true
				continue
			}
			filtered = append(filtered, existing)
		}
		if !found {
			return nil
		}
		object = object.DeepCopy()
		object.SetFinalizers(filtered)
		_, err = resources.Update(ctx, object, metav1.UpdateOptions{})
		return err
	})
	if err != nil {
		return fmt.Errorf("remove finalizer from ScaledObject %s/%s: %w", namespace, name, err)
	}
	return nil
}

func addHPAFinalizer(ctx context.Context, namespace, name, finalizer string) (types.UID, error) {
	hpas := kubeclient.Get(ctx).AutoscalingV2().HorizontalPodAutoscalers(namespace)
	var uid types.UID
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		hpa, err := hpas.Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if hpa.DeletionTimestamp != nil {
			return fmt.Errorf("HPA %s/%s is already terminating", namespace, name)
		}
		uid = hpa.UID
		if containsString(hpa.Finalizers, finalizer) {
			return nil
		}
		hpa = hpa.DeepCopy()
		hpa.Finalizers = append(hpa.Finalizers, finalizer)
		updated, err := hpas.Update(ctx, hpa, metav1.UpdateOptions{})
		if err == nil {
			uid = updated.UID
		}
		return err
	})
	if err != nil {
		return "", fmt.Errorf("add finalizer to HPA %s/%s: %w", namespace, name, err)
	}
	return uid, nil
}

func removeHPAFinalizer(ctx context.Context, namespace, name string, uid types.UID, finalizer string) error {
	hpas := kubeclient.Get(ctx).AutoscalingV2().HorizontalPodAutoscalers(namespace)
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		hpa, err := hpas.Get(ctx, name, metav1.GetOptions{})
		if apierrs.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if hpa.UID != uid {
			return fmt.Errorf("HPA UID changed from %q to %q", uid, hpa.UID)
		}
		if !containsString(hpa.Finalizers, finalizer) {
			return nil
		}
		hpa = hpa.DeepCopy()
		hpa.Finalizers = removeString(hpa.Finalizers, finalizer)
		_, err = hpas.Update(ctx, hpa, metav1.UpdateOptions{})
		return err
	})
	if err != nil {
		return fmt.Errorf("remove finalizer from HPA %s/%s: %w", namespace, name, err)
	}
	return nil
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func removeString(values []string, target string) []string {
	filtered := make([]string, 0, len(values))
	for _, value := range values {
		if value != target {
			filtered = append(filtered, value)
		}
	}
	return filtered
}

func setBrokerAnnotation(ctx context.Context, namespace, name string, uid types.UID, key string, value *string) error {
	brokers := eventingclient.Get(ctx).EventingV1().Brokers(namespace)
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		broker, err := brokers.Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if broker.UID != uid {
			return fmt.Errorf("Broker UID changed from %q to %q", uid, broker.UID)
		}
		broker = broker.DeepCopy()
		annotations := broker.GetAnnotations()
		if annotations == nil {
			annotations = make(map[string]string)
		}
		if value == nil {
			delete(annotations, key)
		} else {
			annotations[key] = *value
		}
		broker.SetAnnotations(annotations)
		updated, err := brokers.Update(ctx, broker, metav1.UpdateOptions{})
		if err != nil {
			return err
		}
		if updated.UID != uid {
			return fmt.Errorf("Broker UID changed from %q to %q during annotation update", uid, updated.UID)
		}
		got, found := updated.Annotations[key]
		if value == nil && found {
			return fmt.Errorf("Broker annotation %q remains %q after removal", key, got)
		}
		if value != nil && (!found || got != *value) {
			return fmt.Errorf("Broker annotation %q = %q (found=%t), want %q", key, got, found, *value)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("set Broker %s/%s annotation %q: %w", namespace, name, key, err)
	}
	return nil
}

func releaseAutoscalerHolds(ctx context.Context, perPhaseTimeout time.Duration, namespace, hpaName string, hpaUID types.UID, hpaFinalizer, scaledObjectName string, scaledObjectUID types.UID, scaledObjectFinalizer string) error {
	phaseCtx, cancel := context.WithTimeout(ctx, perPhaseTimeout)
	err := removeHPAFinalizer(phaseCtx, namespace, hpaName, hpaUID, hpaFinalizer)
	cancel()
	if err != nil {
		return err
	}
	phaseCtx, cancel = context.WithTimeout(ctx, perPhaseTimeout)
	err = waitForHPAUIDGone(phaseCtx, namespace, hpaName, hpaUID)
	cancel()
	if err != nil {
		return err
	}
	phaseCtx, cancel = context.WithTimeout(ctx, perPhaseTimeout)
	err = removeScaledObjectFinalizer(phaseCtx, namespace, scaledObjectName, scaledObjectUID, scaledObjectFinalizer)
	cancel()
	if err != nil {
		return err
	}
	phaseCtx, cancel = context.WithTimeout(ctx, perPhaseTimeout)
	defer cancel()
	return waitForScaledObjectUIDGone(phaseCtx, namespace, scaledObjectName, scaledObjectUID)
}

func waitForAutoscalersTerminating(ctx context.Context, namespace, scaledObjectName string, scaledObjectUID types.UID, scaledObjectFinalizer, hpaName string, hpaUID types.UID, hpaFinalizer string, timeout time.Duration) error {
	var last string
	err := wait.PollUntilContextTimeout(ctx, time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		object, objectErr := dynamicclient.Get(ctx).Resource(brokerautoscaler.ScaledObjectGVR).Namespace(namespace).Get(ctx, scaledObjectName, metav1.GetOptions{})
		if objectErr != nil {
			last = fmt.Sprintf("ScaledObject error=%v", objectErr)
			return false, nil
		}
		if object.GetUID() != scaledObjectUID {
			return false, fmt.Errorf("ScaledObject UID changed from %q to %q", scaledObjectUID, object.GetUID())
		}
		hpa, hpaErr := kubeclient.Get(ctx).AutoscalingV2().HorizontalPodAutoscalers(namespace).Get(ctx, hpaName, metav1.GetOptions{})
		if hpaErr != nil {
			last = fmt.Sprintf("ScaledObject terminating=%t; HPA error=%v", object.GetDeletionTimestamp() != nil, hpaErr)
			return false, nil
		}
		if hpa.UID != hpaUID {
			return false, fmt.Errorf("HPA UID changed from %q to %q", hpaUID, hpa.UID)
		}
		last = fmt.Sprintf("ScaledObject terminating=%t hold=%t; HPA terminating=%t hold=%t",
			object.GetDeletionTimestamp() != nil, containsString(object.GetFinalizers(), scaledObjectFinalizer),
			hpa.DeletionTimestamp != nil, containsString(hpa.Finalizers, hpaFinalizer))
		return object.GetDeletionTimestamp() != nil && containsString(object.GetFinalizers(), scaledObjectFinalizer) &&
			hpa.DeletionTimestamp != nil && containsString(hpa.Finalizers, hpaFinalizer), nil
	})
	if err != nil {
		return fmt.Errorf("ScaledObject %s/%s and HPA %s did not remain terminating under test finalizers (%s): %w", namespace, scaledObjectName, hpaName, last, err)
	}
	return nil
}

func waitForStaticCapacityWhileAutoscalersTerminating(ctx context.Context, namespace, scaledObjectName string, scaledObjectUID types.UID, scaledObjectFinalizer, hpaName string, hpaUID types.UID, hpaFinalizer string, timeout time.Duration) error {
	var last string
	err := wait.PollUntilContextTimeout(ctx, time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		object, objectErr := dynamicclient.Get(ctx).Resource(brokerautoscaler.ScaledObjectGVR).Namespace(namespace).Get(ctx, scaledObjectName, metav1.GetOptions{})
		if objectErr != nil {
			last = fmt.Sprintf("ScaledObject error=%v", objectErr)
			return false, nil
		}
		if object.GetUID() != scaledObjectUID {
			return false, fmt.Errorf("ScaledObject UID changed from %q to %q", scaledObjectUID, object.GetUID())
		}
		hpa, hpaErr := kubeclient.Get(ctx).AutoscalingV2().HorizontalPodAutoscalers(namespace).Get(ctx, hpaName, metav1.GetOptions{})
		if hpaErr != nil {
			last = fmt.Sprintf("ScaledObject terminating=%t; HPA error=%v", object.GetDeletionTimestamp() != nil, hpaErr)
			return false, nil
		}
		if hpa.UID != hpaUID {
			return false, fmt.Errorf("HPA UID changed from %q to %q", hpaUID, hpa.UID)
		}
		deployment, deploymentErr := kubeclient.Get(ctx).AppsV1().Deployments(namespace).Get(ctx, scaledObjectName, metav1.GetOptions{})
		if deploymentErr != nil {
			last = fmt.Sprintf("ScaledObject terminating=%t; HPA terminating=%t; Deployment error=%v", object.GetDeletionTimestamp() != nil, hpa.DeletionTimestamp != nil, deploymentErr)
			return false, nil
		}
		desired := int32(1)
		if deployment.Spec.Replicas != nil {
			desired = *deployment.Spec.Replicas
		}
		last = fmt.Sprintf("ScaledObject terminating=%t hold=%t; HPA terminating=%t hold=%t; desired=%d available=%d",
			object.GetDeletionTimestamp() != nil, containsString(object.GetFinalizers(), scaledObjectFinalizer),
			hpa.DeletionTimestamp != nil, containsString(hpa.Finalizers, hpaFinalizer), desired, deployment.Status.AvailableReplicas)
		return object.GetDeletionTimestamp() != nil && containsString(object.GetFinalizers(), scaledObjectFinalizer) &&
			hpa.DeletionTimestamp != nil && containsString(hpa.Finalizers, hpaFinalizer) &&
			desired == 1 && deployment.Status.AvailableReplicas >= 1, nil
	})
	if err != nil {
		return fmt.Errorf("static capacity was not restored while ScaledObject %s/%s and HPA %s remained terminating (%s): %w", namespace, scaledObjectName, hpaName, last, err)
	}
	return nil
}

func updateDeploymentScale(ctx context.Context, namespace, name string, brokerUID types.UID, replicas int32) error {
	deployments := kubeclient.Get(ctx).AppsV1().Deployments(namespace)
	deployment, err := deployments.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get Deployment %s/%s before scale update: %w", namespace, name, err)
	}
	if err := requireControllerOwner(deployment.OwnerReferences, "eventing.knative.dev/v1", "Broker", brokerUID); err != nil {
		return fmt.Errorf("refuse to scale Deployment %s/%s: %w", namespace, name, err)
	}
	scale, err := deployments.GetScale(ctx, name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get Deployment %s/%s scale: %w", namespace, name, err)
	}
	if scale.UID != "" && scale.UID != deployment.UID {
		return fmt.Errorf("Deployment %s/%s UID changed from %q to %q before scale update", namespace, name, deployment.UID, scale.UID)
	}
	scale.Spec.Replicas = replicas
	updated, err := deployments.UpdateScale(ctx, name, scale, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("write Deployment %s/%s scale=%d: %w", namespace, name, replicas, err)
	}
	if updated.Spec.Replicas != replicas {
		return fmt.Errorf("Deployment %s/%s scale update returned %d, want %d", namespace, name, updated.Spec.Replicas, replicas)
	}
	if updated.UID != "" && updated.UID != deployment.UID {
		return fmt.Errorf("Deployment %s/%s UID changed from %q to %q during scale update", namespace, name, deployment.UID, updated.UID)
	}
	return nil
}

func waitForHPAUIDGone(ctx context.Context, namespace, name string, uid types.UID) error {
	var lastErr error
	err := wait.PollUntilContextTimeout(ctx, time.Second, 3*time.Minute, true, func(ctx context.Context) (bool, error) {
		hpa, getErr := kubeclient.Get(ctx).AutoscalingV2().HorizontalPodAutoscalers(namespace).Get(ctx, name, metav1.GetOptions{})
		if apierrs.IsNotFound(getErr) {
			return true, nil
		}
		lastErr = getErr
		if getErr == nil && hpa.UID != uid {
			lastErr = fmt.Errorf("replacement HPA UID %q appeared before old UID %q was observed gone", hpa.UID, uid)
		}
		return false, nil
	})
	if err != nil {
		return fmt.Errorf("HPA %s/%s UID %q was not deleted (last error: %v): %w", namespace, name, uid, lastErr, err)
	}
	return nil
}

func waitForScaledObjectUIDGone(ctx context.Context, namespace, name string, uid types.UID) error {
	var lastErr error
	err := wait.PollUntilContextTimeout(ctx, time.Second, 3*time.Minute, true, func(ctx context.Context) (bool, error) {
		object, getErr := dynamicclient.Get(ctx).Resource(brokerautoscaler.ScaledObjectGVR).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
		if apierrs.IsNotFound(getErr) {
			return true, nil
		}
		lastErr = getErr
		if getErr == nil && object.GetUID() != uid {
			lastErr = fmt.Errorf("replacement ScaledObject UID %q appeared before old UID %q was observed gone", object.GetUID(), uid)
		}
		return false, nil
	})
	if err != nil {
		return fmt.Errorf("ScaledObject %s/%s UID %q was not deleted (last error: %v): %w", namespace, name, uid, lastErr, err)
	}
	return nil
}

func waitForBrokerReady(ctx context.Context, namespace, name string, timeout time.Duration) error {
	var last string
	err := wait.PollUntilContextTimeout(ctx, time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		broker, err := eventingclient.Get(ctx).EventingV1().Brokers(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			last = err.Error()
			return false, nil
		}
		last = fmt.Sprintf("generation=%d observedGeneration=%d ready=%t", broker.Generation, broker.Status.ObservedGeneration, broker.IsReady())
		return broker.Status.ObservedGeneration == broker.Generation && broker.IsReady(), nil
	})
	if err != nil {
		return fmt.Errorf("Broker %s/%s did not become ready (%s): %w", namespace, name, last, err)
	}
	return nil
}

func waitForDeploymentReplicas(ctx context.Context, namespace, name string, replicas int32, requireAvailable bool, timeout time.Duration) error {
	var last string
	err := wait.PollUntilContextTimeout(ctx, time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		deployment, err := kubeclient.Get(ctx).AppsV1().Deployments(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			last = err.Error()
			return false, nil
		}
		desired := int32(1)
		if deployment.Spec.Replicas != nil {
			desired = *deployment.Spec.Replicas
		}
		last = fmt.Sprintf("desired=%d replicas=%d available=%d", desired, deployment.Status.Replicas, deployment.Status.AvailableReplicas)
		if desired != replicas {
			return false, nil
		}
		if requireAvailable {
			return deployment.Status.AvailableReplicas >= replicas, nil
		}
		if replicas == 0 {
			return deployment.Status.Replicas == 0, nil
		}
		return true, nil
	})
	if err != nil {
		return fmt.Errorf("Deployment %s/%s did not reach replicas=%d, requireAvailable=%t (%s): %w", namespace, name, replicas, requireAvailable, last, err)
	}
	return nil
}

type deploymentRef struct {
	namespace string
	name      string
}

func verifyKEDANativeFallbackFromZero(ctx context.Context, t feature.T) {
	withDeploymentsStopped(ctx, t, []deploymentRef{{
		namespace: system.Namespace(),
		name:      "natsjetstream-broker-controller",
	}}, func() {
		namespace := environment.FromContext(ctx).Namespace()
		// Exercise KEDA's native zero-to-fallback transition on Broker B, which
		// has been idle for the whole scenario. Broker A previously processed a
		// backlog and is covered by the controller safety-wakeup outage check.
		const idleFilter = "autoscale-broker-b-broker-filter"
		originalSpecs := make(map[string]map[string]interface{}, 1)
		restored := false
		defer func() {
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Minute)
			defer cancel()
			if !restored {
				if err := restoreScaledObjectSpecs(cleanupCtx, namespace, originalSpecs); err != nil {
					t.Errorf("failed to restore ScaledObjects after native fallback test: %v", err)
					return
				}
			}
			if err := waitForDeploymentReplicas(cleanupCtx, namespace, idleFilter, 0, false, 3*time.Minute); err != nil {
				t.Errorf("idle filter did not return to zero after native fallback test: %v", err)
			}
		}()

		if err := breakScaledObjectMonitoring(ctx, namespace, idleFilter, originalSpecs); err != nil {
			t.Fatal(err)
		}
		waitForDeployment(ctx, t, idleFilter, func(replicas int32) bool { return replicas == 1 })
		assertFilterReplicas(ctx, t, "autoscale-broker-a-broker-filter", 0)

		if err := restoreScaledObjectSpecs(ctx, namespace, originalSpecs); err != nil {
			t.Fatal(err)
		}
		restored = true
		waitForDeployment(ctx, t, idleFilter, func(replicas int32) bool { return replicas == 0 })
		assertFilterReplicas(ctx, t, "autoscale-broker-a-broker-filter", 0)
	})
}

func breakScaledObjectMonitoring(ctx context.Context, namespace, name string, originalSpecs map[string]map[string]interface{}) error {
	resources := dynamicclient.Get(ctx).Resource(schema.GroupVersionResource{
		Group: "keda.sh", Version: "v1alpha1", Resource: "scaledobjects",
	}).Namespace(namespace)
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		object, err := resources.Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("failed to get ScaledObject %s/%s: %w", namespace, name, err)
		}
		spec, found, err := unstructured.NestedMap(object.Object, "spec")
		if err != nil {
			return fmt.Errorf("failed to read ScaledObject %s/%s spec: %w", namespace, name, err)
		}
		if !found {
			return fmt.Errorf("ScaledObject %s/%s has no spec", namespace, name)
		}
		if _, captured := originalSpecs[name]; !captured {
			originalSpecs[name] = spec
		}

		triggers, found, err := unstructured.NestedSlice(object.Object, "spec", "triggers")
		if err != nil {
			return fmt.Errorf("failed to read ScaledObject %s/%s triggers: %w", namespace, name, err)
		}
		if !found {
			return fmt.Errorf("ScaledObject %s/%s has no triggers", namespace, name)
		}
		for i, rawTrigger := range triggers {
			trigger, ok := rawTrigger.(map[string]interface{})
			if !ok {
				return fmt.Errorf("ScaledObject %s/%s trigger %d has unexpected type %T", namespace, name, i, rawTrigger)
			}
			metadata, ok := trigger["metadata"].(map[string]interface{})
			if !ok {
				return fmt.Errorf("ScaledObject %s/%s trigger %d has no metadata", namespace, name, i)
			}
			metadata["natsServerMonitoringEndpoint"] = "127.0.0.1:1"
		}
		if err := unstructured.SetNestedSlice(object.Object, triggers, "spec", "triggers"); err != nil {
			return err
		}
		_, err = resources.Update(ctx, object, metav1.UpdateOptions{})
		return err
	})
}

func restoreScaledObjectSpecs(ctx context.Context, namespace string, specs map[string]map[string]interface{}) error {
	resources := dynamicclient.Get(ctx).Resource(schema.GroupVersionResource{
		Group: "keda.sh", Version: "v1alpha1", Resource: "scaledobjects",
	}).Namespace(namespace)
	for name, spec := range specs {
		if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			object, err := resources.Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				return err
			}
			if err := unstructured.SetNestedMap(object.Object, spec, "spec"); err != nil {
				return err
			}
			_, err = resources.Update(ctx, object, metav1.UpdateOptions{})
			return err
		}); err != nil {
			return fmt.Errorf("failed to restore ScaledObject %s/%s: %w", namespace, name, err)
		}
	}
	return nil
}

func withDeploymentsStopped(ctx context.Context, t feature.T, refs []deploymentRef, run func()) {
	type originalDeployment struct {
		ref      deploymentRef
		replicas int32
	}
	originals := make([]originalDeployment, 0, len(refs))
	defer func() {
		for i := len(originals) - 1; i >= 0; i-- {
			original := originals[i]
			if err := updateDeploymentReplicas(ctx, original.ref, original.replicas); err != nil {
				t.Errorf("failed to restore Deployment %s/%s: %v", original.ref.namespace, original.ref.name, err)
				continue
			}
			if err := waitForSystemDeployment(ctx, original.ref, original.replicas, true); err != nil {
				t.Errorf("Deployment %s/%s did not recover: %v", original.ref.namespace, original.ref.name, err)
			}
		}
	}()

	for _, ref := range refs {
		deployment, err := kubeclient.Get(ctx).AppsV1().Deployments(ref.namespace).Get(ctx, ref.name, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		replicas := int32(1)
		if deployment.Spec.Replicas != nil {
			replicas = *deployment.Spec.Replicas
		}
		originals = append(originals, originalDeployment{ref: ref, replicas: replicas})
		if err := updateDeploymentReplicas(ctx, ref, 0); err != nil {
			t.Fatal(err)
		}
		if err := waitForSystemDeployment(ctx, ref, 0, false); err != nil {
			t.Fatal(err)
		}
	}
	run()
}

func outageWorkloadStatusSnapshot(ctx context.Context, namespaces []string) string {
	diagnosticsCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	return workloadStatusSnapshot(diagnosticsCtx, namespaces)
}

func workloadStatusSnapshot(ctx context.Context, namespaces []string) string {
	const (
		logTailLines  = int64(200)
		logLimitBytes = int64(256 * 1024)
	)
	client := kubeclient.Get(ctx)
	seen := make(map[string]struct{}, len(namespaces))
	var snapshot strings.Builder
	for _, namespace := range namespaces {
		if namespace == "" {
			continue
		}
		if _, found := seen[namespace]; found {
			continue
		}
		seen[namespace] = struct{}{}
		fmt.Fprintf(&snapshot, "namespace %s\n", namespace)

		deployments, err := client.AppsV1().Deployments(namespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			fmt.Fprintf(&snapshot, "  deployments: %v\n", err)
		} else {
			for _, deployment := range deployments.Items {
				desired := int32(1)
				if deployment.Spec.Replicas != nil {
					desired = *deployment.Spec.Replicas
				}
				fmt.Fprintf(&snapshot, "  deployment %s desired=%d replicas=%d updated=%d ready=%d available=%d unavailable=%d\n",
					deployment.Name, desired, deployment.Status.Replicas, deployment.Status.UpdatedReplicas,
					deployment.Status.ReadyReplicas, deployment.Status.AvailableReplicas, deployment.Status.UnavailableReplicas)
			}
		}

		pods, err := client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			fmt.Fprintf(&snapshot, "  pods: %v\n", err)
		} else {
			for _, pod := range pods.Items {
				fmt.Fprintf(&snapshot, "  pod %s phase=%s node=%s", pod.Name, pod.Status.Phase, pod.Spec.NodeName)
				statuses := make([]corev1.ContainerStatus, 0, len(pod.Status.InitContainerStatuses)+len(pod.Status.ContainerStatuses)+len(pod.Status.EphemeralContainerStatuses))
				statuses = append(statuses, pod.Status.InitContainerStatuses...)
				statuses = append(statuses, pod.Status.ContainerStatuses...)
				statuses = append(statuses, pod.Status.EphemeralContainerStatuses...)
				for _, status := range statuses {
					fmt.Fprintf(&snapshot, " container=%s/ready=%t/restarts=%d", status.Name, status.Ready, status.RestartCount)
				}
				fmt.Fprintln(&snapshot)

				containerNames := make([]string, 0, len(pod.Spec.InitContainers)+len(pod.Spec.Containers)+len(pod.Spec.EphemeralContainers))
				for _, container := range pod.Spec.InitContainers {
					containerNames = append(containerNames, container.Name)
				}
				for _, container := range pod.Spec.Containers {
					containerNames = append(containerNames, container.Name)
				}
				for _, container := range pod.Spec.EphemeralContainers {
					containerNames = append(containerNames, container.Name)
				}
				for _, container := range containerNames {
					for _, previous := range []bool{false, true} {
						instance := "current"
						if previous {
							instance = "previous"
						}
						logs, err := client.CoreV1().Pods(namespace).GetLogs(pod.Name, &corev1.PodLogOptions{
							Container:  container,
							Previous:   previous,
							Timestamps: true,
							TailLines:  ptr.To(logTailLines),
							LimitBytes: ptr.To(logLimitBytes),
						}).DoRaw(ctx)
						if err != nil {
							fmt.Fprintf(&snapshot, "  logs pod=%s container=%s instance=%s error=%v\n", pod.Name, container, instance, err)
							continue
						}
						fmt.Fprintf(&snapshot, "  logs pod=%s container=%s instance=%s tail=%d limitBytes=%d\n%s\n",
							pod.Name, container, instance, logTailLines, logLimitBytes, logs)
					}
				}
			}
		}

		jobs, err := client.BatchV1().Jobs(namespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			fmt.Fprintf(&snapshot, "  jobs: %v\n", err)
		} else {
			for _, job := range jobs.Items {
				fmt.Fprintf(&snapshot, "  job %s active=%d succeeded=%d failed=%d\n", job.Name, job.Status.Active, job.Status.Succeeded, job.Status.Failed)
			}
		}
	}
	return snapshot.String()
}

func updateDeploymentReplicas(ctx context.Context, ref deploymentRef, replicas int32) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		deployments := kubeclient.Get(ctx).AppsV1().Deployments(ref.namespace)
		deployment, err := deployments.Get(ctx, ref.name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		deployment = deployment.DeepCopy()
		deployment.Spec.Replicas = ptr.To(replicas)
		_, err = deployments.Update(ctx, deployment, metav1.UpdateOptions{})
		return err
	})
}

func waitForSystemDeployment(ctx context.Context, ref deploymentRef, replicas int32, requireAvailable bool) error {
	return wait.PollUntilContextTimeout(ctx, time.Second, 3*time.Minute, true, func(ctx context.Context) (bool, error) {
		deployment, err := kubeclient.Get(ctx).AppsV1().Deployments(ref.namespace).Get(ctx, ref.name, metav1.GetOptions{})
		if err != nil {
			return false, nil
		}
		desired := int32(1)
		if deployment.Spec.Replicas != nil {
			desired = *deployment.Spec.Replicas
		}
		if desired != replicas {
			return false, nil
		}
		if requireAvailable {
			return deployment.Status.AvailableReplicas >= replicas, nil
		}
		return deployment.Status.Replicas == 0, nil
	})
}

func waitForFilterReplicas(name string, expected int32) feature.StepFn {
	return func(ctx context.Context, t feature.T) {
		waitForDeployment(ctx, t, name, func(replicas int32) bool { return replicas == expected })
	}
}

func waitForDeployment(ctx context.Context, t feature.T, name string, matches func(int32) bool) {
	namespace := environment.FromContext(ctx).Namespace()
	var last int32 = -1
	err := wait.PollUntilContextTimeout(ctx, time.Second, 3*time.Minute, true, func(ctx context.Context) (bool, error) {
		deployment, err := kubeclient.Get(ctx).AppsV1().Deployments(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return false, nil
		}
		last = 1
		if deployment.Spec.Replicas != nil {
			last = *deployment.Spec.Replicas
		}
		return matches(last), nil
	})
	if err != nil {
		t.Fatalf("deployment %s replicas did not reach the expected state; last=%d: %v", name, last, err)
	}
}

func assertFilterReplicas(ctx context.Context, t feature.T, name string, expected int32) {
	namespace := environment.FromContext(ctx).Namespace()
	deployment, err := kubeclient.Get(ctx).AppsV1().Deployments(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if deployment.Spec.Replicas == nil || *deployment.Spec.Replicas != expected {
		t.Fatalf("deployment %s replicas = %v, want %d", name, deployment.Spec.Replicas, expected)
	}
}

func assertFilterTokenRequestAuthority(ctx context.Context, t feature.T, brokerName string, wantExact bool) {
	namespace := environment.FromContext(ctx).Namespace()
	broker, err := eventingclient.Get(ctx).EventingV1().Brokers(namespace).Get(ctx, brokerName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	filterServiceAccount := brokerutils.FilterServiceAccountName(broker)
	canCreateToken := func(subject, target string) bool {
		review, err := kubeclient.Get(ctx).AuthorizationV1().SubjectAccessReviews().Create(ctx, &authorizationv1.SubjectAccessReview{
			Spec: authorizationv1.SubjectAccessReviewSpec{
				User: fmt.Sprintf("system:serviceaccount:%s:%s", namespace, subject),
				ResourceAttributes: &authorizationv1.ResourceAttributes{
					Namespace:   namespace,
					Verb:        "create",
					Group:       "",
					Resource:    "serviceaccounts",
					Subresource: "token",
					Name:        target,
				},
			},
		}, metav1.CreateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		return review.Status.Allowed
	}
	if got := canCreateToken(filterServiceAccount, brokeroidc.DeliveryServiceAccountName); got != wantExact {
		t.Fatalf("filter ServiceAccount %s/%s exact outbound token permission = %v, want %v", namespace, filterServiceAccount, got, wantExact)
	}
	if canCreateToken(filterServiceAccount, filterServiceAccount) {
		t.Fatalf("filter ServiceAccount %s/%s can mint its own token", namespace, filterServiceAccount)
	}
	if canCreateToken(brokeroidc.DeliveryServiceAccountName, brokeroidc.DeliveryServiceAccountName) {
		t.Fatalf("outbound ServiceAccount %s/%s can mint its own token", namespace, brokeroidc.DeliveryServiceAccountName)
	}
}

// namedRecorderFeature creates a feature that installs an eventshub receiver with the given name.
func namedRecorderFeature(name string, options ...eventshub.EventsHubOption) *feature.Feature {
	svc := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "services"}
	f := new(feature.Feature)
	options = append([]eventshub.EventsHubOption{eventshub.StartReceiver}, options...)
	f.Setup("install "+name, eventshub.Install(name, options...))
	f.Requirement(name+" is addressable", k8s.IsAddressable(svc, name, time.Second, 30*time.Second))
	return f
}

// TestNatsBrokerDirect tests that a NatsJetStream broker delivers events to a consumer.
func TestNatsBrokerDirect(t *testing.T) {
	t.Parallel()
	ctx, env := global.Environment(
		environment.Managed(t),
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithObservabilityConfig,
		k8s.WithEventListener,
	)
	env.Test(ctx, t, RecorderFeature())
	env.Test(ctx, t, NatsBrokerDirectFeature())
}

// NatsBrokerDirectFeature tests direct event delivery through NatsJetStream broker.
//
//	producer ---> broker --[trigger]--> recorder
func NatsBrokerDirectFeature() *feature.Feature {
	f := new(feature.Feature)

	// Install broker and trigger first, then wait for them to be ready before
	// installing the producer. This ensures the NATS consumer exists before
	// events are published, so no events are missed with DeliverNew policy.
	f.Setup("install broker and trigger", natsbroker.InstallBrokerAndTrigger())
	f.Setup("wait for broker and trigger ready", AllGoReady)
	f.Setup("install producer", natsbroker.InstallProducer(5))

	f.Alpha("NatsJetStream broker goes ready").Must("goes ready", AllGoReady)
	f.Alpha("NatsJetStream filter without OIDC has no token authority").Must("cannot mint the outbound service account token", func(ctx context.Context, t feature.T) {
		assertFilterTokenRequestAuthority(ctx, t, "test-nats-broker", false)
	})
	f.Alpha("NatsJetStream broker delivers events").
		Must("the recorder received all sent events within the time",
			func(ctx context.Context, t feature.T) {
				eventshub.StoreFromContext(ctx, "recorder").AssertAtLeast(ctx, t, 5,
					hasEventType("knative.natsbroker.e2etest"))
			})
	return f
}

// TestNatsBrokerDeadLetter tests that a NatsJetStream broker sends failed events to a dead letter sink.
func TestNatsBrokerDeadLetter(t *testing.T) {
	t.Parallel()
	ctx, env := global.Environment(
		environment.Managed(t),
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithObservabilityConfig,
		k8s.WithEventListener,
	)
	env.Test(ctx, t, namedRecorderFeature("dls-recorder", eventshub.OIDCReceiverAudience(e2eOIDCAudience)))
	env.Test(ctx, t, NatsBrokerDeadLetterFeature())
}

// NatsBrokerDeadLetterFeature tests that failed events reach the dead letter sink.
//
//	producer ---> broker --[trigger]--> failing-subscriber (500) --[DLS]--> dls-recorder
func NatsBrokerDeadLetterFeature() *feature.Feature {
	f := new(feature.Feature)

	f.Setup("install broker, trigger and failing-subscriber", deadletter.InstallBrokerAndSubscriber())
	f.Setup("wait for broker and trigger ready", AllGoReady)
	f.Setup("install producer", deadletter.InstallProducer(2))

	f.Alpha("NatsJetStream broker with DLS goes ready").Must("goes ready", AllGoReady)
	f.Alpha("NatsJetStream broker sends failed events to dead letter sink").
		Must("the dls-recorder received all sent events",
			func(ctx context.Context, t feature.T) {
				eventshub.StoreFromContext(ctx, "dls-recorder").AssertAtLeast(ctx, t, 2,
					hasEventType("dls.test.event"))
			})
	return f
}

// TestNatsBrokerFiltering tests that a NatsJetStream broker routes events by type to the correct subscriber.
func TestNatsBrokerFiltering(t *testing.T) {
	t.Parallel()
	ctx, env := global.Environment(
		environment.Managed(t),
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithObservabilityConfig,
		k8s.WithEventListener,
	)
	env.Test(ctx, t, namedRecorderFeature("recorder-type-a"))
	env.Test(ctx, t, namedRecorderFeature("recorder-type-b"))
	env.Test(ctx, t, NatsBrokerFilteringFeature())
}

// NatsBrokerFilteringFeature tests type-based event routing through NatsJetStream broker.
//
//	producer-type-a ---> broker --[trigger type-a filter]--> recorder-type-a
//	producer-type-b ---> broker --[trigger type-b filter]--> recorder-type-b
func NatsBrokerFilteringFeature() *feature.Feature {
	f := new(feature.Feature)

	f.Setup("install broker and triggers", filtering.InstallBrokerAndTriggers())
	f.Setup("wait for broker and triggers ready", AllGoReady)
	f.Setup("install producers", filtering.InstallProducers(3, 2))

	f.Alpha("NatsJetStream broker with filtering goes ready").Must("goes ready", AllGoReady)
	f.Alpha("NatsJetStream broker routes type-a events to correct subscriber").
		Must("recorder-type-a received only type-a events",
			func(ctx context.Context, t feature.T) {
				eventshub.StoreFromContext(ctx, "recorder-type-a").AssertAtLeast(ctx, t, 3,
					hasEventType("type-a"))
			})
	f.Alpha("NatsJetStream broker routes type-b events to correct subscriber").
		Must("recorder-type-b received only type-b events",
			func(ctx context.Context, t feature.T) {
				eventshub.StoreFromContext(ctx, "recorder-type-b").AssertAtLeast(ctx, t, 2,
					hasEventType("type-b"))
			})
	return f
}
