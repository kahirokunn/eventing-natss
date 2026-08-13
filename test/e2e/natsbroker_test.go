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
	"strings"
	"testing"
	"time"

	cetypes "github.com/cloudevents/sdk-go/v2/types"
	// For our e2e testing, we want this linked first so that our
	// system namespace environment variable is defaulted prior to
	// logstream initialization.
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/ptr"
	_ "knative.dev/eventing-natss/test/defaultsystem"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	dynamicclient "knative.dev/pkg/injection/clients/dynamicclient"
	"knative.dev/pkg/system"
	_ "knative.dev/pkg/system/testing"
	"knative.dev/reconciler-test/pkg/environment"
	"knative.dev/reconciler-test/pkg/eventshub"
	"knative.dev/reconciler-test/pkg/feature"
	"knative.dev/reconciler-test/pkg/k8s"
	"knative.dev/reconciler-test/pkg/knative"

	"knative.dev/eventing-natss/test/e2e/config/autoscaling"
	"knative.dev/eventing-natss/test/e2e/config/deadletter"
	"knative.dev/eventing-natss/test/e2e/config/filtering"
	"knative.dev/eventing-natss/test/e2e/config/natsbroker"
)

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
	env.Test(ctx, t, namedRecorderFeature("autoscale-recorder-a", eventshub.ResponseWaitTime(time.Second)))
	env.Test(ctx, t, namedRecorderFeature("autoscale-recorder-b"))
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
		AllGoReady(ctx, t)
	})
	return f
}

func shouldCaptureOutageSnapshot(failedBefore, failedAfter bool) bool {
	return !failedBefore && failedAfter
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
		names := []string{"autoscale-broker-a-broker-filter", "autoscale-broker-b-broker-filter"}
		originalSpecs := make(map[string]map[string]interface{}, len(names))
		restored := false
		defer func() {
			if !restored {
				if err := restoreScaledObjectSpecs(ctx, namespace, originalSpecs); err != nil {
					t.Errorf("failed to restore ScaledObjects after native fallback test: %v", err)
				}
			}
		}()

		for _, name := range names {
			if err := breakScaledObjectMonitoring(ctx, namespace, name, originalSpecs); err != nil {
				t.Fatal(err)
			}
		}
		for _, name := range names {
			waitForDeployment(ctx, t, name, func(replicas int32) bool { return replicas == 1 })
		}

		if err := restoreScaledObjectSpecs(ctx, namespace, originalSpecs); err != nil {
			t.Fatal(err)
		}
		restored = true
		for _, name := range names {
			waitForDeployment(ctx, t, name, func(replicas int32) bool { return replicas == 0 })
		}
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
	env.Test(ctx, t, namedRecorderFeature("dls-recorder"))
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
