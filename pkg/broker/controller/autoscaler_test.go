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
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	kubefake "k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"

	eventingv1 "knative.dev/eventing/pkg/apis/eventing/v1"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"

	"knative.dev/eventing-natss/pkg/broker/autoscaler"
	brokerconfig "knative.dev/eventing-natss/pkg/broker/config"
	"knative.dev/eventing-natss/pkg/broker/controller/resources"
	brokerutils "knative.dev/eventing-natss/pkg/broker/utils"
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

func TestDeleteScaledObjectUsesUIDPreconditionAndForegroundPropagation(t *testing.T) {
	ctx := context.Background()
	broker := autoscaledBroker()
	object := expectedScaledObject(t, broker)
	object.SetUID(types.UID("scaledobject-uid"))
	object.SetResourceVersion("scaledobject-rv")
	client := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), object)
	r := &Reconciler{dynamicClient: client}

	targetName, _, err := unstructured.NestedString(object.Object, "spec", "scaleTargetRef", "name")
	if err != nil {
		t.Fatal(err)
	}
	if err := r.deleteScaledObject(ctx, broker, targetName); err != nil {
		t.Fatal(err)
	}

	var options *metav1.DeleteOptions
	for _, action := range client.Actions() {
		if !action.Matches("delete", "scaledobjects") {
			continue
		}
		got := action.(clienttesting.DeleteAction).GetDeleteOptions()
		options = &got
		break
	}
	if options == nil {
		t.Fatal("ScaledObject delete action was not recorded")
	}
	if options.Preconditions == nil || options.Preconditions.UID == nil || *options.Preconditions.UID != object.GetUID() {
		t.Fatalf("UID precondition = %#v, want %q", options.Preconditions, object.GetUID())
	}
	if options.Preconditions.ResourceVersion == nil || *options.Preconditions.ResourceVersion != object.GetResourceVersion() {
		t.Fatalf("resourceVersion precondition = %#v, want %q", options.Preconditions, object.GetResourceVersion())
	}
	if options.PropagationPolicy == nil || *options.PropagationPolicy != metav1.DeletePropagationForeground {
		t.Fatalf("propagation policy = %v, want %q", options.PropagationPolicy, metav1.DeletePropagationForeground)
	}
}

func TestDeleteScaledObjectForegroundDeletionRequeuesQuickly(t *testing.T) {
	broker := autoscaledBroker()
	object := expectedScaledObject(t, broker)
	object.SetUID(types.UID("scaledobject-uid"))
	now := metav1.Now()
	object.SetDeletionTimestamp(&now)
	object.SetFinalizers([]string{metav1.FinalizerDeleteDependents, "finalizer.keda.sh"})
	client := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), object)
	r := &Reconciler{dynamicClient: client}

	targetName, _, err := unstructured.NestedString(object.Object, "spec", "scaleTargetRef", "name")
	if err != nil {
		t.Fatal(err)
	}
	err = r.deleteScaledObject(context.Background(), broker, targetName)
	if ok, delay := controller.IsRequeueKey(err); !ok || delay != time.Second {
		t.Fatalf("cleanup requeue = (%v, %s), want (true, 1s)", ok, delay)
	}
	for _, action := range client.Actions() {
		if action.Matches("delete", "scaledobjects") {
			t.Fatalf("unexpected repeated delete for an object already terminating: %#v", action)
		}
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

type fakeAutoscalerJetStream struct {
	nats.JetStreamContext
	infos map[string]*nats.ConsumerInfo
	errs  map[string]error
	calls atomic.Int32
}

func (f *fakeAutoscalerJetStream) ConsumerInfo(_, consumer string, _ ...nats.JSOpt) (*nats.ConsumerInfo, error) {
	f.calls.Add(1)
	if err := f.errs[consumer]; err != nil {
		return nil, err
	}
	return f.infos[consumer], nil
}

func TestConsumerLag(t *testing.T) {
	for _, tc := range []struct {
		name string
		info *nats.ConsumerInfo
		want uint64
	}{
		{name: "pending", info: &nats.ConsumerInfo{NumPending: 4}, want: 4},
		{name: "pending and unacknowledged", info: &nats.ConsumerInfo{NumPending: 4, NumAckPending: 3}, want: 7},
		{name: "negative unacknowledged is ignored", info: &nats.ConsumerInfo{NumPending: 4, NumAckPending: -1}, want: 4},
		{name: "overflow saturates", info: &nats.ConsumerInfo{NumPending: ^uint64(0), NumAckPending: 1}, want: ^uint64(0)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := consumerLag(tc.info); got != tc.want {
				t.Fatalf("consumerLag() = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestReconcileAutoscalerSafetyWakeup(t *testing.T) {
	trigger := testTrigger(testNamespace, "trigger", testBrokerName)
	trigger.UID = types.UID("trigger-uid")
	consumer := brokerutils.TriggerConsumerName(string(trigger.UID))

	for _, tc := range []struct {
		name            string
		replicas        int32
		info            *nats.ConsumerInfo
		consumerErr     error
		activation      string
		minScale        int64
		wantReplicas    int32
		wantConsumerGet bool
	}{
		{
			name:            "pending backlog wakes zero replicas",
			info:            &nats.ConsumerInfo{NumPending: 2},
			activation:      "1",
			wantReplicas:    1,
			wantConsumerGet: true,
		},
		{
			name:            "pending backlog honors minimum above one",
			info:            &nats.ConsumerInfo{NumPending: 2},
			activation:      "1",
			minScale:        3,
			wantReplicas:    3,
			wantConsumerGet: true,
		},
		{
			name:            "ack pending backlog wakes zero replicas",
			info:            &nats.ConsumerInfo{NumAckPending: 2},
			activation:      "1",
			wantReplicas:    1,
			wantConsumerGet: true,
		},
		{
			name:            "activation threshold is strict",
			info:            &nats.ConsumerInfo{NumPending: 1},
			activation:      "1",
			wantReplicas:    0,
			wantConsumerGet: true,
		},
		{
			name:            "consumer error does not make an unsafe decision",
			consumerErr:     fmt.Errorf("consumer unavailable"),
			wantReplicas:    0,
			wantConsumerGet: true,
		},
		{
			name:         "running target skips JetStream query",
			replicas:     2,
			info:         &nats.ConsumerInfo{NumPending: 10},
			wantReplicas: 2,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			name := resources.FilterName(testBrokerName)
			deployment := &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: name},
				Spec:       appsv1.DeploymentSpec{Replicas: ptr.To(tc.replicas)},
			}
			kube := kubefake.NewSimpleClientset(deployment)
			currentReplicas := tc.replicas
			kube.PrependReactor("get", "deployments", func(action clienttesting.Action) (bool, runtime.Object, error) {
				if action.GetSubresource() != "scale" {
					return false, nil, nil
				}
				return true, &autoscalingv1.Scale{
					ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: name, ResourceVersion: "1"},
					Spec:       autoscalingv1.ScaleSpec{Replicas: currentReplicas},
				}, nil
			})
			kube.PrependReactor("update", "deployments", func(action clienttesting.Action) (bool, runtime.Object, error) {
				if action.GetSubresource() != "scale" {
					return false, nil, nil
				}
				scale := action.(clienttesting.UpdateAction).GetObject().(*autoscalingv1.Scale)
				currentReplicas = scale.Spec.Replicas
				return true, scale, nil
			})
			js := &fakeAutoscalerJetStream{
				infos: map[string]*nats.ConsumerInfo{consumer: tc.info},
				errs:  map[string]error{consumer: tc.consumerErr},
			}
			broker := autoscaledBroker()
			if tc.activation != "" {
				trigger.Annotations = map[string]string{autoscaler.ActivationLagThresholdAnnotation: tc.activation}
			} else {
				trigger.Annotations = nil
			}
			r := &Reconciler{kubeClientSet: kube, js: js}

			if err := r.reconcileAutoscalerSafetyWakeup(testContext(), broker, []*eventingv1.Trigger{trigger}, brokerutils.BrokerStreamName(broker), autoscaler.Settings{MinScale: tc.minScale}); err != nil {
				t.Fatal(err)
			}
			if currentReplicas != tc.wantReplicas {
				t.Fatalf("replicas = %d, want %d", currentReplicas, tc.wantReplicas)
			}
			if calls := js.calls.Load(); (calls > 0) != tc.wantConsumerGet {
				t.Fatalf("ConsumerInfo calls = %d, wantConsumerGet %v", calls, tc.wantConsumerGet)
			}
		})
	}
}

const testSafetyWakeupConcurrency = 8

type channelConsumerResponse struct {
	info               *nats.ConsumerInfo
	err                error
	gate               <-chan struct{}
	ignoreCancellation bool
}

type channelAutoscalerJetStream struct {
	nats.JetStreamContext

	responses map[string]channelConsumerResponse
	started   chan string
	deadlines chan time.Time
	reached   chan struct{}
	reachAt   int32

	calls     atomic.Int32
	active    atomic.Int32
	maxActive atomic.Int32
	reachOnce sync.Once
}

func newChannelAutoscalerJetStream(capacity int) *channelAutoscalerJetStream {
	return &channelAutoscalerJetStream{
		responses: make(map[string]channelConsumerResponse, capacity),
		started:   make(chan string, capacity),
		deadlines: make(chan time.Time, capacity),
		reached:   make(chan struct{}),
		reachAt:   int32(min(capacity, testSafetyWakeupConcurrency)),
	}
}

func (f *channelAutoscalerJetStream) ConsumerInfo(_, consumer string, opts ...nats.JSOpt) (*nats.ConsumerInfo, error) {
	ctx := context.Background()
	for _, opt := range opts {
		if contextOpt, ok := opt.(nats.ContextOpt); ok && contextOpt.Context != nil {
			ctx = contextOpt.Context
		}
	}
	f.calls.Add(1)
	active := f.active.Add(1)
	for {
		maximum := f.maxActive.Load()
		if active <= maximum || f.maxActive.CompareAndSwap(maximum, active) {
			break
		}
	}
	if active >= f.reachAt {
		f.reachOnce.Do(func() { close(f.reached) })
	}
	defer f.active.Add(-1)
	f.started <- consumer
	if deadline, ok := ctx.Deadline(); ok {
		f.deadlines <- deadline
	}

	response := f.responses[consumer]
	if response.gate != nil {
		if response.ignoreCancellation {
			<-response.gate
		} else {
			select {
			case <-response.gate:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}
	return response.info, response.err
}

func TestReconcileAutoscalerSafetyWakeupBoundsConsumerInfoConcurrency(t *testing.T) {
	const triggerCount = 24
	broker := autoscaledBroker()
	triggers := safetyWakeupTriggers(triggerCount)
	release := make(chan struct{})
	js := newChannelAutoscalerJetStream(triggerCount)
	for _, trigger := range triggers {
		consumer := brokerutils.TriggerConsumerName(string(trigger.UID))
		js.responses[consumer] = channelConsumerResponse{info: &nats.ConsumerInfo{}, gate: release}
	}
	kube, _ := safetyWakeupScaleClient(0)
	r := &Reconciler{kubeClientSet: kube, js: js}
	ctx, cancel := context.WithTimeout(testContext(), 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- r.reconcileAutoscalerSafetyWakeup(ctx, broker, triggers, brokerutils.BrokerStreamName(broker), autoscaler.Settings{})
	}()

	select {
	case <-js.reached:
	case <-time.After(200 * time.Millisecond):
		// Unblock the current serial implementation so the RED test terminates.
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got := js.maxActive.Load(); got != testSafetyWakeupConcurrency {
		t.Fatalf("maximum concurrent ConsumerInfo calls = %d, want %d", got, testSafetyWakeupConcurrency)
	}
	if got := js.calls.Load(); got != triggerCount {
		t.Fatalf("ConsumerInfo calls = %d, want %d", got, triggerCount)
	}
	if got := js.active.Load(); got != 0 {
		t.Fatalf("active ConsumerInfo calls after return = %d, want 0", got)
	}
}

func TestReconcileAutoscalerSafetyWakeupBacklogCancelsPeersAndScalesEarly(t *testing.T) {
	const triggerCount = 24
	broker := autoscaledBroker()
	triggers := safetyWakeupTriggers(triggerCount)
	backlogReady := make(chan struct{})
	blocked := make(chan struct{})
	js := newChannelAutoscalerJetStream(triggerCount)
	for index, trigger := range triggers {
		consumer := brokerutils.TriggerConsumerName(string(trigger.UID))
		if index == 0 {
			js.responses[consumer] = channelConsumerResponse{info: &nats.ConsumerInfo{NumPending: 1}, gate: backlogReady}
		} else {
			js.responses[consumer] = channelConsumerResponse{gate: blocked}
		}
	}
	kube, replicas := safetyWakeupScaleClient(0)
	r := &Reconciler{kubeClientSet: kube, js: js}
	ctx, cancel := context.WithTimeout(testContext(), 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- r.reconcileAutoscalerSafetyWakeup(ctx, broker, triggers, brokerutils.BrokerStreamName(broker), autoscaler.Settings{})
	}()

	select {
	case <-js.reached:
		close(backlogReady)
	case <-time.After(200 * time.Millisecond):
		cancel()
		close(backlogReady)
	}
	if err := <-done; err != nil {
		t.Fatalf("reconcileAutoscalerSafetyWakeup() error: %v", err)
	}
	if got := replicas.Load(); got != 1 {
		t.Fatalf("replicas = %d, want early wakeup to 1", got)
	}
	if got := js.maxActive.Load(); got != testSafetyWakeupConcurrency {
		t.Errorf("concurrent ConsumerInfo calls before backlog decision = %d, want %d", got, testSafetyWakeupConcurrency)
	}
	if got := js.calls.Load(); got > testSafetyWakeupConcurrency {
		t.Errorf("ConsumerInfo calls after backlog decision = %d, want at most %d", got, testSafetyWakeupConcurrency)
	}
	if got := js.active.Load(); got != 0 {
		t.Fatalf("active ConsumerInfo calls after return = %d, want 0 (worker leak)", got)
	}
}

func TestReconcileAutoscalerSafetyWakeupAddsPerBrokerDeadline(t *testing.T) {
	broker := autoscaledBroker()
	triggers := safetyWakeupTriggers(2)
	js := newChannelAutoscalerJetStream(len(triggers))
	for _, trigger := range triggers {
		consumer := brokerutils.TriggerConsumerName(string(trigger.UID))
		js.responses[consumer] = channelConsumerResponse{info: &nats.ConsumerInfo{}}
	}
	kube, _ := safetyWakeupScaleClient(0)
	r := &Reconciler{kubeClientSet: kube, js: js}

	if err := r.reconcileAutoscalerSafetyWakeup(testContext(), broker, triggers, brokerutils.BrokerStreamName(broker), autoscaler.Settings{}); err != nil {
		t.Fatal(err)
	}
	close(js.deadlines)
	deadlineCount := 0
	for deadline := range js.deadlines {
		deadlineCount++
		if remaining := time.Until(deadline); remaining <= 0 || remaining > 5*time.Second {
			t.Errorf("ConsumerInfo deadline in %s, want a positive per-Broker bound no greater than 5s", remaining)
		}
	}
	if deadlineCount != len(triggers) {
		t.Fatalf("ConsumerInfo contexts with deadlines = %d, want %d", deadlineCount, len(triggers))
	}
}

func TestReconcileAutoscalerSafetyWakeupAllConsumerErrorsWarnOnce(t *testing.T) {
	consumerErr := errors.New("consumer lookup unavailable")
	broker := autoscaledBroker()
	triggers := safetyWakeupTriggers(12)
	js := newChannelAutoscalerJetStream(len(triggers))
	for _, trigger := range triggers {
		consumer := brokerutils.TriggerConsumerName(string(trigger.UID))
		js.responses[consumer] = channelConsumerResponse{err: consumerErr}
	}
	kube, replicas := safetyWakeupScaleClient(0)
	r := &Reconciler{kubeClientSet: kube, js: js}
	ctx, logs := safetyWakeupObservedContext()

	if err := r.reconcileAutoscalerSafetyWakeup(ctx, broker, triggers, brokerutils.BrokerStreamName(broker), autoscaler.Settings{}); err != nil {
		t.Fatalf("all query errors must not make the Broker unavailable: %v", err)
	}
	if got := replicas.Load(); got != 0 {
		t.Fatalf("replicas = %d, want no unsafe scale decision", got)
	}
	if got := logLineCount(logs); got != 1 {
		t.Fatalf("warning logs = %d, want one aggregated warning; logs=%s", got, logs.String())
	}
	if !strings.Contains(logs.String(), "Could not complete JetStream consumer lag checks") {
		t.Errorf("warning = %q, want aggregated lag-check warning", logs.String())
	}
}

func TestReconcileAutoscalerSafetyWakeupCallerCancellationStopsWorkers(t *testing.T) {
	const triggerCount = 24
	broker := autoscaledBroker()
	triggers := safetyWakeupTriggers(triggerCount)
	blocked := make(chan struct{})
	js := newChannelAutoscalerJetStream(triggerCount)
	for _, trigger := range triggers {
		consumer := brokerutils.TriggerConsumerName(string(trigger.UID))
		js.responses[consumer] = channelConsumerResponse{gate: blocked}
	}
	kube, replicas := safetyWakeupScaleClient(0)
	r := &Reconciler{kubeClientSet: kube, js: js}
	ctx, cancel := context.WithCancel(testContext())
	done := make(chan error, 1)
	go func() {
		done <- r.reconcileAutoscalerSafetyWakeup(ctx, broker, triggers, brokerutils.BrokerStreamName(broker), autoscaler.Settings{})
	}()

	select {
	case <-js.reached:
	case <-time.After(200 * time.Millisecond):
	}
	cancel()
	err := <-done
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("reconcileAutoscalerSafetyWakeup() error = %v, want context.Canceled", err)
	}
	if got := js.calls.Load(); got > testSafetyWakeupConcurrency {
		t.Errorf("ConsumerInfo calls after cancellation = %d, want at most %d", got, testSafetyWakeupConcurrency)
	}
	if got := js.active.Load(); got != 0 {
		t.Fatalf("active ConsumerInfo calls after return = %d, want 0 (worker leak)", got)
	}
	if got := replicas.Load(); got != 0 {
		t.Fatalf("replicas = %d, want no scale after cancellation", got)
	}
}

func TestReconcileAutoscalerSafetyWakeupCanceledValidationStopsBeforeQueries(t *testing.T) {
	for _, tc := range []struct {
		name    string
		context func() (context.Context, context.CancelFunc)
		wantErr error
	}{
		{
			name: "canceled",
			context: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(testContext())
				cancel()
				return ctx, func() {}
			},
			wantErr: context.Canceled,
		},
		{
			name: "deadline exceeded",
			context: func() (context.Context, context.CancelFunc) {
				return context.WithDeadline(testContext(), time.Now().Add(-time.Second))
			},
			wantErr: context.DeadlineExceeded,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			broker := autoscaledBroker()
			triggers := safetyWakeupTriggers(10_000)
			js := newChannelAutoscalerJetStream(len(triggers))
			kube, replicas := safetyWakeupScaleClient(0)
			r := &Reconciler{kubeClientSet: kube, js: js}
			ctx, cancel := tc.context()
			defer cancel()
			started := time.Now()

			err := r.reconcileAutoscalerSafetyWakeup(ctx, broker, triggers, brokerutils.BrokerStreamName(broker), autoscaler.Settings{})
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("reconcileAutoscalerSafetyWakeup() error = %v, want %v", err, tc.wantErr)
			}
			if elapsed := time.Since(started); elapsed > 250*time.Millisecond {
				t.Errorf("canceled validation returned after %s, want prompt interruption", elapsed)
			}
			if got := js.calls.Load(); got != 0 {
				t.Fatalf("ConsumerInfo calls after canceled validation = %d, want 0", got)
			}
			if got := countScaleActions(kube.Actions(), "update"); got != 0 || replicas.Load() != 0 {
				t.Fatalf("scale changed after canceled validation: updates=%d replicas=%d", got, replicas.Load())
			}
		})
	}
}

func TestReconcileAutoscalerSafetyWakeupTenThousandErrorsHaveBoundedWarning(t *testing.T) {
	const queryCount = 10_000
	queryErr := errors.New("consumer unavailable")
	broker := autoscaledBroker()
	triggers := safetyWakeupTriggers(queryCount)
	js := newChannelAutoscalerJetStream(len(triggers))
	for _, trigger := range triggers {
		consumer := brokerutils.TriggerConsumerName(string(trigger.UID))
		js.responses[consumer] = channelConsumerResponse{err: queryErr}
	}
	kube, replicas := safetyWakeupScaleClient(0)
	r := &Reconciler{kubeClientSet: kube, js: js}
	ctx, logs := safetyWakeupObservedContext()

	if err := r.reconcileAutoscalerSafetyWakeup(ctx, broker, triggers, brokerutils.BrokerStreamName(broker), autoscaler.Settings{}); err != nil {
		t.Fatalf("query failures must not make the Broker unavailable: %v", err)
	}
	if got := js.calls.Load(); got != queryCount {
		t.Fatalf("ConsumerInfo calls = %d, want %d", got, queryCount)
	}
	if got := replicas.Load(); got != 0 {
		t.Fatalf("replicas = %d, want no unsafe scale", got)
	}
	if got := logLineCount(logs); got != 1 {
		t.Fatalf("warning logs = %d, want one aggregate; logs=%s", got, logs.String())
	}
	warning := logs.String()
	if !strings.Contains(warning, "10000 JetStream consumer lag checks failed") {
		t.Errorf("warning does not report all failures: %q", warning)
	}
	if !strings.Contains(warning, "showing 3") {
		t.Errorf("warning does not report sample bound 3: %q", warning)
	}
	if got := strings.Count(warning, "failed to read JetStream consumer"); got > 3 || got == 0 {
		t.Errorf("individual error samples = %d, want 1..3", got)
	}
	if got := len(warning); got > 4096 {
		t.Errorf("warning length = %d bytes, want fixed bound <=4096", got)
	}
}

func TestReconcileAutoscalerSafetyWakeupMultipleBacklogsUpdateAndEventOnce(t *testing.T) {
	const triggerCount = testSafetyWakeupConcurrency
	broker := autoscaledBroker()
	triggers := safetyWakeupTriggers(triggerCount)
	release := make(chan struct{})
	js := newChannelAutoscalerJetStream(triggerCount)
	for _, trigger := range triggers {
		consumer := brokerutils.TriggerConsumerName(string(trigger.UID))
		js.responses[consumer] = channelConsumerResponse{
			info: &nats.ConsumerInfo{NumPending: 2}, gate: release, ignoreCancellation: true,
		}
	}
	kube, replicas := safetyWakeupScaleClient(0)
	recorder := record.NewFakeRecorder(10)
	ctx := controller.WithEventRecorder(testContext(), recorder)
	r := &Reconciler{kubeClientSet: kube, js: js}
	done := make(chan error, 1)
	go func() {
		done <- r.reconcileAutoscalerSafetyWakeup(ctx, broker, triggers, brokerutils.BrokerStreamName(broker), autoscaler.Settings{})
	}()
	<-js.reached
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got := replicas.Load(); got != 1 {
		t.Fatalf("replicas = %d, want 1", got)
	}
	if got := countScaleActions(kube.Actions(), "update"); got != 1 {
		t.Fatalf("scale updates = %d, want exactly 1; actions=%#v", got, kube.Actions())
	}
	select {
	case event := <-recorder.Events:
		if !strings.Contains(event, ReasonAutoscalerSafetyWakeup) {
			t.Errorf("event = %q, want reason %q", event, ReasonAutoscalerSafetyWakeup)
		}
	default:
		t.Fatal("autoscaler safety wakeup event was not recorded")
	}
	select {
	case event := <-recorder.Events:
		t.Fatalf("unexpected duplicate autoscaler safety wakeup event: %q", event)
	default:
	}
}

func TestReconcileAutoscalerSafetyWakeupValidatesEveryTriggerBeforeQuerying(t *testing.T) {
	broker := autoscaledBroker()
	triggers := safetyWakeupTriggers(3)
	triggers[len(triggers)-1].Annotations = map[string]string{
		autoscaler.ActivationLagThresholdAnnotation: "not-an-integer",
	}
	js := newChannelAutoscalerJetStream(len(triggers))
	kube, replicas := safetyWakeupScaleClient(0)
	r := &Reconciler{kubeClientSet: kube, js: js}

	err := r.reconcileAutoscalerSafetyWakeup(testContext(), broker, triggers, brokerutils.BrokerStreamName(broker), autoscaler.Settings{})
	if err == nil || !strings.Contains(err.Error(), triggers[len(triggers)-1].Name) {
		t.Fatalf("reconcileAutoscalerSafetyWakeup() error = %v, want invalid trailing Trigger annotation", err)
	}
	if got := js.calls.Load(); got != 0 {
		t.Fatalf("ConsumerInfo calls = %d, want 0 before all Trigger annotations validate", got)
	}
	if got := countScaleActions(kube.Actions(), "update"); got != 0 || replicas.Load() != 0 {
		t.Fatalf("scale changed after validation failure: updates=%d replicas=%d", got, replicas.Load())
	}
}

func TestReconcileAutoscalerSafetyWakeupBacklogWinsOverQueryError(t *testing.T) {
	queryErr := errors.New("consumer unavailable")
	broker := autoscaledBroker()
	triggers := safetyWakeupTriggers(2)
	release := make(chan struct{})
	js := newChannelAutoscalerJetStream(len(triggers))
	js.responses[brokerutils.TriggerConsumerName(string(triggers[0].UID))] = channelConsumerResponse{
		err: queryErr, gate: release, ignoreCancellation: true,
	}
	js.responses[brokerutils.TriggerConsumerName(string(triggers[1].UID))] = channelConsumerResponse{
		info: &nats.ConsumerInfo{NumPending: 1}, gate: release, ignoreCancellation: true,
	}
	kube, replicas := safetyWakeupScaleClient(0)
	r := &Reconciler{kubeClientSet: kube, js: js}
	done := make(chan error, 1)
	go func() {
		done <- r.reconcileAutoscalerSafetyWakeup(testContext(), broker, triggers, brokerutils.BrokerStreamName(broker), autoscaler.Settings{})
	}()
	<-js.reached
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("actionable backlog should win over query error, got %v", err)
	}
	if got := replicas.Load(); got != 1 {
		t.Fatalf("replicas = %d, want wakeup to 1", got)
	}
	if got := js.calls.Load(); got != 2 {
		t.Fatalf("ConsumerInfo calls = %d, want both mixed results observed", got)
	}
}

func TestReconcileAutoscalerSafetyWakeupNilConsumerInfoIsFailure(t *testing.T) {
	broker := autoscaledBroker()
	triggers := safetyWakeupTriggers(1)
	js := newChannelAutoscalerJetStream(len(triggers))
	consumer := brokerutils.TriggerConsumerName(string(triggers[0].UID))
	js.responses[consumer] = channelConsumerResponse{info: nil}
	kube, replicas := safetyWakeupScaleClient(0)
	r := &Reconciler{kubeClientSet: kube, js: js}
	ctx, logs := safetyWakeupObservedContext()

	if err := r.reconcileAutoscalerSafetyWakeup(ctx, broker, triggers, brokerutils.BrokerStreamName(broker), autoscaler.Settings{}); err != nil {
		t.Fatalf("nil ConsumerInfo must not make the Broker unavailable: %v", err)
	}
	if got := replicas.Load(); got != 0 {
		t.Fatalf("replicas = %d, want no unsafe scale for nil ConsumerInfo", got)
	}
	if got := countScaleActions(kube.Actions(), "update"); got != 0 {
		t.Fatalf("scale updates = %d, want 0", got)
	}
	if got := logLineCount(logs); got != 1 {
		t.Fatalf("warning logs for nil ConsumerInfo = %d, want 1; logs=%s", got, logs.String())
	}
}

func TestReconcileAutoscalerSafetyWakeupKEDARecoverySkipsFinalUpdate(t *testing.T) {
	broker := autoscaledBroker()
	triggers := safetyWakeupTriggers(1)
	js := newChannelAutoscalerJetStream(len(triggers))
	consumer := brokerutils.TriggerConsumerName(string(triggers[0].UID))
	js.responses[consumer] = channelConsumerResponse{info: &nats.ConsumerInfo{NumPending: 1}}
	kube, replicas := safetyWakeupScaleClient(0)
	var scaleGets atomic.Int32
	kube.PrependReactor("get", "deployments", func(action clienttesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() != "scale" {
			return false, nil, nil
		}
		value := int32(0)
		if scaleGets.Add(1) > 1 {
			value = 2
			replicas.Store(value)
		}
		return true, &autoscalingv1.Scale{
			ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: resources.FilterName(testBrokerName), ResourceVersion: "1"},
			Spec:       autoscalingv1.ScaleSpec{Replicas: value},
		}, nil
	})
	recorder := record.NewFakeRecorder(10)
	ctx := controller.WithEventRecorder(testContext(), recorder)
	r := &Reconciler{kubeClientSet: kube, js: js}

	if err := r.reconcileAutoscalerSafetyWakeup(ctx, broker, triggers, brokerutils.BrokerStreamName(broker), autoscaler.Settings{}); err != nil {
		t.Fatal(err)
	}
	if got := scaleGets.Load(); got != 2 {
		t.Fatalf("scale GETs = %d, want initial and final refresh", got)
	}
	if got := countScaleActions(kube.Actions(), "update"); got != 0 {
		t.Fatalf("scale updates after KEDA recovery = %d, want 0", got)
	}
	if got := replicas.Load(); got != 2 {
		t.Fatalf("replicas = %d, want KEDA value 2 preserved", got)
	}
	select {
	case event := <-recorder.Events:
		t.Fatalf("unexpected wakeup event after KEDA recovery: %q", event)
	default:
	}
}

func TestReconcileAutoscalerSafetyWakeupScaleConflictDoesNotEmitEvent(t *testing.T) {
	broker := autoscaledBroker()
	triggers := safetyWakeupTriggers(1)
	js := newChannelAutoscalerJetStream(len(triggers))
	consumer := brokerutils.TriggerConsumerName(string(triggers[0].UID))
	js.responses[consumer] = channelConsumerResponse{info: &nats.ConsumerInfo{NumPending: 1}}
	kube, replicas := safetyWakeupScaleClient(0)
	kube.PrependReactor("update", "deployments", func(action clienttesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() != "scale" {
			return false, nil, nil
		}
		return true, nil, apierrs.NewConflict(appsv1.Resource("deployments/scale"), resources.FilterName(testBrokerName), errors.New("KEDA updated scale concurrently"))
	})
	recorder := record.NewFakeRecorder(10)
	ctx := controller.WithEventRecorder(testContext(), recorder)
	r := &Reconciler{kubeClientSet: kube, js: js}

	err := r.reconcileAutoscalerSafetyWakeup(ctx, broker, triggers, brokerutils.BrokerStreamName(broker), autoscaler.Settings{})
	if err == nil || !strings.Contains(err.Error(), "failed to wake filter") {
		t.Fatalf("reconcileAutoscalerSafetyWakeup() error = %v, want scale conflict", err)
	}
	if got := countScaleActions(kube.Actions(), "update"); got != 1 {
		t.Fatalf("scale update attempts = %d, want 1", got)
	}
	if got := replicas.Load(); got != 0 {
		t.Fatalf("replicas = %d, want unchanged zero after conflict", got)
	}
	select {
	case event := <-recorder.Events:
		t.Fatalf("unexpected wakeup event after failed scale update: %q", event)
	default:
	}
}

func safetyWakeupTriggers(count int) []*eventingv1.Trigger {
	triggers := make([]*eventingv1.Trigger, 0, count)
	for index := 0; index < count; index++ {
		trigger := testTrigger(testNamespace, fmt.Sprintf("trigger-%02d", index), testBrokerName)
		trigger.UID = types.UID(fmt.Sprintf("trigger-uid-%02d", index))
		triggers = append(triggers, trigger)
	}
	return triggers
}

func safetyWakeupScaleClient(initial int32) (*kubefake.Clientset, *atomic.Int32) {
	name := resources.FilterName(testBrokerName)
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: name},
		Spec:       appsv1.DeploymentSpec{Replicas: ptr.To(initial)},
	}
	kube := kubefake.NewSimpleClientset(deployment)
	replicas := &atomic.Int32{}
	replicas.Store(initial)
	kube.PrependReactor("get", "deployments", func(action clienttesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() != "scale" {
			return false, nil, nil
		}
		return true, &autoscalingv1.Scale{
			ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: name, ResourceVersion: "1"},
			Spec:       autoscalingv1.ScaleSpec{Replicas: replicas.Load()},
		}, nil
	})
	kube.PrependReactor("update", "deployments", func(action clienttesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() != "scale" {
			return false, nil, nil
		}
		scale := action.(clienttesting.UpdateAction).GetObject().(*autoscalingv1.Scale)
		replicas.Store(scale.Spec.Replicas)
		return true, scale, nil
	})
	return kube, replicas
}

func countScaleActions(actions []clienttesting.Action, verb string) int {
	count := 0
	for _, action := range actions {
		if action.Matches(verb, "deployments") && action.GetSubresource() == "scale" {
			count++
		}
	}
	return count
}

func safetyWakeupObservedContext() (context.Context, *bytes.Buffer) {
	logs := &bytes.Buffer{}
	core := zapcore.NewCore(zapcore.NewJSONEncoder(zap.NewDevelopmentEncoderConfig()), zapcore.AddSync(logs), zap.WarnLevel)
	return logging.WithLogger(testContext(), zap.New(core).Sugar()), logs
}

func logLineCount(logs *bytes.Buffer) int {
	trimmed := strings.TrimSpace(logs.String())
	if trimmed == "" {
		return 0
	}
	return strings.Count(trimmed, "\n") + 1
}

func TestReconcileAutoscalerFallbackForcesReplicaAfterCleanupError(t *testing.T) {
	broker := autoscaledBroker()
	name := resources.FilterName(broker.Name)
	deployment := ownedFilterDeployment(broker, 0)
	kube := kubefake.NewSimpleClientset(deployment)
	dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
	dynamicClient.PrependReactor("get", "scaledobjects", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, fmt.Errorf("KEDA API unavailable")
	})
	r := &Reconciler{
		kubeClientSet:        kube,
		dynamicClient:        dynamicClient,
		deploymentLister:     newDeploymentLister(deployment),
		filterImage:          "filter:latest",
		filterServiceAccount: "dp-sa",
		natsURL:              "nats://localhost:4222",
	}

	cleanupErr, reconcileErr := r.reconcileAutoscalerFallback(testContext(), broker, "TEST_STREAM", brokerconfig.DefaultBrokerConfig(), 1, fmt.Errorf("scaler failed"))
	if cleanupErr == nil {
		t.Fatal("cleanup error = nil, want KEDA API error")
	}
	if reconcileErr != nil {
		t.Fatalf("replica reconciliation failed: %v", reconcileErr)
	}
	got, err := kube.AppsV1().Deployments(broker.Namespace).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got.Spec.Replicas == nil || *got.Spec.Replicas != 1 {
		t.Fatalf("replicas = %v, want 1 despite cleanup error", got.Spec.Replicas)
	}
}

func TestReconcileAutoscalerFallbackRestoresMinimumWhileScaledObjectFinalizerIsStuck(t *testing.T) {
	for _, tc := range []struct {
		name         string
		minScale     string
		wantReplicas int32
	}{
		{name: "scale to zero restores one safety replica", minScale: "0", wantReplicas: 1},
		{name: "minimum above one is preserved", minScale: "3", wantReplicas: 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			broker := autoscaledBroker()
			broker.Annotations[autoscaler.MinScaleAnnotation] = tc.minScale
			name := resources.FilterName(broker.Name)
			deployment := ownedFilterDeployment(broker, 0)
			scaledObject := expectedScaledObject(t, broker)
			scaledObject.SetUID(types.UID("scaledobject-uid"))
			scaledObject.SetFinalizers([]string{"finalizer.keda.sh"})

			kube := kubefake.NewSimpleClientset(deployment)
			dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), scaledObject)
			actions := make([]string, 0, 2)
			kube.PrependReactor("update", "deployments", func(action clienttesting.Action) (bool, runtime.Object, error) {
				if action.GetSubresource() == "" {
					actions = append(actions, "deployment-update")
				}
				return false, nil, nil
			})
			deleteCalls := 0
			dynamicClient.PrependReactor("delete", "scaledobjects", func(clienttesting.Action) (bool, runtime.Object, error) {
				// Simulate an accepted deletion whose KEDA finalizer cannot be
				// removed because the operator is stopped. The object deliberately
				// remains in the fake tracker.
				deleteCalls++
				actions = append(actions, "scaledobject-delete")
				return true, nil, nil
			})
			r := &Reconciler{
				kubeClientSet:        kube,
				dynamicClient:        dynamicClient,
				deploymentLister:     newDeploymentLister(deployment),
				filterImage:          "filter:latest",
				filterServiceAccount: "dp-sa",
				natsURL:              "nats://localhost:4222",
			}

			cleanupErr, reconcileErr := r.reconcileAutoscalerFallback(
				testContext(), broker, "TEST_STREAM", brokerconfig.DefaultBrokerConfig(), tc.wantReplicas, fmt.Errorf("scaler failed"),
			)
			if cleanupErr == nil {
				t.Fatal("cleanup error = nil, want requeue while the ScaledObject finalizer remains")
			}
			if ok, delay := controller.IsRequeueKey(cleanupErr); !ok || delay != 30*time.Second {
				t.Fatalf("cleanup requeue = (%v, %s), want (true, 30s)", ok, delay)
			}
			if reconcileErr != nil {
				t.Fatalf("replica reconciliation failed: %v", reconcileErr)
			}
			if deleteCalls != 1 {
				t.Fatalf("ScaledObject delete calls = %d, want 1", deleteCalls)
			}
			if len(actions) != 2 || actions[0] != "deployment-update" || actions[1] != "scaledobject-delete" {
				t.Fatalf("mutation order = %v, want [deployment-update scaledobject-delete]", actions)
			}

			remaining, err := dynamicClient.Resource(autoscaler.ScaledObjectGVR).Namespace(broker.Namespace).Get(
				context.Background(), scaledObject.GetName(), metav1.GetOptions{},
			)
			if err != nil {
				t.Fatalf("get finalizing ScaledObject: %v", err)
			}
			if len(remaining.GetFinalizers()) != 1 || remaining.GetFinalizers()[0] != "finalizer.keda.sh" {
				t.Fatalf("ScaledObject finalizers = %v, want stuck KEDA finalizer", remaining.GetFinalizers())
			}

			got, err := kube.AppsV1().Deployments(broker.Namespace).Get(context.Background(), name, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if got.Spec.Replicas == nil || *got.Spec.Replicas != tc.wantReplicas {
				t.Fatalf("replicas = %v, want %d before ScaledObject deletion completes", got.Spec.Replicas, tc.wantReplicas)
			}
		})
	}
}

func TestReconcileAutoscalerDisabledRestoresStaticReplicasBeforeDeletingFinalizedScaledObject(t *testing.T) {
	for _, tc := range []struct {
		name             string
		templateReplicas int32
		wantReplicas     int32
	}{
		{name: "zero template remains zero", templateReplicas: 0, wantReplicas: 0},
		{name: "configured static replicas are restored", templateReplicas: 3, wantReplicas: 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			broker := autoscaledBroker()
			delete(broker.Annotations, autoscaler.ClassAnnotation)
			name := resources.FilterName(broker.Name)
			deployment := ownedFilterDeployment(broker, 0)
			scaledObject := expectedScaledObject(t, broker)
			scaledObject.SetUID(types.UID("scaledobject-uid"))
			scaledObject.SetFinalizers([]string{"finalizer.keda.sh"})
			brokerConfig := brokerconfig.DefaultBrokerConfig()
			brokerConfig.Filter = &brokerconfig.DeploymentTemplate{Replicas: ptr.To(tc.templateReplicas)}

			kube := kubefake.NewSimpleClientset(deployment)
			dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), scaledObject)
			actions := make([]string, 0, 2)
			kube.PrependReactor("update", "deployments", func(action clienttesting.Action) (bool, runtime.Object, error) {
				if action.GetSubresource() == "" {
					actions = append(actions, "deployment-update")
				}
				return false, nil, nil
			})
			dynamicClient.PrependReactor("delete", "scaledobjects", func(clienttesting.Action) (bool, runtime.Object, error) {
				// The stopped KEDA operator cannot clear its finalizer, so deletion is
				// accepted but does not remove the object from the fake tracker.
				actions = append(actions, "scaledobject-delete")
				return true, nil, nil
			})
			r := &Reconciler{
				kubeClientSet:        kube,
				dynamicClient:        dynamicClient,
				deploymentLister:     newDeploymentLister(deployment),
				filterImage:          "filter:latest",
				filterServiceAccount: "dp-sa",
				natsURL:              "nats://localhost:4222",
			}

			cleanupErr, reconcileErr := r.reconcileAutoscalerDisabled(testContext(), broker, "TEST_STREAM", brokerConfig)
			if cleanupErr == nil {
				t.Fatal("cleanup error = nil, want requeue while the ScaledObject finalizer remains")
			}
			if ok, delay := controller.IsRequeueKey(cleanupErr); !ok || delay != 30*time.Second {
				t.Fatalf("cleanup requeue = (%v, %s), want (true, 30s)", ok, delay)
			}
			if reconcileErr != nil {
				t.Fatalf("replica reconciliation failed: %v", reconcileErr)
			}
			wantActions := []string{"scaledobject-delete"}
			if tc.wantReplicas != 0 {
				wantActions = []string{"deployment-update", "scaledobject-delete"}
			}
			if fmt.Sprint(actions) != fmt.Sprint(wantActions) {
				t.Fatalf("mutation order = %v, want %v", actions, wantActions)
			}

			remaining, err := dynamicClient.Resource(autoscaler.ScaledObjectGVR).Namespace(broker.Namespace).Get(
				context.Background(), scaledObject.GetName(), metav1.GetOptions{},
			)
			if err != nil {
				t.Fatalf("get finalizing ScaledObject: %v", err)
			}
			if len(remaining.GetFinalizers()) != 1 || remaining.GetFinalizers()[0] != "finalizer.keda.sh" {
				t.Fatalf("ScaledObject finalizers = %v, want stuck KEDA finalizer", remaining.GetFinalizers())
			}

			got, err := kube.AppsV1().Deployments(broker.Namespace).Get(context.Background(), name, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if got.Spec.Replicas == nil || *got.Spec.Replicas != tc.wantReplicas {
				t.Fatalf("replicas = %v, want %d before ScaledObject deletion completes", got.Spec.Replicas, tc.wantReplicas)
			}
		})
	}
}

func TestReconcileAutoscalerDisabledDoesNotChangeForeignOwnedTarget(t *testing.T) {
	broker := autoscaledBroker()
	delete(broker.Annotations, autoscaler.ClassAnnotation)
	name := resources.FilterName(broker.Name)
	deployment := ownedFilterDeployment(broker, 0)
	foreign := expectedScaledObject(t, broker)
	foreign.SetOwnerReferences(nil)
	brokerConfig := brokerconfig.DefaultBrokerConfig()
	brokerConfig.Filter = &brokerconfig.DeploymentTemplate{Replicas: ptr.To[int32](3)}

	kube := kubefake.NewSimpleClientset(deployment)
	dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), foreign)
	r := &Reconciler{
		kubeClientSet:        kube,
		dynamicClient:        dynamicClient,
		deploymentLister:     newDeploymentLister(deployment),
		filterImage:          "filter:latest",
		filterServiceAccount: "dp-sa",
		natsURL:              "nats://localhost:4222",
	}

	cleanupErr, reconcileErr := r.reconcileAutoscalerDisabled(testContext(), broker, "TEST_STREAM", brokerConfig)
	if cleanupErr != nil {
		t.Fatalf("cleanupErr = %v, want nil for hard ownership error", cleanupErr)
	}
	if !errors.Is(reconcileErr, errScaledObjectNotOwned) {
		t.Fatalf("reconcileErr = %v, want errScaledObjectNotOwned", reconcileErr)
	}
	for _, action := range kube.Actions() {
		if action.Matches("update", "deployments") {
			t.Fatalf("unexpected Deployment update before rejecting foreign ScaledObject: %#v", action)
		}
	}
	for _, action := range dynamicClient.Actions() {
		if action.Matches("delete", "scaledobjects") {
			t.Fatalf("unexpected foreign ScaledObject deletion: %#v", action)
		}
	}

	got, err := kube.AppsV1().Deployments(broker.Namespace).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got.Spec.Replicas == nil || *got.Spec.Replicas != 0 {
		t.Fatalf("replicas = %v, want unchanged foreign-owned target", got.Spec.Replicas)
	}
}

func TestReconcileAutoscalerFallbackDoesNotChangeForeignDeployment(t *testing.T) {
	broker := autoscaledBroker()
	name := resources.FilterName(broker.Name)
	foreignDeployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: broker.Namespace,
			Name:      name,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1",
				Kind:       "Deployment",
				Name:       "foreign-owner",
				UID:        types.UID("foreign-owner-uid"),
				Controller: ptr.To(true),
			}},
		},
		Spec: appsv1.DeploymentSpec{Replicas: ptr.To[int32](0)},
	}
	scaledObject := expectedScaledObject(t, broker)
	scaledObject.SetUID(types.UID("scaledobject-uid"))
	kube := kubefake.NewSimpleClientset(foreignDeployment)
	dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), scaledObject)
	r := &Reconciler{
		kubeClientSet:        kube,
		dynamicClient:        dynamicClient,
		deploymentLister:     newDeploymentLister(foreignDeployment),
		filterImage:          "filter:latest",
		filterServiceAccount: "dp-sa",
		natsURL:              "nats://localhost:4222",
	}

	cleanupErr, reconcileErr := r.reconcileAutoscalerFallback(
		testContext(), broker, "TEST_STREAM", brokerconfig.DefaultBrokerConfig(), 3, fmt.Errorf("scaler failed"),
	)
	if cleanupErr != nil {
		t.Fatalf("cleanupErr = %v, want nil for hard Deployment ownership error", cleanupErr)
	}
	if !errors.Is(reconcileErr, errFilterDeploymentNotOwned) {
		t.Fatalf("reconcileErr = %v, want errFilterDeploymentNotOwned", reconcileErr)
	}
	for _, action := range kube.Actions() {
		if action.Matches("update", "deployments") {
			t.Fatalf("unexpected foreign Deployment update: %#v", action)
		}
	}
	for _, action := range dynamicClient.Actions() {
		if action.Matches("delete", "scaledobjects") {
			t.Fatalf("unexpected ScaledObject deletion after foreign Deployment rejection: %#v", action)
		}
	}

	got, err := kube.AppsV1().Deployments(broker.Namespace).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got.Spec.Replicas == nil || *got.Spec.Replicas != 0 {
		t.Fatalf("replicas = %v, want unchanged foreign Deployment", got.Spec.Replicas)
	}
}

func TestReconcileAutoscalerFallbackDoesNotFightForeignOwner(t *testing.T) {
	broker := autoscaledBroker()
	name := resources.FilterName(broker.Name)
	deployment := ownedFilterDeployment(broker, 0)
	foreign := expectedScaledObject(t, broker)
	foreign.SetOwnerReferences(nil)
	kube := kubefake.NewSimpleClientset(deployment)
	r := &Reconciler{
		kubeClientSet:    kube,
		dynamicClient:    dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), foreign),
		deploymentLister: newDeploymentLister(deployment),
	}

	cleanupErr, reconcileErr := r.reconcileAutoscalerFallback(testContext(), broker, "TEST_STREAM", brokerconfig.DefaultBrokerConfig(), 1, fmt.Errorf("invalid settings"))
	if cleanupErr != nil {
		t.Fatalf("cleanupErr = %v, want hard ownership error", cleanupErr)
	}
	if !errors.Is(reconcileErr, errScaledObjectNotOwned) {
		t.Fatalf("reconcileErr = %v, want errScaledObjectNotOwned", reconcileErr)
	}
	got, err := kube.AppsV1().Deployments(broker.Namespace).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got.Spec.Replicas == nil || *got.Spec.Replicas != 0 {
		t.Fatalf("replicas = %v, want unchanged foreign-owned target", got.Spec.Replicas)
	}
}
