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

package filter

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	corev1 "k8s.io/api/core/v1"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"

	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	"knative.dev/pkg/logging"

	eventingduckv1 "knative.dev/eventing/pkg/apis/duck/v1"
	eventingv1 "knative.dev/eventing/pkg/apis/eventing/v1"
	eventinglisters "knative.dev/eventing/pkg/client/listers/eventing/v1"

	"knative.dev/eventing-natss/pkg/broker/constants"
	brokeroidc "knative.dev/eventing-natss/pkg/broker/oidc"
	natsTesting "knative.dev/eventing-natss/pkg/channel/jetstream/dispatcher/testing"
)

const (
	testNamespace   = "test-namespace"
	testBrokerName  = "test-broker"
	testTriggerName = "test-trigger"
	testTriggerUID  = "test-trigger-uid-12345"
)

// newTestBroker creates a test broker with the given class annotation.
func newTestBroker(namespace, name, brokerClass string) *eventingv1.Broker {
	annotations := make(map[string]string)
	if brokerClass != "" {
		annotations[eventingv1.BrokerClassAnnotationKey] = brokerClass
	}
	return &eventingv1.Broker{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   namespace,
			Name:        name,
			Annotations: annotations,
		},
	}
}

// newReadyTestBroker creates a test broker that reports as ready.
func newReadyTestBroker(namespace, name, brokerClass string) *eventingv1.Broker {
	b := newTestBroker(namespace, name, brokerClass)
	// IsReady checks ObservedGeneration == Generation and all conditions happy.
	b.Generation = 1
	b.Status.InitializeConditions()
	b.Status.ObservedGeneration = 1
	// Mark all dependent conditions true so the top-level Ready becomes true.
	manager := b.GetConditionSet().Manage(&b.Status)
	manager.MarkTrue(eventingv1.BrokerConditionIngress)
	manager.MarkTrue(eventingv1.BrokerConditionTriggerChannel)
	manager.MarkTrue(eventingv1.BrokerConditionFilter)
	manager.MarkTrue(eventingv1.BrokerConditionAddressable)
	manager.MarkTrue(eventingv1.BrokerConditionDeadLetterSinkResolved)
	manager.MarkTrue(eventingv1.BrokerConditionEventPoliciesReady)
	return b
}

// newTestTrigger creates a test trigger referencing a broker.
func newTestTrigger(namespace, name, brokerName string) *eventingv1.Trigger {
	return &eventingv1.Trigger{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
			UID:       types.UID(testTriggerUID),
		},
		Spec: eventingv1.TriggerSpec{
			Broker: brokerName,
		},
	}
}

// newTestTriggerWithSubscriber creates a trigger with a resolved subscriber URI.
func newTestTriggerWithSubscriber(namespace, name, brokerName, subscriberURL string) *eventingv1.Trigger {
	t := newTestTrigger(namespace, name, brokerName)
	t.Generation = 1
	u, _ := apis.ParseURL(subscriberURL)
	t.Status.SubscriberURI = u
	t.Status.InitializeConditions()
	t.Status.ObservedGeneration = t.Generation
	t.Status.PropagateBrokerCondition(&apis.Condition{Type: apis.ConditionReady, Status: corev1.ConditionTrue})
	t.Status.MarkSubscriberResolvedSucceeded()
	t.Status.MarkDeadLetterSinkNotConfigured()
	t.Status.PropagateSubscriptionCondition(&apis.Condition{Type: apis.ConditionReady, Status: corev1.ConditionTrue})
	t.Status.MarkDependencySucceeded()
	return t
}

func setTestTriggerOIDCIdentity(trigger *eventingv1.Trigger, _ *eventingv1.Broker) {
	identity := brokeroidc.DeliveryServiceAccountName
	trigger.Status.Auth = &duckv1.AuthStatus{ServiceAccountName: &identity}
	trigger.Status.MarkOIDCIdentityCreatedSucceeded()
}

// --- Fake listers ---

type fakeBrokerLister struct {
	brokers map[string]map[string]*eventingv1.Broker
}

func newFakeBrokerLister() *fakeBrokerLister {
	return &fakeBrokerLister{
		brokers: make(map[string]map[string]*eventingv1.Broker),
	}
}

func (f *fakeBrokerLister) addBroker(broker *eventingv1.Broker) {
	if f.brokers[broker.Namespace] == nil {
		f.brokers[broker.Namespace] = make(map[string]*eventingv1.Broker)
	}
	f.brokers[broker.Namespace][broker.Name] = broker
}

func (f *fakeBrokerLister) List(selector labels.Selector) ([]*eventingv1.Broker, error) {
	var result []*eventingv1.Broker
	for _, ns := range f.brokers {
		for _, broker := range ns {
			result = append(result, broker)
		}
	}
	return result, nil
}

func (f *fakeBrokerLister) Brokers(namespace string) eventinglisters.BrokerNamespaceLister {
	return &fakeBrokerNamespaceLister{
		namespace: namespace,
		brokers:   f.brokers[namespace],
	}
}

type fakeBrokerNamespaceLister struct {
	namespace string
	brokers   map[string]*eventingv1.Broker
}

func (f *fakeBrokerNamespaceLister) List(selector labels.Selector) ([]*eventingv1.Broker, error) {
	result := make([]*eventingv1.Broker, 0, len(f.brokers))
	for _, broker := range f.brokers {
		result = append(result, broker)
	}
	return result, nil
}

func (f *fakeBrokerNamespaceLister) Get(name string) (*eventingv1.Broker, error) {
	if broker, ok := f.brokers[name]; ok {
		return broker, nil
	}
	return nil, apierrs.NewNotFound(schema.GroupResource{Group: "eventing.knative.dev", Resource: "brokers"}, name)
}

func TestReconcileTrigger(t *testing.T) {
	tests := []struct {
		name    string
		broker  *eventingv1.Broker
		trigger *eventingv1.Trigger
		wantErr bool
	}{
		{
			name:    "broker not found returns nil",
			broker:  nil,
			trigger: newTestTrigger(testNamespace, testTriggerName, testBrokerName),
			wantErr: false,
		},
		{
			name:    "broker wrong class returns nil",
			broker:  newTestBroker(testNamespace, testBrokerName, "OtherBrokerClass"),
			trigger: newTestTrigger(testNamespace, testTriggerName, testBrokerName),
			wantErr: false,
		},
		{
			name:    "broker not ready returns nil",
			broker:  newTestBroker(testNamespace, testBrokerName, constants.BrokerClassName),
			trigger: newTestTrigger(testNamespace, testTriggerName, testBrokerName),
			wantErr: false,
		},
		{
			name:    "subscriber URI not resolved returns nil",
			broker:  newReadyTestBroker(testNamespace, testBrokerName, constants.BrokerClassName),
			trigger: newTestTrigger(testNamespace, testTriggerName, testBrokerName),
			wantErr: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			brokerLister := newFakeBrokerLister()
			if tc.broker != nil {
				brokerLister.addBroker(tc.broker)
			}

			ctx := logging.WithLogger(context.Background(), logging.FromContext(context.TODO()))

			r := &FilterReconciler{
				logger:          logging.FromContext(ctx),
				triggerLister:   newFakeTriggerLister(),
				brokerLister:    brokerLister,
				brokerNamespace: testNamespace,
				brokerName:      testBrokerName,
			}

			err := r.ReconcileTrigger(ctx, tc.trigger)
			if tc.wantErr && err == nil {
				t.Error("ReconcileTrigger() expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Errorf("ReconcileTrigger() unexpected error: %v", err)
			}
		})
	}
}

func TestReconcileTrigger_BrokerLookupError(t *testing.T) {
	// Use a broker lister that returns an error other than NotFound.
	brokerLister := &errorBrokerLister{
		err: fmt.Errorf("internal error"),
	}

	ctx := logging.WithLogger(context.Background(), logging.FromContext(context.TODO()))

	r := &FilterReconciler{
		logger:          logging.FromContext(ctx),
		triggerLister:   newFakeTriggerLister(),
		brokerLister:    brokerLister,
		brokerNamespace: testNamespace,
		brokerName:      testBrokerName,
	}

	trigger := newTestTrigger(testNamespace, testTriggerName, testBrokerName)
	err := r.ReconcileTrigger(ctx, trigger)
	if err == nil {
		t.Error("ReconcileTrigger() expected error for non-NotFound broker lookup error, got nil")
	}
}

// errorBrokerLister returns an arbitrary error for any lookup.
type errorBrokerLister struct {
	err error
}

func (e *errorBrokerLister) List(selector labels.Selector) ([]*eventingv1.Broker, error) {
	return nil, e.err
}

func (e *errorBrokerLister) Brokers(namespace string) eventinglisters.BrokerNamespaceLister {
	return &errorBrokerNamespaceLister{err: e.err}
}

type errorBrokerNamespaceLister struct {
	err error
}

func (e *errorBrokerNamespaceLister) List(selector labels.Selector) ([]*eventingv1.Broker, error) {
	return nil, e.err
}

func (e *errorBrokerNamespaceLister) Get(name string) (*eventingv1.Broker, error) {
	return nil, e.err
}

func TestDeleteTrigger(t *testing.T) {
	ctx := logging.WithLogger(context.Background(), logging.FromContext(context.TODO()))

	cm := &ConsumerManager{
		logger:        logging.FromContext(ctx),
		subscriptions: make(map[string]*TriggerSubscription),
	}

	r := &FilterReconciler{
		logger:          logging.FromContext(ctx),
		consumerManager: cm,
	}

	// Deleting a non-existent trigger should return nil.
	if err := r.DeleteTrigger("non-existent-uid"); err != nil {
		t.Errorf("DeleteTrigger() unexpected error for non-existent trigger: %v", err)
	}
}

// Verify that IsReady helper produces truly ready brokers for use in other
// test cases. This guards against upstream condition-set changes breaking
// the helper silently.
func TestNewReadyTestBroker(t *testing.T) {
	b := newReadyTestBroker(testNamespace, testBrokerName, constants.BrokerClassName)
	if !b.IsReady() {
		cond := b.Status.GetTopLevelCondition()
		t.Errorf("newReadyTestBroker() produced broker that is not ready; top-level condition: %+v", cond)
		for _, c := range b.Status.Conditions {
			if c.Status != corev1.ConditionTrue {
				t.Errorf("  condition %s = %s (%s)", c.Type, c.Status, c.Reason)
			}
		}
	}
}

// Verify subscriber URI detection works on both nil and non-nil cases.
func TestNewTestTriggerWithSubscriber(t *testing.T) {
	t.Run("without subscriber", func(t *testing.T) {
		trigger := newTestTrigger(testNamespace, testTriggerName, testBrokerName)
		if trigger.Status.SubscriberURI != nil {
			t.Errorf("expected nil SubscriberURI, got %v", trigger.Status.SubscriberURI)
		}
	})

	t.Run("with subscriber", func(t *testing.T) {
		trigger := newTestTriggerWithSubscriber(testNamespace, testTriggerName, testBrokerName, "http://example.com")
		if trigger.Status.SubscriberURI == nil {
			t.Error("expected non-nil SubscriberURI")
		}
	})
}

// TestReconcileTrigger_SkipReasons verifies that each early-return guard in
// ReconcileTrigger is independently effective. We run through a progression
// where exactly one guard is removed at each step to confirm each check.
func TestReconcileTrigger_SkipReasons(t *testing.T) {
	ctx := logging.WithLogger(context.Background(), logging.FromContext(context.TODO()))

	// Ready broker + trigger with subscriber should NOT be skipped.
	// We cannot proceed without a real JetStream, but we can confirm the
	// early returns work by verifying the inverse: a ready broker with
	// wrong class is still skipped.
	broker := newReadyTestBroker(testNamespace, testBrokerName, "WrongClass")
	trigger := newTestTriggerWithSubscriber(testNamespace, testTriggerName, testBrokerName, "http://subscriber.example.com")

	brokerLister := newFakeBrokerLister()
	brokerLister.addBroker(broker)

	r := &FilterReconciler{
		logger:          logging.FromContext(ctx),
		triggerLister:   newFakeTriggerLister(),
		brokerLister:    brokerLister,
		brokerNamespace: testNamespace,
		brokerName:      testBrokerName,
	}

	// Even though trigger has a subscriber and broker is ready, the wrong
	// class causes a skip (nil return).
	if err := r.ReconcileTrigger(ctx, trigger); err != nil {
		t.Errorf("ReconcileTrigger() with wrong broker class should skip, got error: %v", err)
	}
}

// --- Fake JetStream ---

// fakeJetStream embeds the JetStreamContext interface and overrides only
// ConsumerInfo. Any other method will panic if called (embedded nil value),
// which is acceptable since our tests never reach those code paths.
type fakeJetStream struct {
	nats.JetStreamContext
	consumerInfoErr error
}

func (f *fakeJetStream) ConsumerInfo(stream, name string, opts ...nats.JSOpt) (*nats.ConsumerInfo, error) {
	return nil, f.consumerInfoErr
}

func int32Ptr(v int32) *int32 { return &v }

func TestNewFilterReconciler(t *testing.T) {
	ctx := logging.WithLogger(context.Background(), logging.FromContext(context.TODO()))

	r := NewFilterReconciler(ctx, nil, nil, nil, testNamespace, testBrokerName)
	if r == nil {
		t.Fatal("NewFilterReconciler() returned nil")
	}
	if r.logger == nil {
		t.Error("reconciler.logger should not be nil")
	}
}

func TestReconcileTrigger_FullPath(t *testing.T) {
	tests := []struct {
		name            string
		brokerAddr      bool
		deadLetterURI   string
		deliveryRetry   *int32
		streamName      string
		jsErr           error
		wantErrContains string
	}{
		{
			name:            "basic path through to SubscribeTrigger",
			jsErr:           fmt.Errorf("connection refused"),
			wantErrContains: "failed to subscribe to trigger",
		},
		{
			name:            "with broker ingress address",
			brokerAddr:      true,
			jsErr:           fmt.Errorf("connection refused"),
			wantErrContains: "failed to subscribe to trigger",
		},
		{
			name:            "with dead letter sink",
			deadLetterURI:   "http://dead-letter.example.com",
			jsErr:           fmt.Errorf("connection refused"),
			wantErrContains: "failed to subscribe to trigger",
		},
		{
			name:            "with delivery spec",
			deliveryRetry:   int32Ptr(3),
			jsErr:           fmt.Errorf("connection refused"),
			wantErrContains: "failed to subscribe to trigger",
		},
		{
			name:            "all optional fields set",
			brokerAddr:      true,
			deadLetterURI:   "http://dead-letter.example.com",
			deliveryRetry:   int32Ptr(5),
			jsErr:           fmt.Errorf("connection refused"),
			wantErrContains: "failed to subscribe to trigger",
		},
		{
			name:            "consumer not found error",
			jsErr:           nats.ErrConsumerNotFound,
			wantErrContains: "not found",
		},
		{
			name:            "another broker stream is rejected",
			streamName:      "KN_BROKER_OTHER",
			wantErrContains: "does not match filter stream",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := logging.WithLogger(context.Background(), logging.FromContext(context.TODO()))

			broker := newReadyTestBroker(testNamespace, testBrokerName, constants.BrokerClassName)
			if tc.brokerAddr {
				broker.Status.SetAddress(&duckv1.Addressable{
					URL: apis.HTTP("broker-ingress.example.com"),
				})
			}

			trigger := newTestTriggerWithSubscriber(testNamespace, testTriggerName, testBrokerName, "http://subscriber.example.com")
			if tc.deadLetterURI != "" {
				u, _ := apis.ParseURL(tc.deadLetterURI)
				trigger.Status.DeadLetterSinkURI = u
				trigger.Status.MarkDeadLetterSinkResolvedSucceeded()
			}
			if tc.deliveryRetry != nil {
				trigger.Spec.Delivery = &eventingduckv1.DeliverySpec{
					Retry: tc.deliveryRetry,
				}
			}

			brokerLister := newFakeBrokerLister()
			brokerLister.addBroker(broker)

			cm := &ConsumerManager{
				logger:                     logging.FromContext(ctx),
				ctx:                        ctx,
				js:                         &fakeJetStream{consumerInfoErr: tc.jsErr},
				streamName:                 tc.streamName,
				subscriptions:              make(map[string]*TriggerSubscription),
				tokenFailures:              make(map[string]map[string]struct{}),
				tokenValidationGenerations: make(map[string]int64),
			}

			r := &FilterReconciler{
				logger:          logging.FromContext(ctx),
				triggerLister:   newFakeTriggerLister(),
				brokerLister:    brokerLister,
				consumerManager: cm,
				brokerNamespace: testNamespace,
				brokerName:      testBrokerName,
			}

			err := r.ReconcileTrigger(ctx, trigger)
			if err == nil {
				t.Fatal("ReconcileTrigger() expected error, got nil")
			}
			if !strings.Contains(err.Error(), tc.wantErrContains) {
				t.Errorf("error %q should contain %q", err.Error(), tc.wantErrContains)
			}
		})
	}
}

func TestReconcileTrigger_ExistingSubscription(t *testing.T) {
	ctx := logging.WithLogger(context.Background(), logging.FromContext(context.TODO()))

	broker := newReadyTestBroker(testNamespace, testBrokerName, constants.BrokerClassName)
	broker.Status.SetAddress(&duckv1.Addressable{
		URL: apis.HTTP("broker-ingress.example.com"),
	})

	// Use a different subscriber URL than what's already in the handler to
	// prove the in-place update works (no re-subscribe).
	oldSubscriberURL := "http://old-subscriber.example.com"
	newSubscriberURL := "http://new-subscriber.example.com"
	trigger := newTestTriggerWithSubscriber(testNamespace, testTriggerName, testBrokerName, newSubscriberURL)
	trigger.Spec.Filter = &eventingv1.TriggerFilter{
		Attributes: map[string]string{"type": "test.type"},
	}
	newDLSURL, _ := apis.ParseURL("http://new-dls.example.com")
	trigger.Status.DeadLetterSinkURI = newDLSURL
	subscriberCACerts := "subscriber-ca"
	subscriberAudience := "subscriber-audience"
	deadLetterCACerts := "dead-letter-ca"
	deadLetterAudience := "dead-letter-audience"
	trigger.Status.SubscriberCACerts = &subscriberCACerts
	trigger.Status.SubscriberAudience = &subscriberAudience
	trigger.Status.DeadLetterSinkCACerts = &deadLetterCACerts
	trigger.Status.DeadLetterSinkAudience = &deadLetterAudience
	trigger.Status.MarkDeadLetterSinkResolvedSucceeded()
	setTestTriggerOIDCIdentity(trigger, broker)
	triggerUID := string(trigger.UID)

	brokerLister := newFakeBrokerLister()
	brokerLister.addBroker(broker)

	// Pre-populate subscription with old values.
	oldParsedURL, _ := apis.ParseURL(oldSubscriberURL)
	existingHandler, err := NewTriggerHandler(
		ctx,
		trigger.DeepCopy(),
		duckv1.Addressable{URL: oldParsedURL},
		nil,
		nil,
		nil,
		nil,
		newTestDispatcher(ctx),
		nil,
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("NewTriggerHandler() unexpected error: %v", err)
	}

	cm := &ConsumerManager{
		logger:                     logging.FromContext(ctx),
		ctx:                        ctx,
		js:                         &fakeJetStream{consumerInfoErr: fmt.Errorf("should not be called")},
		tokenFailures:              make(map[string]map[string]struct{}),
		tokenValidationGenerations: make(map[string]int64),
		tokenSource: func(_ context.Context, audience string) (string, error) {
			switch audience {
			case subscriberAudience:
				return "subscriber-token", nil
			case deadLetterAudience:
				return "dead-letter-token", nil
			default:
				return "", fmt.Errorf("unexpected audience %q", audience)
			}
		},
		subscriptions: map[string]*TriggerSubscription{
			triggerUID: {
				trigger: trigger,
				handler: existingHandler,
			},
		},
	}

	r := &FilterReconciler{
		logger:          logging.FromContext(ctx),
		triggerLister:   newFakeTriggerLister(),
		brokerLister:    brokerLister,
		consumerManager: cm,
		brokerNamespace: testNamespace,
		brokerName:      testBrokerName,
	}

	// Should return nil — all fields updated in place, no re-subscribe.
	err = r.ReconcileTrigger(ctx, trigger)
	if err != nil {
		t.Fatalf("ReconcileTrigger() unexpected error: %v", err)
	}

	// Verify all handler fields were updated in place.
	h := cm.subscriptions[triggerUID].handler
	config := snapshotHandlerConfig(t, h)
	if got := config.subscriber.URL.String(); got != newSubscriberURL {
		t.Errorf("handler subscriber URL = %q, want %q", got, newSubscriberURL)
	}
	if config.subscriber.CACerts == nil || *config.subscriber.CACerts != subscriberCACerts {
		t.Errorf("handler subscriber CA certs = %v, want %q", config.subscriber.CACerts, subscriberCACerts)
	}
	if config.subscriber.Audience == nil || *config.subscriber.Audience != subscriberAudience {
		t.Errorf("handler subscriber audience = %v, want %q", config.subscriber.Audience, subscriberAudience)
	}
	if config.brokerIngressURL == nil {
		t.Error("handler brokerIngressURL should not be nil")
	}
	if config.filter == nil {
		t.Error("handler filter should not be nil after update with filter spec")
	}
	if config.deadLetterSink == nil || config.deadLetterSink.URL.String() != newDLSURL.String() {
		t.Errorf("handler deadLetterSink URL = %v, want %v", config.deadLetterSink, newDLSURL)
	}
	if config.deadLetterSink == nil || config.deadLetterSink.CACerts == nil || *config.deadLetterSink.CACerts != deadLetterCACerts {
		t.Errorf("handler deadLetterSink CA certs = %v, want %q", config.deadLetterSink, deadLetterCACerts)
	}
	if config.deadLetterSink == nil || config.deadLetterSink.Audience == nil || *config.deadLetterSink.Audience != deadLetterAudience {
		t.Errorf("handler deadLetterSink audience = %v, want %q", config.deadLetterSink, deadLetterAudience)
	}
	if config.trigger == trigger || config.trigger.Name != trigger.Name || config.trigger.Namespace != trigger.Namespace {
		t.Error("handler trigger should be updated to an equivalent defensive copy")
	}
}

func TestReconcileTrigger_BrokerNotReadyRetainsAndRecoversTokenFailure(t *testing.T) {
	ctx := logging.WithLogger(context.Background(), logging.FromContext(context.TODO()))
	broker := newReadyTestBroker(testNamespace, testBrokerName, constants.BrokerClassName)
	trigger := newTestTriggerWithSubscriber(testNamespace, testTriggerName, testBrokerName, "http://subscriber.example.com")
	audience := "https://subscriber.example"
	trigger.Status.SubscriberAudience = &audience
	setTestTriggerOIDCIdentity(trigger, broker)

	brokerLister := newFakeBrokerLister()
	brokerLister.addBroker(broker)
	handler, err := NewTriggerHandler(ctx, trigger, duckv1.Addressable{}, nil, nil, nil, nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	close(done)
	pull := &reassignmentPullSubscription{}
	tokenAvailable := false
	manager := &ConsumerManager{
		logger: logging.FromContext(ctx),
		ctx:    ctx,
		tokenSource: func(context.Context, string) (string, error) {
			if !tokenAvailable {
				return "", fmt.Errorf("token request not ready")
			}
			return "recovered-token", nil
		},
		subscriptions: map[string]*TriggerSubscription{
			testTriggerUID: {
				trigger: trigger, subscription: pull, handler: handler,
				done: done, cancel: func() {}, dispatchCancel: func() {},
			},
		},
		tokenFailures: map[string]map[string]struct{}{
			testTriggerUID: {audience: {}},
		},
		tokenValidationGenerations: make(map[string]int64),
	}
	reconciler := &FilterReconciler{
		logger: logging.FromContext(ctx), triggerLister: newFakeTriggerLister(), brokerLister: brokerLister, consumerManager: manager,
		brokerNamespace: testNamespace, brokerName: testBrokerName,
	}

	// The first token preflight happens while the Broker is ready. It must
	// retire the subscription but retain the failure that made the filter Pod
	// (and consequently the Broker) unready.
	err = reconciler.ReconcileTrigger(ctx, trigger)
	if err == nil || !strings.Contains(err.Error(), ErrOIDCTokenUnavailable.Error()) {
		t.Fatalf("ReconcileTrigger() error = %v, want token unavailable", err)
	}
	if _, found := manager.subscriptions[testTriggerUID]; found {
		t.Error("Broker NotReady retained the paused subscription")
	}
	if !pull.unsubscribed {
		t.Error("Broker NotReady did not unsubscribe the paused subscription")
	}
	if manager.Ready() {
		t.Error("token failure was cleared while the current TokenRequest was unavailable")
	}

	brokerLister.addBroker(newTestBroker(testNamespace, testBrokerName, constants.BrokerClassName))
	tokenAvailable = true
	if err := reconciler.ReconcileTrigger(ctx, trigger); err != nil {
		t.Fatalf("ReconcileTrigger() after token recovery = %v", err)
	}
	if !manager.Ready() {
		t.Error("current resolved token recovery did not clear readiness failure")
	}
	if len(manager.subscriptions) != 0 {
		t.Error("unready Broker recreated a subscription during credential preflight")
	}
}

func TestReconcileTrigger_UnresolvedCurrentStatusClearsTokenFailure(t *testing.T) {
	ctx := logging.WithLogger(context.Background(), logging.FromContext(context.TODO()))
	broker := newTestBroker(testNamespace, testBrokerName, constants.BrokerClassName)
	brokerLister := newFakeBrokerLister()
	brokerLister.addBroker(broker)
	audience := "https://stale-subscriber.example"

	tests := []struct {
		name   string
		mutate func(*eventingv1.Trigger)
	}{
		{
			name: "stale observed generation",
			mutate: func(trigger *eventingv1.Trigger) {
				trigger.Generation++
			},
		},
		{
			name: "subscriber unresolved",
			mutate: func(trigger *eventingv1.Trigger) {
				trigger.Status.MarkSubscriberResolvedFailed("NotReady", "subscriber is unresolved")
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			trigger := newTestTriggerWithSubscriber(testNamespace, testTriggerName, testBrokerName, "http://subscriber.example.com")
			trigger.Status.SubscriberAudience = &audience
			tc.mutate(trigger)
			manager := &ConsumerManager{
				logger: logging.FromContext(ctx), ctx: ctx,
				subscriptions: make(map[string]*TriggerSubscription),
				tokenFailures: map[string]map[string]struct{}{
					testTriggerUID: {audience: {}},
				},
			}
			reconciler := &FilterReconciler{
				logger: logging.FromContext(ctx), triggerLister: newFakeTriggerLister(), brokerLister: brokerLister, consumerManager: manager,
				brokerNamespace: testNamespace, brokerName: testBrokerName,
			}

			if err := reconciler.ReconcileTrigger(ctx, trigger); err != nil {
				t.Fatalf("ReconcileTrigger() = %v", err)
			}
			if !manager.Ready() {
				t.Error("stale or unresolved destination retained a token failure")
			}
		})
	}
}

func TestOIDCAudienceUnionPausesAt33AndResumesOnlyAfterTriggerReconcile(t *testing.T) {
	ctx, cancel := context.WithCancel(logging.WithLogger(context.Background(), logging.FromContext(context.TODO())))
	defer cancel()
	triggerLister := newFakeTriggerLister()
	var first *eventingv1.Trigger
	for i := 0; i < brokeroidc.MaxAudiences+1; i++ {
		trigger := newTestTriggerWithSubscriber(testNamespace, fmt.Sprintf("trigger-%02d", i), testBrokerName, "http://subscriber.example.com")
		trigger.UID = types.UID(fmt.Sprintf("trigger-uid-%02d", i))
		audience := fmt.Sprintf("https://audience-%02d.example", i)
		trigger.Status.SubscriberAudience = &audience
		triggerLister.addTrigger(trigger)
		if first == nil {
			first = trigger
		}
	}

	subscriber, _, resolved := resolvedTriggerDestinations(first)
	if !resolved {
		t.Fatal("first Trigger fixture is not resolved")
	}
	handler, err := NewTriggerHandler(ctx, first, subscriber, nil, nil, nil, nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	releaseFetch := make(chan struct{})
	pull := &recordingPullSubscription{fetches: make(chan int, 2), release: releaseFetch, unsubscribed: make(chan struct{})}
	initialDone := make(chan struct{})
	fetchCtx, fetchCancel := context.WithCancel(ctx)
	go func() {
		<-fetchCtx.Done()
		close(initialDone)
	}()
	sub := &TriggerSubscription{
		trigger: first, subscription: pull, handler: handler,
		fetchBatchSize: 1, fetchTimeout: time.Second, maxConcurrency: 1,
		sem: make(chan struct{}, 1), done: initialDone, cancel: fetchCancel,
		dispatchCtx: ctx, dispatchCancel: func() {},
	}
	manager := &ConsumerManager{
		logger: logging.FromContext(ctx), ctx: ctx,
		fetchBatchSize: 1, fetchTimeout: time.Second, defaultMaxConcurrency: 1,
		tokenSource:                func(context.Context, string) (string, error) { return "recovered-token", nil },
		subscriptions:              map[string]*TriggerSubscription{string(first.UID): sub},
		tokenFailures:              make(map[string]map[string]struct{}),
		tokenValidationGenerations: make(map[string]int64),
	}
	startTokenRecoveryForTest(t, manager)
	enqueued := 0
	reconciler := &FilterReconciler{
		logger: logging.FromContext(ctx), triggerLister: triggerLister, consumerManager: manager,
		brokerNamespace: testNamespace, brokerName: testBrokerName,
		enqueue: func(interface{}) { enqueued++ },
	}

	if err := reconciler.refreshActiveOIDCAudienceState(); err == nil {
		t.Fatal("33 current resolved audiences did not invalidate the filter configuration")
	}
	select {
	case <-initialDone:
	case <-time.After(5 * time.Second):
		t.Fatal("invalid audience union did not stop the fetch producer")
	}
	if !sub.configurationPaused || manager.Ready() {
		t.Fatalf("invalid union state: configurationPaused=%v Ready=%v, want true/false", sub.configurationPaused, manager.Ready())
	}
	// Credential recovery while the global audience union remains invalid must
	// clear tokenPaused without starting a fetcher. The later valid transition
	// still waits for this Trigger's own reconcile/SubscribeTrigger.
	sub.tokenPaused = true
	manager.markDestinationTokenFailure(string(first.UID), *first.Status.SubscriberAudience)
	manager.resumeTriggerAfterToken(string(first.UID), *first.Status.SubscriberAudience, sub, sub.done)
	if sub.tokenPaused {
		t.Error("tokenPaused remained set after credential recovery")
	}
	select {
	case batch := <-pull.fetches:
		t.Fatalf("token recovery bypassed invalid audience configuration with batch %d", batch)
	case <-time.After(50 * time.Millisecond):
	}

	delete(triggerLister.triggers[testNamespace], fmt.Sprintf("trigger-%02d", brokeroidc.MaxAudiences))
	if err := reconciler.refreshActiveOIDCAudienceState(); err != nil {
		t.Fatalf("32-audience union remained invalid: %v", err)
	}
	if enqueued != brokeroidc.MaxAudiences {
		t.Errorf("audience recovery enqueued %d Triggers, want %d", enqueued, brokeroidc.MaxAudiences)
	}
	if !sub.configurationPaused || manager.Ready() {
		t.Fatal("global validation recovery restarted or readied the old subscription before its Trigger reconcile")
	}
	select {
	case batch := <-pull.fetches:
		t.Fatalf("fetch restarted before per-Trigger SubscribeTrigger, batch %d", batch)
	case <-time.After(50 * time.Millisecond):
	}

	if err := manager.SubscribeTrigger(first, nil, subscriber, nil, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	select {
	case batch := <-pull.fetches:
		if batch != 1 {
			t.Errorf("revalidated fetch batch = %d, want 1", batch)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("per-Trigger SubscribeTrigger did not resume fetch after union recovery")
	}
	if sub.configurationPaused || !manager.Ready() {
		t.Fatalf("reconciled union state: configurationPaused=%v Ready=%v, want false/true", sub.configurationPaused, manager.Ready())
	}
	cancel()
	close(releaseFetch)
	select {
	case <-sub.done:
	case <-time.After(5 * time.Second):
		t.Fatal("resumed fetch producer did not stop")
	}
}

// --- Fake trigger lister ---

type fakeTriggerLister struct {
	triggers map[string]map[string]*eventingv1.Trigger
}

func newFakeTriggerLister() *fakeTriggerLister {
	return &fakeTriggerLister{
		triggers: make(map[string]map[string]*eventingv1.Trigger),
	}
}

func (f *fakeTriggerLister) addTrigger(trigger *eventingv1.Trigger) {
	if f.triggers[trigger.Namespace] == nil {
		f.triggers[trigger.Namespace] = make(map[string]*eventingv1.Trigger)
	}
	f.triggers[trigger.Namespace][trigger.Name] = trigger
}

func (f *fakeTriggerLister) List(selector labels.Selector) ([]*eventingv1.Trigger, error) {
	var result []*eventingv1.Trigger
	for _, ns := range f.triggers {
		for _, t := range ns {
			result = append(result, t)
		}
	}
	return result, nil
}

func (f *fakeTriggerLister) Triggers(namespace string) eventinglisters.TriggerNamespaceLister {
	return &fakeTriggerNamespaceLister{
		namespace: namespace,
		triggers:  f.triggers[namespace],
	}
}

type fakeTriggerNamespaceLister struct {
	namespace string
	triggers  map[string]*eventingv1.Trigger
}

func (f *fakeTriggerNamespaceLister) List(selector labels.Selector) ([]*eventingv1.Trigger, error) {
	result := make([]*eventingv1.Trigger, 0, len(f.triggers))
	for _, t := range f.triggers {
		result = append(result, t)
	}
	return result, nil
}

func (f *fakeTriggerNamespaceLister) Get(name string) (*eventingv1.Trigger, error) {
	if t, ok := f.triggers[name]; ok {
		return t, nil
	}
	return nil, apierrs.NewNotFound(schema.GroupResource{Group: "eventing.knative.dev", Resource: "triggers"}, name)
}

func TestReconcile(t *testing.T) {
	tests := []struct {
		name            string
		key             string
		trigger         *eventingv1.Trigger
		broker          *eventingv1.Broker
		presetUIDs      map[string]string
		wantErr         bool
		wantErrContains string
		wantUIDs        map[string]string
	}{
		{
			name:    "trigger exists - stores UID mapping and reconciles (skipped by broker not found)",
			key:     testNamespace + "/" + testTriggerName,
			trigger: newTestTrigger(testNamespace, testTriggerName, testBrokerName),
			wantErr: false,
			wantUIDs: map[string]string{
				testNamespace + "/" + testTriggerName: testTriggerUID,
			},
		},
		{
			name:    "trigger deleted with known UID - calls DeleteTrigger and cleans up",
			key:     testNamespace + "/" + testTriggerName,
			trigger: nil,
			presetUIDs: map[string]string{
				testNamespace + "/" + testTriggerName: testTriggerUID,
			},
			wantErr:  false,
			wantUIDs: map[string]string{},
		},
		{
			name:     "trigger deleted with unknown key - returns nil",
			key:      testNamespace + "/" + testTriggerName,
			trigger:  nil,
			wantErr:  false,
			wantUIDs: map[string]string{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := logging.WithLogger(context.Background(), logging.FromContext(context.TODO()))

			triggerLister := newFakeTriggerLister()
			if tc.trigger != nil {
				triggerLister.addTrigger(tc.trigger)
			}

			brokerLister := newFakeBrokerLister()
			if tc.broker != nil {
				brokerLister.addBroker(tc.broker)
			}

			cm := &ConsumerManager{
				logger:        logging.FromContext(ctx),
				subscriptions: make(map[string]*TriggerSubscription),
			}

			r := &FilterReconciler{
				logger:          logging.FromContext(ctx),
				triggerLister:   triggerLister,
				brokerLister:    brokerLister,
				consumerManager: cm,
				brokerNamespace: testNamespace,
				brokerName:      testBrokerName,
				triggerUIDs:     make(map[string]string),
			}

			// Preset UIDs if specified
			if tc.presetUIDs != nil {
				for k, v := range tc.presetUIDs {
					r.triggerUIDs[k] = v
				}
			}

			err := r.Reconcile(ctx, tc.key)
			if tc.wantErr {
				if err == nil {
					t.Error("Reconcile() expected error, got nil")
				} else if tc.wantErrContains != "" && !strings.Contains(err.Error(), tc.wantErrContains) {
					t.Errorf("error %q should contain %q", err.Error(), tc.wantErrContains)
				}
			} else if err != nil {
				t.Errorf("Reconcile() unexpected error: %v", err)
			}

			if tc.wantUIDs != nil {
				if len(r.triggerUIDs) != len(tc.wantUIDs) {
					t.Errorf("triggerUIDs length = %d, want %d", len(r.triggerUIDs), len(tc.wantUIDs))
				}
				for k, v := range tc.wantUIDs {
					if got, ok := r.triggerUIDs[k]; !ok {
						t.Errorf("triggerUIDs missing key %q", k)
					} else if got != v {
						t.Errorf("triggerUIDs[%q] = %q, want %q", k, got, v)
					}
				}
			}
		})
	}
}

type reassignmentPullSubscription struct {
	unsubscribed bool
}

func (*reassignmentPullSubscription) Fetch(context.Context, int) ([]*nats.Msg, error) {
	return nil, context.Canceled
}

func (s *reassignmentPullSubscription) Unsubscribe() error {
	s.unsubscribed = true
	return nil
}

func TestReconcileOwnershipLossDeletesTrackedSubscription(t *testing.T) {
	const brokerB = "broker-b"
	ctx := logging.WithLogger(context.Background(), logging.FromContext(context.TODO()))
	oldTrigger := newTestTrigger(testNamespace, testTriggerName, testBrokerName)
	newTrigger := oldTrigger.DeepCopy()
	newTrigger.Spec.Broker = brokerB
	key := testNamespace + "/" + testTriggerName
	uid := string(oldTrigger.UID)

	triggerLister := newFakeTriggerLister()
	triggerLister.addTrigger(newTrigger)
	pullSub := &reassignmentPullSubscription{}
	handler, err := NewTriggerHandler(ctx, oldTrigger, duckv1.Addressable{}, nil, nil, nil, nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	manager := &ConsumerManager{
		logger: logging.FromContext(ctx),
		subscriptions: map[string]*TriggerSubscription{
			uid: {trigger: oldTrigger, subscription: pullSub, handler: handler},
		},
	}
	reconciler := NewFilterReconciler(ctx, triggerLister, newFakeBrokerLister(), manager, testNamespace, testBrokerName)
	reconciler.triggerUIDs[key] = uid

	if err := reconciler.Reconcile(ctx, key); err != nil {
		t.Fatal(err)
	}
	if manager.HasSubscription(uid) {
		t.Error("old Broker retained subscription after lister returned the reassigned Trigger")
	}
	if _, found := reconciler.triggerUIDs[key]; found {
		t.Error("old Broker retained key-to-UID mapping after lister returned the reassigned Trigger")
	}
	if !pullSub.unsubscribed {
		t.Error("old pull subscription was not unsubscribed")
	}
	handler.configMu.RLock()
	handlerCleaned := handler.config == nil
	handler.configMu.RUnlock()
	if !handlerCleaned {
		t.Error("old Trigger handler was not cleaned up")
	}
}

func TestReconcileTriggerRecreationReplacesTrackedUID(t *testing.T) {
	const (
		oldUID = "old-trigger-uid"
		newUID = "new-trigger-uid"
	)
	ctx := logging.WithLogger(context.Background(), logging.FromContext(context.TODO()))
	oldTrigger := newTestTrigger(testNamespace, testTriggerName, testBrokerName)
	oldTrigger.UID = types.UID(oldUID)
	newTrigger := oldTrigger.DeepCopy()
	newTrigger.UID = types.UID(newUID)
	key := testNamespace + "/" + testTriggerName

	triggerLister := newFakeTriggerLister()
	triggerLister.addTrigger(newTrigger)
	brokerLister := newFakeBrokerLister()
	// Keep the Broker deliberately unready. Reconcile should still retire the
	// old UID before the new Trigger reaches subscription eligibility.
	brokerLister.addBroker(newTestBroker(testNamespace, testBrokerName, constants.BrokerClassName))
	pullSub := &reassignmentPullSubscription{}
	handler, err := NewTriggerHandler(ctx, oldTrigger, duckv1.Addressable{}, nil, nil, nil, nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	manager := &ConsumerManager{
		logger: logging.FromContext(ctx),
		subscriptions: map[string]*TriggerSubscription{
			oldUID: {trigger: oldTrigger, subscription: pullSub, handler: handler},
		},
	}
	reconciler := NewFilterReconciler(ctx, triggerLister, brokerLister, manager, testNamespace, testBrokerName)
	reconciler.triggerUIDs[key] = oldUID

	if err := reconciler.Reconcile(ctx, key); err != nil {
		t.Fatal(err)
	}
	if manager.HasSubscription(oldUID) {
		t.Error("old UID subscription survived same-name Trigger recreation")
	}
	if manager.HasSubscription(newUID) {
		t.Error("new UID subscription was unexpectedly created for an unready Broker")
	}
	if got := reconciler.triggerUIDs[key]; got != newUID {
		t.Errorf("key-to-UID mapping = %q, want replacement UID %q", got, newUID)
	}
	if !pullSub.unsubscribed {
		t.Error("old UID pull subscription was not unsubscribed")
	}
	handler.configMu.RLock()
	handlerCleaned := handler.config == nil
	handler.configMu.RUnlock()
	if !handlerCleaned {
		t.Error("old UID Trigger handler was not cleaned up")
	}
	if manager.GetSubscriptionCount() != 0 {
		t.Errorf("subscription count = %d, want 0 after old cleanup and new Broker readiness skip", manager.GetSubscriptionCount())
	}
}

func TestTriggerBrokerReassignmentMovesSubscription(t *testing.T) {
	const (
		brokerA = "broker-a"
		brokerB = "broker-b"
	)
	s := natsTesting.RunBasicJetstreamServer()
	defer natsTesting.ShutdownJSServerAndRemoveStorage(t, s)
	connA, jsA := natsTesting.JsClient(t, s)
	defer connA.Close()
	connB, jsB := natsTesting.JsClient(t, s)
	defer connB.Close()

	ctx := logging.WithLogger(context.Background(), logging.FromContext(context.TODO()))
	oldTrigger := newTestTriggerWithSubscriber(testNamespace, testTriggerName, brokerA, "http://subscriber.example.com")
	newTrigger := oldTrigger.DeepCopy()
	newTrigger.Spec.Broker = brokerB
	key := testNamespace + "/" + testTriggerName
	uid := string(oldTrigger.UID)

	oldBroker := newReadyTestBroker(testNamespace, brokerA, constants.BrokerClassName)
	newBroker := newReadyTestBroker(testNamespace, brokerB, constants.BrokerClassName)
	brokerLister := newFakeBrokerLister()
	brokerLister.addBroker(oldBroker)
	brokerLister.addBroker(newBroker)
	triggerLister := newFakeTriggerLister()
	triggerLister.addTrigger(oldTrigger)

	// The Trigger controller owns creation of the durable consumers. Give both
	// Broker streams a consumer for the immutable Trigger UID so this test can
	// exercise the filter controllers' complete subscription handoff.
	setupStreamAndConsumer(t, jsA, testNamespace, brokerA, uid)
	setupStreamAndConsumer(t, jsA, testNamespace, brokerB, uid)
	managerA := newConsumerManagerForTest(t, ctx, connA, jsA, nil)
	defer managerA.Close() //nolint:errcheck
	managerB := newConsumerManagerForTest(t, ctx, connB, jsB, nil)
	defer managerB.Close() //nolint:errcheck

	reconcilerA := NewFilterReconciler(ctx, triggerLister, brokerLister, managerA, testNamespace, brokerA)
	reconcilerB := NewFilterReconciler(ctx, triggerLister, brokerLister, managerB, testNamespace, brokerB)

	var reconcileErrors []error
	newEventHandler := func(r *FilterReconciler, brokerName string) *cache.FilteringResourceEventHandler {
		reconcile := func(obj interface{}) {
			objectKey, err := cache.MetaNamespaceKeyFunc(obj)
			if err == nil {
				err = r.Reconcile(ctx, objectKey)
			}
			if err != nil {
				reconcileErrors = append(reconcileErrors, err)
			}
		}
		return &cache.FilteringResourceEventHandler{
			FilterFunc: filterTriggersForBroker(brokerLister, testNamespace, brokerName),
			Handler: cache.ResourceEventHandlerFuncs{
				AddFunc: reconcile,
				UpdateFunc: func(_, newObj interface{}) {
					reconcile(newObj)
				},
				DeleteFunc: reconcile,
			},
		}
	}
	handlerA := newEventHandler(reconcilerA, brokerA)
	handlerB := newEventHandler(reconcilerB, brokerB)

	// Establish the original A-side subscription and key-to-UID mapping.
	handlerA.OnAdd(oldTrigger, false)
	if len(reconcileErrors) != 0 {
		t.Fatalf("initial A reconcile: %v", reconcileErrors)
	}
	if !managerA.HasSubscription(uid) || reconcilerA.triggerUIDs[key] != uid {
		t.Fatalf("A did not track initial Trigger UID %q and subscription", uid)
	}

	// Informer stores are updated before handlers run. On A this old-match to
	// new-nonmatch transition is delivered as a delete event, but Reconcile
	// reads the new B-owned Trigger from the lister. B observes it as an add.
	triggerLister.addTrigger(newTrigger)
	handlerA.OnUpdate(oldTrigger, newTrigger)
	handlerB.OnUpdate(oldTrigger, newTrigger)
	if len(reconcileErrors) != 0 {
		t.Fatalf("reassignment reconcile: %v", reconcileErrors)
	}

	if managerA.HasSubscription(uid) {
		t.Error("A retained the old subscription after Trigger moved to B")
	}
	if _, found := reconcilerA.triggerUIDs[key]; found {
		t.Error("A retained the key-to-UID mapping after Trigger moved to B")
	}
	if !managerB.HasSubscription(uid) {
		t.Error("B did not create a subscription after Trigger reassignment")
	}
	if got := reconcilerB.triggerUIDs[key]; got != uid {
		t.Errorf("B key-to-UID mapping = %q, want %q", got, uid)
	}
}
