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
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"

	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	"knative.dev/pkg/controller"

	"knative.dev/eventing-natss/pkg/broker/constants"
	brokerutils "knative.dev/eventing-natss/pkg/broker/utils"
	natsTesting "knative.dev/eventing-natss/pkg/channel/jetstream/dispatcher/testing"
	eventingduckv1 "knative.dev/eventing/pkg/apis/duck/v1"
	"knative.dev/eventing/pkg/kncloudevents"
)

func updateAckWaitForTest(t *testing.T, js nats.JetStreamContext, stream, consumer string, ackWait time.Duration) {
	t.Helper()
	info, err := js.ConsumerInfo(stream, consumer)
	require.NoError(t, err)
	info.Config.AckWait = ackWait
	_, err = js.UpdateConsumer(stream, &info.Config)
	require.NoError(t, err)
}

func TestConsumerAckWait_WaitsForControllerBeforeFetching(t *testing.T) {
	s := natsTesting.RunBasicJetstreamServer()
	t.Cleanup(func() { natsTesting.ShutdownJSServerAndRemoveStorage(t, s) })
	conn, js := natsTesting.JsClient(t, s)
	t.Cleanup(conn.Close)
	trigger := newTestTriggerWithSubscriber("default", "waiting-trigger", "waiting-broker", "http://subscriber.example.com")
	stream, consumer := setupStreamAndConsumer(t, js, trigger.Namespace, trigger.Spec.Broker, string(trigger.UID))
	updateAckWaitForTest(t, js, stream, consumer, 30*time.Second)
	before, err := js.ConsumerInfo(stream, consumer)
	require.NoError(t, err)

	received := make(chan string, 10)
	subscriber := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received <- r.Header.Get("Ce-Id")
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(subscriber.Close)
	trigger.Status.SubscriberURI, err = apis.ParseURL(subscriber.URL)
	require.NoError(t, err)
	cm := newConsumerManagerForTest(t, conn, js, nil)
	t.Cleanup(func() { require.NoError(t, cm.Close()) })
	brokers := newFakeBrokerLister()
	brokers.addBroker(newReadyTestBroker(trigger.Namespace, trigger.Spec.Broker, constants.BrokerClassName))
	triggers := newFakeTriggerLister()
	triggers.addTrigger(trigger)
	r := NewFilterReconciler(logCtx(), triggers, brokers, cm)
	publishStructuredCE(t, js, brokerutils.BrokerPublishSubjectName(trigger.Namespace, trigger.Spec.Broker), "waiting-event")

	err = r.Reconcile(logCtx(), trigger.Namespace+"/"+trigger.Name)
	requeue, delay := controller.IsRequeueKey(err)
	require.True(t, requeue, "an AckWait mismatch must be retried without a new Trigger event")
	require.Equal(t, time.Second, delay)
	require.False(t, cm.HasSubscription(string(trigger.UID)))
	info, err := js.ConsumerInfo(stream, consumer)
	require.NoError(t, err)
	require.Equal(t, before.Config, info.Config, "the filter must not change consumer settings")
	require.Zero(t, info.NumAckPending)
	require.EqualValues(t, 1, info.NumPending)
	require.Empty(t, received)

	// A waiting Trigger must not hold the manager's lock or stop another one.
	setupStreamAndConsumer(t, js, "default", "ready-broker", "ready-trigger")
	require.NoError(t, cm.SubscribeTrigger(
		makeTriggerWithUID("default", "ready-trigger", "ready-broker", "ready-trigger"),
		makeTestBrokerForNats("default", "ready-broker"),
		duckv1.Addressable{URL: trigger.Status.SubscriberURI}, nil, nil, nil, nil))
	publishStructuredCE(t, js, brokerutils.BrokerPublishSubjectName("default", "ready-broker"), "ready-event")
	require.Equal(t, "ready-event", receiveWithin(t, received, "waiting Trigger blocked another Trigger"))

	updateAckWaitForTest(t, js, stream, consumer, brokerutils.ConsumerAckWait(0))
	require.NoError(t, r.Reconcile(logCtx(), trigger.Namespace+"/"+trigger.Name))
	require.Equal(t, "waiting-event", receiveWithin(t, received, "stored event was not delivered after AckWait changed"))
	waitTimeoutMessageSettled(t, js, stream, consumer)
	require.Empty(t, received)
}

func TestConsumerAckWait_ActivatesBrokerTimeoutAndTriggerOverride(t *testing.T) {
	for _, consumerFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("consumer first=%t", consumerFirst), func(t *testing.T) {
			oldRequests := make(chan struct{}, 10)
			oldSubscriber := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				oldRequests <- struct{}{}
				w.WriteHeader(http.StatusNoContent)
			}))
			t.Cleanup(oldSubscriber.Close)
			completed := make(chan error, 10)
			newSubscriber := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				select {
				case <-r.Context().Done():
					completed <- r.Context().Err()
				case <-time.After(300 * time.Millisecond):
					w.WriteHeader(http.StatusNoContent)
					completed <- nil
				}
			}))
			t.Cleanup(newSubscriber.Close)
			cm, js, stream, consumer := subscribeTimeoutTest(t, oldSubscriber.URL, "", "",
				&kncloudevents.RetryConfig{}, 200*time.Millisecond, brokerutils.ConsumerAckWait(200*time.Millisecond))
			trigger := newTestTriggerWithSubscriber("default", "timeout-trigger", "timeout-broker", newSubscriber.URL)
			trigger.UID = "timeout-trigger"
			broker := newReadyTestBroker("default", "timeout-broker", constants.BrokerClassName)
			timeout := "PT2S"
			broker.Spec.Delivery = &eventingduckv1.DeliverySpec{Timeout: &timeout}
			brokers := newFakeBrokerLister()
			brokers.addBroker(broker)
			r := NewFilterReconciler(logCtx(), nil, brokers, cm)
			handler := cm.subscriptions["timeout-trigger"].handler
			previous := snapshotHandlerConfig(t, handler)

			// A failed management query must preserve the active handler too.
			cm.mu.Lock()
			realJS := cm.js
			cm.js = &fakeJetStream{consumerInfoErr: fmt.Errorf("temporary management failure")}
			cm.mu.Unlock()
			err := r.ReconcileTrigger(logCtx(), trigger)
			cm.mu.Lock()
			cm.js = realJS
			cm.mu.Unlock()
			require.ErrorContains(t, err, "temporary management failure")
			require.Equal(t, previous, snapshotHandlerConfig(t, handler))

			if consumerFirst {
				updateAckWaitForTest(t, js, stream, consumer, brokerutils.ConsumerAckWait(2*time.Second))
			} else {
				err = r.ReconcileTrigger(logCtx(), trigger)
				requeue, _ := controller.IsRequeueKey(err)
				require.True(t, requeue)
				require.Equal(t, previous, snapshotHandlerConfig(t, handler))
				publishStructuredCE(t, js, brokerutils.BrokerPublishSubjectName("default", "timeout-broker"), "before-update")
				receiveWithin(t, oldRequests, "old settings did not remain active while waiting")
				waitTimeoutMessageSettled(t, js, stream, consumer)
				updateAckWaitForTest(t, js, stream, consumer, brokerutils.ConsumerAckWait(2*time.Second))
			}
			require.NoError(t, r.ReconcileTrigger(logCtx(), trigger))
			require.Same(t, handler, cm.subscriptions["timeout-trigger"].handler)
			publishStructuredCE(t, js, brokerutils.BrokerPublishSubjectName("default", "timeout-broker"), "broker-timeout")
			require.NoError(t, receiveWithin(t, completed, "new Broker timeout was not activated"))

			// A Trigger delivery overrides the Broker as a whole. The 300ms
			// request must now time out at 200ms instead of inheriting two seconds.
			triggerTimeout := "PT0.2S"
			trigger.Spec.Delivery = &eventingduckv1.DeliverySpec{Timeout: &triggerTimeout}
			require.NoError(t, r.ReconcileTrigger(logCtx(), trigger))
			publishStructuredCE(t, js, brokerutils.BrokerPublishSubjectName("default", "timeout-broker"), "trigger-override")
			require.ErrorIs(t, receiveWithin(t, completed, "Trigger override did not time out"), context.Canceled)
		})
	}
}

func TestConsumerAckWait_ShorterTimeoutPreservesInflightRequest(t *testing.T) {
	for _, consumerFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("consumer first=%t", consumerFirst), func(t *testing.T) {
			started := make(chan time.Time, 10)
			oldCompleted := make(chan error, 10)
			newCompleted := make(chan error, 10)
			release := make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			subscriber := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				if r.Header.Get("Ce-Id") == "old-request" {
					started <- time.Now()
					select {
					case <-r.Context().Done():
						oldCompleted <- r.Context().Err()
					case <-release:
						w.WriteHeader(http.StatusNoContent)
						oldCompleted <- nil
					}
					return
				}
				<-r.Context().Done()
				newCompleted <- r.Context().Err()
			}))
			t.Cleanup(subscriber.Close)
			cm, js, stream, consumer := subscribeTimeoutTest(t, subscriber.URL, "", "",
				&kncloudevents.RetryConfig{}, 20*time.Second, brokerutils.ConsumerAckWait(20*time.Second))
			t.Cleanup(unblock)
			// Leave room for redelivery so a missing progress update is visible
			// as a second subscriber call rather than hidden by MaxDeliver.
			info, err := js.ConsumerInfo(stream, consumer)
			require.NoError(t, err)
			info.Config.MaxDeliver = 3
			_, err = js.UpdateConsumer(stream, &info.Config)
			require.NoError(t, err)
			publishStructuredCE(t, js, brokerutils.BrokerPublishSubjectName("default", "timeout-broker"), "old-request")
			start := receiveWithin(t, started, "old request did not start")
			trigger := newTestTriggerWithSubscriber("default", "timeout-trigger", "timeout-broker", subscriber.URL)
			trigger.UID = "timeout-trigger"
			timeout := "PT1S"
			trigger.Spec.Delivery = &eventingduckv1.DeliverySpec{Timeout: &timeout}
			brokers := newFakeBrokerLister()
			brokers.addBroker(newReadyTestBroker("default", "timeout-broker", constants.BrokerClassName))
			r := NewFilterReconciler(logCtx(), nil, brokers, cm)
			newAckWait := brokerutils.ConsumerAckWait(time.Second)
			if consumerFirst {
				updateAckWaitForTest(t, js, stream, consumer, newAckWait)
			}
			require.NoError(t, r.ReconcileTrigger(logCtx(), trigger))
			if !consumerFirst {
				updateAckWaitForTest(t, js, stream, consumer, newAckWait)
			}
			publishStructuredCE(t, js, brokerutils.BrokerPublishSubjectName("default", "timeout-broker"), "new-request")
			require.ErrorIs(t, receiveWithin(t, newCompleted, "new timeout did not take effect"), context.Canceled)

			// Cross the new server-side deadline while the old HTTP request is
			// still running. Without progress, it is delivered a second time.
			select {
			case err := <-oldCompleted:
				t.Fatalf("old request ended before release: %v", err)
			case <-time.After(time.Until(start.Add(newAckWait + 500*time.Millisecond))):
			}
			require.Empty(t, started, "old event was redelivered while its request was running")
			unblock()
			require.NoError(t, receiveWithin(t, oldCompleted, "old request did not finish"))
			require.Eventually(t, func() bool {
				info, err := js.ConsumerInfo(stream, consumer)
				return err == nil && info.NumAckPending == 0 && info.AckFloor.Stream == 2
			}, 5*time.Second, 10*time.Millisecond)
		})
	}
}
