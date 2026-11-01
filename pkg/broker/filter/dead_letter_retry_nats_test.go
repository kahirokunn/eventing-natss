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
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	brokerutils "knative.dev/eventing-natss/pkg/broker/utils"
	natsTesting "knative.dev/eventing-natss/pkg/channel/jetstream/dispatcher/testing"
	jsutils "knative.dev/eventing-natss/pkg/channel/jetstream/utils"
	deliveryv1 "knative.dev/eventing/pkg/apis/duck/v1"
	"knative.dev/eventing/pkg/kncloudevents"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
)

// TestDeadLetterRetries exercises independent subscriber and DLS budgets and
// makes the DLS backoff outlive AckWait to verify progress notifications.
func TestDeadLetterRetries(t *testing.T) {
	for _, tc := range []struct {
		name                string
		subscriberStatus    int
		deadLetterStatuses  []int
		retries             int
		unsetRetry          bool
		cancelDLS           bool
		timeoutFirst        bool
		wantSubscriberCalls int64
		wantDeadLetterCalls int64
	}{
		{name: "non-retriable subscriber, DLS recovers", subscriberStatus: 400, deadLetterStatuses: []int{503, 202}, retries: 2, wantSubscriberCalls: 1, wantDeadLetterCalls: 2},
		{name: "subscriber exhausted, DLS recovers", subscriberStatus: 503, deadLetterStatuses: []int{503, 503, 202}, retries: 2, wantSubscriberCalls: 3, wantDeadLetterCalls: 3},
		{name: "DLS retries exhausted", subscriberStatus: 400, deadLetterStatuses: []int{503}, retries: 2, wantSubscriberCalls: 1, wantDeadLetterCalls: 3},
		{name: "non-retriable DLS response", subscriberStatus: 400, deadLetterStatuses: []int{400}, retries: 2, wantSubscriberCalls: 1, wantDeadLetterCalls: 1},
		{name: "DLS request timeout recovers", subscriberStatus: 400, deadLetterStatuses: []int{503, 202}, retries: 2, timeoutFirst: true, wantSubscriberCalls: 1, wantDeadLetterCalls: 2},
		{name: "zero retries", subscriberStatus: 400, deadLetterStatuses: []int{503}, retries: 0, wantSubscriberCalls: 1, wantDeadLetterCalls: 1},
		{name: "unset retries", subscriberStatus: 400, deadLetterStatuses: []int{503}, unsetRetry: true, wantSubscriberCalls: 1, wantDeadLetterCalls: 1},
		{name: "consumer canceled during DLS", subscriberStatus: 400, deadLetterStatuses: []int{503}, retries: 2, cancelDLS: true, wantSubscriberCalls: 1, wantDeadLetterCalls: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := natsTesting.RunBasicJetstreamServer()
			defer natsTesting.ShutdownJSServerAndRemoveStorage(t, s)
			conn, js := natsTesting.JsClient(t, s)
			defer conn.Close()
			const namespace, brokerName, uid = "default", "retry-broker", "retry-trigger"
			stream, consumer := setupStreamAndConsumer(t, js, namespace, brokerName, uid)
			info, err := js.ConsumerInfo(stream, consumer)
			if err != nil {
				t.Fatal(err)
			}
			cfg := info.Config
			cfg.MaxDeliver = tc.retries + 1
			if _, err := js.UpdateConsumer(stream, &cfg); err != nil {
				t.Fatal(err)
			}

			var subscriberCalls, deadLetterCalls atomic.Int64
			subscriber := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				subscriberCalls.Add(1)
				w.WriteHeader(tc.subscriberStatus)
			}))
			defer subscriber.Close()
			started := make(chan struct{}, 1)
			dls := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				call := int(deadLetterCalls.Add(1))
				if subscriberCalls.Load() != tc.wantSubscriberCalls {
					t.Errorf("subscriber called again during DLS: %d", subscriberCalls.Load())
				}
				info, err := js.ConsumerInfo(stream, consumer)
				if err != nil || info.NumAckPending != 1 {
					t.Errorf("pending during DLS: %+v, %v", info, err)
				}
				if tc.timeoutFirst && call == 1 {
					<-r.Context().Done()
					return
				}
				if tc.cancelDLS {
					started <- struct{}{}
					<-r.Context().Done()
					return
				}
				w.WriteHeader(tc.deadLetterStatuses[min(call, len(tc.deadLetterStatuses))-1])
			}))
			defer dls.Close()
			cm := newConsumerManagerForTest(t, conn, js, &ConsumerManagerConfig{FetchBatchSize: 1, FetchTimeout: 20 * time.Millisecond, MaxConcurrency: 1})
			defer cm.Close() //nolint:errcheck
			// Accelerate the DLS shutdown grace without bypassing the initial
			// consumer AckWait compatibility check.
			lifecycle, cancel := context.WithCancel(cm.ctx)
			defer cancel()
			cm.ctx = jsutils.WithDeliveryContext(lifecycle, 100*time.Millisecond)
			subscriberURL, _ := apis.ParseURL(subscriber.URL)
			dlsURL, _ := apis.ParseURL(dls.URL)
			var retryConfig *kncloudevents.RetryConfig
			if !tc.unsetRetry {
				retries := int32(tc.retries)
				delay, policy := "PT1S", deliveryv1.BackoffPolicyLinear
				config, err := kncloudevents.RetryConfigFromDeliverySpec(deliveryv1.DeliverySpec{Retry: &retries, BackoffDelay: &delay, BackoffPolicy: &policy})
				if err != nil {
					t.Fatal(err)
				}
				// Use short durations so DLS retries outlive the test AckWait.
				config.RequestTimeout = 500 * time.Millisecond
				if tc.timeoutFirst {
					config.RequestTimeout = 30 * time.Millisecond
				}
				config.Backoff = func(attempt int, _ *http.Response) time.Duration {
					return time.Duration(attempt+1) * 50 * time.Millisecond
				}
				retryConfig = &config
			}
			noRetry := kncloudevents.NoRetries()
			noRetry.RequestTimeout = 500 * time.Millisecond
			if err := cm.SubscribeTrigger(makeTriggerWithUID(namespace, "retry", brokerName, uid), makeTestBrokerForNats(namespace, brokerName), duckv1.Addressable{URL: subscriberURL}, nil, &duckv1.Addressable{URL: dlsURL}, retryConfig, &noRetry); err != nil {
				t.Fatal(err)
			}
			// Shorten AckWait only after the valid subscription is active.
			updateAckWaitForTest(t, js, stream, consumer, 100*time.Millisecond)
			publishStructuredCE(t, js, brokerutils.BrokerPublishSubjectName(namespace, brokerName), "retry-event")
			if tc.cancelDLS {
				select {
				case <-started:
				case <-time.After(5 * time.Second):
					t.Fatal("DLS never started")
				}
				cancel()
				if err := cm.Close(); err != nil {
					t.Fatal(err)
				}
				info, err := js.ConsumerInfo(stream, consumer)
				if err != nil || info.NumAckPending != 1 {
					t.Fatalf("canceled DLS was settled: %+v, %v", info, err)
				}
			} else {
				require.Eventually(t, func() bool {
					info, err := js.ConsumerInfo(stream, consumer)
					return err == nil && info.NumAckPending == 0 && info.NumPending == 0
				}, 5*time.Second, 10*time.Millisecond, "message not settled")
			}
			if got := subscriberCalls.Load(); got != tc.wantSubscriberCalls {
				t.Errorf("subscriber calls=%d, want %d", got, tc.wantSubscriberCalls)
			}
			if got := deadLetterCalls.Load(); got != tc.wantDeadLetterCalls {
				t.Errorf("DLS calls=%d, want %d", got, tc.wantDeadLetterCalls)
			}
		})
	}
}
