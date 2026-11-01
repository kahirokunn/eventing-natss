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

package dispatcher

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"knative.dev/eventing-natss/pkg/apis/messaging/v1alpha1"
	dispatchertesting "knative.dev/eventing-natss/pkg/channel/jetstream/dispatcher/testing"
	"knative.dev/eventing/pkg/channel/fanout"
	"knative.dev/eventing/pkg/eventingtls"
	"knative.dev/eventing/pkg/kncloudevents"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	"knative.dev/pkg/logging"
)

// TestChannelDeadLetterRetries uses the same failure scenarios as the Broker
// tests, through both real consumer implementations rather than send alone.
func TestChannelDeadLetterRetries(t *testing.T) {
	for _, consumerType := range []v1alpha1.ConsumerType{v1alpha1.PullConsumerType, v1alpha1.PushConsumerType} {
		t.Run(string(consumerType), func(t *testing.T) {
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
					s := dispatchertesting.RunBasicJetstreamServer()
					defer dispatchertesting.ShutdownJSServerAndRemoveStorage(t, s)
					conn, js := dispatchertesting.JsClient(t, s)
					defer conn.Close()
					const stream, subject, consumerName = "PARITY", "events", "consumer"
					if _, err := js.AddStream(&nats.StreamConfig{Name: stream, Subjects: []string{subject}, Storage: nats.MemoryStorage}); err != nil {
						t.Fatal(err)
					}
					cfg := nats.ConsumerConfig{Durable: consumerName, Name: consumerName, AckPolicy: nats.AckExplicitPolicy, AckWait: 100 * time.Millisecond, MaxDeliver: tc.retries + 1}
					if consumerType == v1alpha1.PushConsumerType {
						cfg.DeliverSubject = "deliver"
						cfg.DeliverGroup = "workers"
					}
					if _, err := js.AddConsumer(stream, &cfg); err != nil {
						t.Fatal(err)
					}
					var subscriberCalls, deadLetterCalls atomic.Int64
					subscriber := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						subscriberCalls.Add(1)
						w.WriteHeader(tc.subscriberStatus)
						_, _ = w.Write([]byte("subscriber failure"))
					}))
					defer subscriber.Close()
					started := make(chan struct{}, 1)
					dls := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						call := int(deadLetterCalls.Add(1))
						if subscriberCalls.Load() != tc.wantSubscriberCalls {
							t.Errorf("subscriber called again during DLS: %d", subscriberCalls.Load())
						}
						info, err := js.ConsumerInfo(stream, consumerName)
						if err != nil || info.NumAckPending != 1 {
							t.Errorf("pending during DLS: %+v, %v", info, err)
						}
						if got := r.Header.Get("Ce-Knativeerrorcode"); got != strconv.Itoa(tc.subscriberStatus) {
							t.Errorf("original error code changed: %q", got)
						}
						if got := strings.TrimSuffix(r.Header.Get("Ce-Knativeerrordest"), "/"); got != subscriber.URL {
							t.Errorf("original error destination changed: %q", got)
						}
						if got := r.Header.Get("Ce-Knativeerrordata"); got != base64.StdEncoding.EncodeToString([]byte("subscriber failure")) {
							t.Errorf("original error body changed: %q", got)
						}
						data, err := io.ReadAll(r.Body)
						var original struct {
							Value int `json:"value"`
						}
						if err != nil || json.Unmarshal(data, &original) != nil || original.Value != 7 || r.Header.Get("Ce-Id") != "parity-event" {
							t.Errorf("original event changed: %q, %v", data, err)
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
					var retryConfig *kncloudevents.RetryConfig
					if !tc.unsetRetry {
						cfg := kncloudevents.NoRetries()
						cfg.RetryMax = tc.retries
						cfg.CheckRetry = kncloudevents.SelectiveRetry
						cfg.RequestTimeout = 500 * time.Millisecond
						if tc.timeoutFirst {
							cfg.RequestTimeout = 30 * time.Millisecond
						}
						cfg.Backoff = func(attempt int, _ *http.Response) time.Duration {
							return time.Duration(attempt+1) * 50 * time.Millisecond
						}
						retryConfig = &cfg
					}
					subscriberURL, _ := apis.ParseURL(subscriber.URL)
					dlsURL, _ := apis.ParseURL(dls.URL)
					subscription := Subscription{UID: "consumer", Subscription: fanout.Subscription{Subscriber: duckv1.Addressable{URL: subscriberURL}, DeadLetter: &duckv1.Addressable{URL: dlsURL}, RetryConfig: retryConfig}}
					channelConfig := ChannelConfig{StreamName: stream, ConsumerConfigTemplate: &v1alpha1.ConsumerConfigTemplate{ConsumerType: consumerType, FetchMaxWait: metav1.Duration{Duration: 20 * time.Millisecond}}}
					lifecycle, cancel := context.WithCancel(logging.WithLogger(context.Background(), zap.NewNop().Sugar()))
					defer cancel()
					dispatcher := kncloudevents.NewDispatcher(eventingtls.ClientConfig{}, nil)
					var stop func() error
					if consumerType == v1alpha1.PullConsumerType {
						sub, err := js.PullSubscribe(subject, consumerName, nats.Bind(stream, consumerName))
						if err != nil {
							t.Fatal(err)
						}
						defer sub.Unsubscribe() //nolint:errcheck
						consumer, err := NewPullConsumer(lifecycle, sub, subscription, dispatcher, &channelConfig)
						if err != nil {
							t.Fatal(err)
						}
						done := make(chan error, 1)
						go func() { done <- consumer.Start() }()
						require.Eventually(t, func() bool { consumer.lifecycleMu.Lock(); defer consumer.lifecycleMu.Unlock(); return consumer.started }, time.Second, time.Millisecond, "pull consumer did not start")
						stop = sync.OnceValue(func() error {
							err := consumer.Close()
							startErr := <-done
							if err != nil {
								return err
							}
							return startErr
						})
					} else {
						d := &Dispatcher{dispatcher: dispatcher, js: js, consumerNameFunc: func(string) string { return consumerName }, consumers: map[types.UID]Consumer{}}
						if _, err := d.subscribe(lifecycle, channelConfig, subscription, false); err != nil {
							t.Fatal(err)
						}
						stop = sync.OnceValue(d.consumers[subscription.UID].Close)
					}
					defer func() {
						if err := stop(); err != nil {
							t.Errorf("Close: %v", err)
						}
					}()
					event := &nats.Msg{Subject: subject, Header: nats.Header{"Content-Type": []string{"application/cloudevents+json"}}, Data: []byte(`{"specversion":"1.0","type":"test.type","source":"test/source","id":"parity-event","data":{"value":7}}`)}
					if _, err := js.PublishMsg(event); err != nil {
						t.Fatal(err)
					}
					if tc.cancelDLS {
						select {
						case <-started:
						case <-time.After(5 * time.Second):
							t.Fatal("DLS never started")
						}
						if err := stop(); err != nil {
							t.Fatal(err)
						}
						info, err := js.ConsumerInfo(stream, consumerName)
						if err != nil || info.NumAckPending != 1 {
							t.Fatalf("canceled DLS was settled: %+v, %v", info, err)
						}
					} else {
						require.Eventually(t, func() bool {
							info, err := js.ConsumerInfo(stream, consumerName)
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
		})
	}
}
