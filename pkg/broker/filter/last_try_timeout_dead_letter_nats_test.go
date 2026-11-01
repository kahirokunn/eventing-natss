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
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"

	brokerutils "knative.dev/eventing-natss/pkg/broker/utils"
	natsTesting "knative.dev/eventing-natss/pkg/channel/jetstream/dispatcher/testing"
)

// TestLastTryTimeoutSendsToDeadLetterSink verifies that an event whose last
// delivery times out at AckWait is sent to the dead letter sink. JetStream
// stops delivering a message once MaxDeliver is reached, so without this the
// event would be dropped without reaching the dead letter sink.
func TestLastTryTimeoutSendsToDeadLetterSink(t *testing.T) {
	s := natsTesting.RunBasicJetstreamServer()
	defer natsTesting.ShutdownJSServerAndRemoveStorage(t, s)
	conn, js := natsTesting.JsClient(t, s)
	defer conn.Close()

	const namespace, brokerName, triggerUID = "default", "last-try-timeout-broker", "last-try-timeout-trigger-uid"
	broker := makeTestBrokerForNats(namespace, brokerName)
	// MaxDeliver 1 matches the nil retry config below (RetryMax 0), so the
	// first delivery is the last try.
	setupStreamAndConsumer(t, js, namespace, brokerName, triggerUID, func(c *nats.ConsumerConfig) {
		c.AckWait = 500 * time.Millisecond
		c.MaxDeliver = 1
	})

	var subscriberCalls atomic.Int64
	subscriberServer := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		subscriberCalls.Add(1)
		// Never answer; the filter cancels the request at AckWait.
		<-r.Context().Done()
	}))
	defer subscriberServer.Close()

	deadLettered := make(chan struct{}, 1)
	deadLetterServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		select {
		case deadLettered <- struct{}{}:
		default:
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer deadLetterServer.Close()

	cm := newConsumerManagerForTest(t, conn, js, &ConsumerManagerConfig{
		FetchBatchSize: 1,
		FetchTimeout:   100 * time.Millisecond,
		MaxConcurrency: 1,
	})
	defer cm.Close() //nolint:errcheck

	trigger := makeTriggerWithUID(namespace, "last-try-timeout-trigger", brokerName, triggerUID)
	subscriberURL, _ := apis.ParseURL(subscriberServer.URL)
	deadLetterURL, _ := apis.ParseURL(deadLetterServer.URL)

	if err := cm.SubscribeTrigger(
		trigger,
		broker,
		duckv1.Addressable{URL: subscriberURL},
		nil,
		&duckv1.Addressable{URL: deadLetterURL},
		nil,
		nil,
	); err != nil {
		t.Fatalf("SubscribeTrigger: %v", err)
	}

	publishStructuredCE(t, js, brokerutils.BrokerPublishSubjectName(namespace, brokerName), "last-try-timeout-event")

	receiveWithin(t, deadLettered, "event was not sent to the dead letter sink")

	if got := subscriberCalls.Load(); got != 1 {
		t.Errorf("subscriber calls = %d, want 1", got)
	}
}
