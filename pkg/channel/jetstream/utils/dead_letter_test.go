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

package utils_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	dispatchertesting "knative.dev/eventing-natss/pkg/channel/jetstream/dispatcher/testing"
	jsutils "knative.dev/eventing-natss/pkg/channel/jetstream/utils"
)

func pendingMessage(t *testing.T) (*nats.Msg, *nats.Subscription, *nats.Conn) {
	t.Helper()
	s := dispatchertesting.RunBasicJetstreamServer()
	t.Cleanup(func() { dispatchertesting.ShutdownJSServerAndRemoveStorage(t, s) })
	conn, js := dispatchertesting.JsClient(t, s)
	t.Cleanup(conn.Close)
	if _, err := js.AddStream(&nats.StreamConfig{Name: "DLS", Subjects: []string{"dls"}, Storage: nats.MemoryStorage}); err != nil {
		t.Fatal(err)
	}
	sub, err := js.PullSubscribe("dls", "consumer", nats.AckExplicit(), nats.AckWait(60*time.Millisecond), nats.MaxDeliver(2))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := js.Publish("dls", []byte("event")); err != nil {
		t.Fatal(err)
	}
	msgs, err := sub.Fetch(1, nats.MaxWait(time.Second))
	if err != nil || len(msgs) != 1 {
		t.Fatalf("Fetch: %v, %d messages", err, len(msgs))
	}
	return msgs[0], sub, conn
}

func TestDeadLetterContextKeepsMessagePendingPastSubscriberDeadline(t *testing.T) {
	msg, sub, _ := pendingMessage(t)
	lifecycle, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx := jsutils.WithDeliveryContext(lifecycle, 60*time.Millisecond)
	guard, cancelGuard := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancelGuard()
	// A second call must retain the consumer lifetime, not the short guard.
	guard = jsutils.WithDeliveryContext(guard, time.Second)
	dlsCtx, stop, err := jsutils.StartDeadLetter(guard, msg)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	<-guard.Done()
	if dlsCtx.Err() != nil {
		t.Fatalf("subscriber deadline canceled DLS: %v", dlsCtx.Err())
	}
	if msgs, err := sub.Fetch(1, nats.MaxWait(180*time.Millisecond)); !errors.Is(err, nats.ErrTimeout) {
		t.Fatalf("redelivered during DLS: %d messages, %v", len(msgs), err)
	}
	info, err := sub.ConsumerInfo()
	if err != nil || info.NumAckPending != 1 {
		t.Fatalf("pending during DLS: %+v, %v", info, err)
	}
	stop()
	msgs, err := sub.Fetch(1, nats.MaxWait(time.Second))
	if err != nil || len(msgs) != 1 {
		t.Fatalf("heartbeat survived stop: %d messages, %v", len(msgs), err)
	}
	if err := msgs[0].Term(); err != nil {
		t.Fatal(err)
	}
}

func TestDeadLetterContextStopsOnConsumerCancellation(t *testing.T) {
	msg, _, _ := pendingMessage(t)
	lifecycle, cancel := context.WithCancel(context.Background())
	dlsCtx, stop, err := jsutils.StartDeadLetter(jsutils.WithDeliveryContext(lifecycle, 60*time.Millisecond), msg)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	defer stop()
	cancel()
	select {
	case <-dlsCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("DLS ignored consumer cancellation")
	}
}

func TestDeadLetterContextStopsOnHeartbeatFailure(t *testing.T) {
	msg, _, conn := pendingMessage(t)
	dlsCtx, stop, err := jsutils.StartDeadLetter(jsutils.WithDeliveryContext(context.Background(), 60*time.Millisecond), msg)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	conn.Close()
	select {
	case <-dlsCtx.Done():
		if !errors.Is(context.Cause(dlsCtx), nats.ErrConnectionClosed) {
			t.Fatalf("cause = %v", context.Cause(dlsCtx))
		}
	case <-time.After(time.Second):
		t.Fatal("DLS ignored failed heartbeat")
	}
}

func TestDeadLetterContextAllowsGracefulCompletion(t *testing.T) {
	msg, sub, _ := pendingMessage(t)
	lifecycle, cancel := context.WithCancel(context.Background())
	dlsCtx, stop, err := jsutils.StartDeadLetter(jsutils.WithDeliveryContext(lifecycle, 60*time.Millisecond), msg)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	defer stop()
	cancel()
	select {
	case <-dlsCtx.Done():
		t.Fatalf("DLS was canceled without a shutdown grace period: %v", context.Cause(dlsCtx))
	case <-time.After(20 * time.Millisecond):
	}
	if err := msg.Ack(nats.Context(dlsCtx)); err != nil {
		t.Fatal(err)
	}
	info, err := sub.ConsumerInfo()
	if err != nil || info.NumAckPending != 0 {
		t.Fatalf("graceful DLS completion was not settled: %+v, %v", info, err)
	}
}
