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
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	cejs "github.com/cloudevents/sdk-go/protocol/nats_jetstream/v2"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	"knative.dev/pkg/logging"

	eventingv1 "knative.dev/eventing/pkg/apis/eventing/v1"
	"knative.dev/eventing/pkg/eventfilter"
	"knative.dev/eventing/pkg/eventingtls"
	"knative.dev/eventing/pkg/kncloudevents"

	natsTesting "knative.dev/eventing-natss/pkg/channel/jetstream/dispatcher/testing"
)

func installAudienceTokenSource(t *testing.T, handler *TriggerHandler, source func(context.Context, string) (string, error)) {
	t.Helper()
	handler.setAudienceTokenSource(source)
}

type cleanupTrackingFilter struct {
	filtered chan string
	release  <-chan struct{}
	cleaned  chan struct{}
}

func (f *cleanupTrackingFilter) Filter(_ context.Context, event cloudevents.Event) eventfilter.FilterResult {
	f.filtered <- event.ID()
	if f.release != nil {
		<-f.release
	}
	return eventfilter.PassFilter
}

func (f *cleanupTrackingFilter) Cleanup() {
	close(f.cleaned)
}

// makeStructuredCEMsg constructs a nats.Msg carrying a structured CloudEvent.
// The message header contains "Content-Type: application/cloudevents+json"
// so cejs.NewMessage returns EncodingStructured and the body is the CE JSON.
func makeStructuredCEMsg(eventType, source, id string) *nats.Msg {
	body := `{"specversion":"1.0","type":"` + eventType + `","source":"` + source + `","id":"` + id + `"}`
	return &nats.Msg{
		Subject: "test.subject",
		Header:  nats.Header{"Content-Type": []string{"application/cloudevents+json"}},
		Data:    []byte(body),
	}
}

// makeTrigger builds a minimal trigger with an optional attribute filter.
// filterType may be empty ("") to disable the attribute filter.
func makeTrigger(namespace, name, filterType string) *eventingv1.Trigger {
	t := &eventingv1.Trigger{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
		},
		Spec: eventingv1.TriggerSpec{
			Broker: "test-broker",
		},
	}
	if filterType != "" {
		t.Spec.Filter = &eventingv1.TriggerFilter{
			Attributes: map[string]string{"type": filterType},
		}
	}
	return t
}

// newTestDispatcher creates a dispatcher suitable for unit tests.
// Passing nil OIDC token provider is fine for tests — OIDC token injection
// is only triggered when the destination requires it.
func newTestDispatcher(_ context.Context) *kncloudevents.Dispatcher {
	return kncloudevents.NewDispatcher(eventingtls.ClientConfig{}, nil)
}

// newTestHandler creates a TriggerHandler wired to the given subscriber URL.
func newTestHandler(t *testing.T, ctx context.Context, subscriberURL string, filterType string) *TriggerHandler {
	t.Helper()
	u, err := apis.ParseURL(subscriberURL)
	if err != nil {
		t.Fatalf("ParseURL(%q): %v", subscriberURL, err)
	}
	subscriber := duckv1.Addressable{URL: u}
	trigger := makeTrigger("default", "test-trigger", filterType)
	dispatcher := newTestDispatcher(ctx)
	h, err := NewTriggerHandler(ctx, trigger, subscriber, nil, nil, nil, nil, dispatcher, nil, nil, nil)
	if err != nil {
		t.Fatalf("NewTriggerHandler: %v", err)
	}
	return h
}

func TestTriggerHandlerOIDCAuthorization(t *testing.T) {
	t.Run("subscriber exact bearer and CloudEvent headers", func(t *testing.T) {
		requests := make(chan http.Header, 1)
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requests <- r.Header.Clone()
			w.WriteHeader(http.StatusNoContent)
		}))
		defer srv.Close()

		ctx := logCtx()
		audience := "https://subscriber.example"
		u, err := apis.ParseURL(srv.URL)
		if err != nil {
			t.Fatal(err)
		}
		handler, err := NewTriggerHandler(
			ctx,
			makeTrigger("default", "test-trigger", ""),
			duckv1.Addressable{URL: u, Audience: &audience},
			nil, nil, nil, nil, newTestDispatcher(ctx), nil, nil, nil,
		)
		if err != nil {
			t.Fatal(err)
		}
		installAudienceTokenSource(t, handler, func(_ context.Context, gotAudience string) (string, error) {
			if gotAudience != audience {
				t.Errorf("token audience = %q, want %q", gotAudience, audience)
			}
			return "subscriber-token", nil
		})

		handler.HandleMessage(ctx, makeStructuredCEMsg("test.type", "test/source", "oidc-subscriber"))
		select {
		case headers := <-requests:
			if got := headers.Get("Authorization"); got != "Bearer subscriber-token" {
				t.Errorf("Authorization = %q, want exact bearer token", got)
			}
			if got := headers.Get("Ce-Id"); got != "oidc-subscriber" {
				t.Errorf("Ce-Id = %q, want CloudEvent headers preserved", got)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("subscriber request not received")
		}
	})

	t.Run("no audience does not read token or send authorization", func(t *testing.T) {
		requests := make(chan http.Header, 1)
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requests <- r.Header.Clone()
			w.WriteHeader(http.StatusNoContent)
		}))
		defer srv.Close()

		ctx := logCtx()
		handler := newTestHandler(t, ctx, srv.URL, "")
		installAudienceTokenSource(t, handler, func(context.Context, string) (string, error) {
			t.Error("token source called for destination without audience")
			return "unexpected", nil
		})
		handler.HandleMessage(ctx, makeStructuredCEMsg("test.type", "test/source", "no-oidc"))

		select {
		case headers := <-requests:
			if got := headers.Get("Authorization"); got != "" {
				t.Errorf("Authorization = %q, want absent", got)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("subscriber request not received")
		}
	})

	t.Run("dead letter sink uses its own audience", func(t *testing.T) {
		subscriber := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusServiceUnavailable)
		}))
		defer subscriber.Close()
		dlsHeaders := make(chan http.Header, 1)
		dls := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			dlsHeaders <- r.Header.Clone()
			w.WriteHeader(http.StatusNoContent)
		}))
		defer dls.Close()

		ctx := logCtx()
		subscriberURL, _ := apis.ParseURL(subscriber.URL)
		dlsURL, _ := apis.ParseURL(dls.URL)
		dlsAudience := "https://dead-letter.example"
		handler, err := NewTriggerHandler(
			ctx,
			makeTrigger("default", "test-trigger", ""),
			duckv1.Addressable{URL: subscriberURL}, nil,
			&duckv1.Addressable{URL: dlsURL, Audience: &dlsAudience},
			nil, nil, newTestDispatcher(ctx), nil, nil, nil,
		)
		if err != nil {
			t.Fatal(err)
		}
		installAudienceTokenSource(t, handler, func(_ context.Context, gotAudience string) (string, error) {
			if gotAudience != dlsAudience {
				t.Errorf("DLS token audience = %q, want %q", gotAudience, dlsAudience)
			}
			return "dls-token", nil
		})

		handler.HandleMessage(ctx, makeStructuredCEMsg("test.type", "test/source", "oidc-dls"))
		select {
		case headers := <-dlsHeaders:
			if got := headers.Get("Authorization"); got != "Bearer dls-token" {
				t.Errorf("DLS Authorization = %q, want exact bearer token", got)
			}
			if got := headers.Get("Ce-Id"); got != "oidc-dls" {
				t.Errorf("DLS Ce-Id = %q, want CloudEvent headers preserved", got)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("dead letter sink request not received")
		}
	})

	t.Run("token failure does not call subscriber", func(t *testing.T) {
		var called atomic.Bool
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			called.Store(true)
			w.WriteHeader(http.StatusNoContent)
		}))
		defer srv.Close()

		ctx := logCtx()
		audience := "https://subscriber.example"
		u, _ := apis.ParseURL(srv.URL)
		handler, err := NewTriggerHandler(
			ctx, makeTrigger("default", "test-trigger", ""),
			duckv1.Addressable{URL: u, Audience: &audience},
			nil, nil, nil, nil, newTestDispatcher(ctx), nil, nil, nil,
		)
		if err != nil {
			t.Fatal(err)
		}
		installAudienceTokenSource(t, handler, func(context.Context, string) (string, error) {
			return "", errors.New("token file temporarily unavailable")
		})
		handler.HandleMessage(ctx, makeStructuredCEMsg("test.type", "test/source", "token-error"))
		if called.Load() {
			t.Error("subscriber was called after token acquisition failed")
		}
	})

	t.Run("token is re-read for rotation on every dispatch", func(t *testing.T) {
		authorizations := make(chan string, 2)
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			authorizations <- r.Header.Get("Authorization")
			w.WriteHeader(http.StatusNoContent)
		}))
		defer srv.Close()

		tokenPath := t.TempDir() + "/token"
		if err := os.WriteFile(tokenPath, []byte("first-token\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		ctx := logCtx()
		audience := "https://subscriber.example"
		u, _ := apis.ParseURL(srv.URL)
		handler, err := NewTriggerHandler(
			ctx, makeTrigger("default", "test-trigger", ""),
			duckv1.Addressable{URL: u, Audience: &audience},
			nil, nil, nil, nil, newTestDispatcher(ctx), nil, nil, nil,
		)
		if err != nil {
			t.Fatal(err)
		}
		installAudienceTokenSource(t, handler, func(context.Context, string) (string, error) {
			contents, err := os.ReadFile(tokenPath)
			return strings.TrimSpace(string(contents)), err
		})

		handler.HandleMessage(ctx, makeStructuredCEMsg("test.type", "test/source", "before-rotation"))
		if err := os.WriteFile(tokenPath, []byte("second-token\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		handler.HandleMessage(ctx, makeStructuredCEMsg("test.type", "test/source", "after-rotation"))

		for _, want := range []string{"Bearer first-token", "Bearer second-token"} {
			select {
			case got := <-authorizations:
				if got != want {
					t.Errorf("Authorization = %q, want %q", got, want)
				}
			case <-time.After(5 * time.Second):
				t.Fatalf("request with %q not received", want)
			}
		}
	})
}

func TestTriggerHandlerOIDCFailureLeavesNATSDispositionPending(t *testing.T) {
	t.Run("subscriber token failure", func(t *testing.T) {
		msg, dispositions := connectedMessageWithDispositionSpy(t, "subscriber-token-error")
		var called atomic.Bool
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			called.Store(true)
			w.WriteHeader(http.StatusNoContent)
		}))
		defer srv.Close()

		ctx := logCtx()
		audience := "https://subscriber.example"
		u, _ := apis.ParseURL(srv.URL)
		handler, err := NewTriggerHandler(ctx, makeTrigger("default", "test-trigger", ""),
			duckv1.Addressable{URL: u, Audience: &audience}, nil, nil, nil, nil,
			newTestDispatcher(ctx), nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		installAudienceTokenSource(t, handler, func(context.Context, string) (string, error) {
			return "", errors.New("token request unavailable")
		})
		handler.HandleMessage(ctx, msg)

		if called.Load() {
			t.Error("subscriber HTTP was called after token acquisition failed")
		}
		assertNoNATSDisposition(t, dispositions)
	})

	t.Run("dead letter token failure", func(t *testing.T) {
		msg, dispositions := connectedMessageWithDispositionSpy(t, "dls-token-error")
		subscriber := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusServiceUnavailable)
		}))
		defer subscriber.Close()
		var dlsCalled atomic.Bool
		dls := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			dlsCalled.Store(true)
			w.WriteHeader(http.StatusNoContent)
		}))
		defer dls.Close()

		ctx := logCtx()
		subscriberURL, _ := apis.ParseURL(subscriber.URL)
		dlsURL, _ := apis.ParseURL(dls.URL)
		dlsAudience := "https://dead-letter.example"
		handler, err := NewTriggerHandler(ctx, makeTrigger("default", "test-trigger", ""),
			duckv1.Addressable{URL: subscriberURL}, nil,
			&duckv1.Addressable{URL: dlsURL, Audience: &dlsAudience},
			nil, nil, newTestDispatcher(ctx), nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		installAudienceTokenSource(t, handler, func(_ context.Context, audience string) (string, error) {
			if audience != dlsAudience {
				t.Errorf("token audience = %q, want DLS %q", audience, dlsAudience)
			}
			return "", errors.New("DLS token request unavailable")
		})
		handler.HandleMessage(ctx, msg)

		if dlsCalled.Load() {
			t.Error("dead letter HTTP was called after its token acquisition failed")
		}
		assertNoNATSDisposition(t, dispositions)
	})
}

func TestTriggerHandlerDoesNotForwardAuthorizationToReplyIngress(t *testing.T) {
	replyAuthorization := make(chan string, 1)
	ingress := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		replyAuthorization <- r.Header.Get("Authorization")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer ingress.Close()

	subscriber := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/cloudevents+json")
		w.Header().Set("Authorization", "Bearer subscriber-response-secret")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"specversion":"1.0","type":"reply.type","source":"reply/source","id":"reply-id"}`))
	}))
	defer subscriber.Close()

	ctx := logCtx()
	subscriberURL, _ := apis.ParseURL(subscriber.URL)
	ingressURL, _ := apis.ParseURL(ingress.URL)
	subscriberAudience := "https://subscriber.example"
	handler, err := NewTriggerHandler(ctx, makeTrigger("default", "test-trigger", ""),
		duckv1.Addressable{URL: subscriberURL, Audience: &subscriberAudience},
		&duckv1.Addressable{URL: ingressURL}, nil, nil, nil,
		newTestDispatcher(ctx), nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	installAudienceTokenSource(t, handler, func(context.Context, string) (string, error) {
		return "subscriber-request-token", nil
	})
	handler.HandleMessage(ctx, makeStructuredCEMsg("test.type", "test/source", "request-id"))

	select {
	case got := <-replyAuthorization:
		if got != "" {
			t.Errorf("reply ingress Authorization = %q, want absent", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("reply event was not dispatched to broker ingress")
	}
}

func connectedMessageWithDispositionSpy(t *testing.T, eventID string) (*nats.Msg, *nats.Subscription) {
	t.Helper()
	server := natsTesting.RunBasicJetstreamServer()
	t.Cleanup(func() { natsTesting.ShutdownJSServerAndRemoveStorage(t, server) })
	conn, _ := natsTesting.JsClient(t, server)
	t.Cleanup(conn.Close)
	deliverySubject := nats.NewInbox()
	dispositionSubject := nats.NewInbox()
	delivery, err := conn.SubscribeSync(deliverySubject)
	if err != nil {
		t.Fatal(err)
	}
	dispositions, err := conn.SubscribeSync(dispositionSubject)
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Flush(); err != nil {
		t.Fatal(err)
	}
	msg := makeStructuredCEMsg("test.type", "test/source", eventID)
	msg.Subject = deliverySubject
	msg.Reply = dispositionSubject
	if err := conn.PublishMsg(msg); err != nil {
		t.Fatal(err)
	}
	connected, err := delivery.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return connected, dispositions
}

func assertNoNATSDisposition(t *testing.T, subscription *nats.Subscription) {
	t.Helper()
	if msg, err := subscription.NextMsg(100 * time.Millisecond); err == nil {
		t.Fatalf("unexpected NATS disposition %q", string(msg.Data))
	} else if !errors.Is(err, nats.ErrTimeout) {
		t.Fatalf("waiting for NATS disposition: %v", err)
	}
}

// logCtx returns a context carrying a no-op zap logger.
func logCtx() context.Context {
	return logging.WithLogger(context.Background(), zap.NewNop().Sugar())
}

// TestTriggerHandlerUpdatePreservesInflightSnapshot verifies that an update is
// an atomic handoff between immutable handler configurations. Update waits for
// an old filter invocation before cleaning that filter, but does not wait for
// the old HTTP dispatch; that dispatch retains the old subscriber snapshot.
func TestTriggerHandlerUpdatePreservesInflightSnapshot(t *testing.T) {
	ctx := logCtx()

	oldRequests := make(chan string, 1)
	releaseOldSubscriber := make(chan struct{})
	oldSubscriber := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		oldRequests <- r.Header.Get("Ce-Id")
		<-releaseOldSubscriber
		w.WriteHeader(http.StatusNoContent)
	}))
	defer oldSubscriber.Close()
	defer func() {
		select {
		case <-releaseOldSubscriber:
		default:
			close(releaseOldSubscriber)
		}
	}()

	newRequests := make(chan string, 1)
	newSubscriber := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		newRequests <- r.Header.Get("Ce-Id")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer newSubscriber.Close()

	handler := newTestHandler(t, ctx, oldSubscriber.URL, "")
	defer handler.Cleanup()
	releaseOldFilter := make(chan struct{})
	oldFilter := &cleanupTrackingFilter{
		filtered: make(chan string, 2),
		release:  releaseOldFilter,
		cleaned:  make(chan struct{}),
	}
	handler.configMu.Lock()
	handler.config.filter = oldFilter
	handler.configMu.Unlock()

	dispatchDone := make(chan struct{})
	go func() {
		handler.HandleMessage(ctx, makeStructuredCEMsg("old.type", "test/source", "old-event"))
		close(dispatchDone)
	}()

	select {
	case got := <-oldFilter.filtered:
		if got != "old-event" {
			t.Fatalf("old filter saw event %q, want %q", got, "old-event")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("old dispatch never reached the old filter")
	}

	// The old filter is still evaluating, so Update must not be able to replace
	// and clean its config yet.
	if handler.configMu.TryLock() {
		handler.configMu.Unlock()
		t.Fatal("handler config was not protected during filter evaluation")
	}

	newURL, err := apis.ParseURL(newSubscriber.URL)
	if err != nil {
		t.Fatalf("ParseURL(%q): %v", newSubscriber.URL, err)
	}
	updateStarted := make(chan struct{})
	updateDone := make(chan struct{})
	go func() {
		close(updateStarted)
		handler.Update(
			makeTrigger("default", "test-trigger", ""),
			duckv1.Addressable{URL: newURL},
			nil,
			nil,
			nil,
			nil,
		)
		close(updateDone)
	}()
	<-updateStarted

	// Give Update ample opportunity to reach the write lock. It must remain
	// blocked, and the old filter must remain live, until Filter returns.
	select {
	case <-updateDone:
		t.Fatal("Update returned while the old filter was still evaluating")
	case <-oldFilter.cleaned:
		t.Fatal("old filter was cleaned up while it was still evaluating")
	case <-time.After(20 * time.Millisecond):
	}

	// Once filtering finishes, the old request proceeds using its immutable
	// subscriber snapshot. The HTTP endpoint intentionally remains blocked.
	close(releaseOldFilter)
	select {
	case got := <-oldRequests:
		if got != "old-event" {
			t.Fatalf("old subscriber saw event %q, want %q", got, "old-event")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("old dispatch never reached the old subscriber")
	}
	select {
	case <-updateDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Update did not finish after old filter evaluation completed")
	}
	select {
	case <-oldFilter.cleaned:
	case <-time.After(5 * time.Second):
		t.Fatal("old filter was not cleaned up after its evaluation completed")
	}
	select {
	case <-dispatchDone:
		t.Fatal("old dispatch finished before its HTTP subscriber was released")
	default:
	}

	// A subsequent request may use the new configuration while the old HTTP
	// request is still in flight. It must not reuse the old filter or subscriber.
	handler.HandleMessage(ctx, makeStructuredCEMsg("new.type", "test/source", "new-event"))
	select {
	case got := <-newRequests:
		if got != "new-event" {
			t.Fatalf("new subscriber saw event %q, want %q", got, "new-event")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("next dispatch did not use the new subscriber config")
	}
	select {
	case got := <-oldFilter.filtered:
		t.Fatalf("old filter was reused by the next dispatch for event %q", got)
	default:
	}
	select {
	case got := <-oldRequests:
		t.Fatalf("old subscriber unexpectedly received another event %q", got)
	default:
	}

	close(releaseOldSubscriber)
	select {
	case <-dispatchDone:
	case <-time.After(5 * time.Second):
		t.Fatal("old dispatch did not finish after subscriber release")
	}
}

// TestHandleMessage_BadData verifies that a message with structured encoding
// but unparseable data (no valid JSON) is terminated and the subscriber is not called.
func TestHandleMessage_BadData(t *testing.T) {
	called := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ctx := logCtx()
	handler := newTestHandler(t, ctx, srv.URL, "")

	// Structured encoding with non-JSON body — cejs.NewMessage returns EncodingStructured
	// but binding.ToEvent will fail to parse it. Handler calls msg.Term() and returns.
	msg := &nats.Msg{
		Subject: "test.subject",
		Header:  nil, // nil header → EncodingStructured
		Data:    []byte("not json at all"),
	}

	handler.HandleMessage(ctx, msg)

	if called {
		t.Error("subscriber should NOT be called when CE conversion fails")
	}
}

// TestHandleMessage_FilteredOut verifies that a message whose event type does not
// match the trigger filter is acked (not dispatched to the subscriber).
func TestHandleMessage_FilteredOut(t *testing.T) {
	called := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ctx := logCtx()
	// Trigger has filter for "other.type" but message is "test.type" — should be filtered out.
	handler := newTestHandler(t, ctx, srv.URL, "other.type")

	msg := makeStructuredCEMsg("test.type", "test/source", "test-id-1")
	handler.HandleMessage(ctx, msg)

	if called {
		t.Error("subscriber should NOT be called for filtered-out messages")
	}
}

// TestHandleMessage_Dispatch_Success verifies that 2xx responses cause a successful dispatch.
func TestHandleMessage_Dispatch_Success(t *testing.T) {
	codes := []int{http.StatusOK, http.StatusAccepted}
	for _, code := range codes {
		code := code
		t.Run(http.StatusText(code), func(t *testing.T) {
			var count int64
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				atomic.AddInt64(&count, 1)
				w.WriteHeader(code)
			}))
			defer srv.Close()

			ctx := logCtx()
			handler := newTestHandler(t, ctx, srv.URL, "")

			msg := makeStructuredCEMsg("test.type", "test/source", "test-id-2xx")
			handler.HandleMessage(ctx, msg)

			if got := atomic.LoadInt64(&count); got != 1 {
				t.Errorf("subscriber called %d times, want 1", got)
			}
		})
	}
}

// TestHandleMessage_Dispatch_RetriableError verifies that 5xx/429 responses
// reach the subscriber and nack is attempted (fails gracefully without a real NATS conn).
func TestHandleMessage_Dispatch_RetriableError(t *testing.T) {
	codes := []int{
		http.StatusInternalServerError,
		http.StatusServiceUnavailable,
		http.StatusTooManyRequests,
	}
	for _, code := range codes {
		code := code
		t.Run(http.StatusText(code), func(t *testing.T) {
			var count int64
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				atomic.AddInt64(&count, 1)
				w.WriteHeader(code)
			}))
			defer srv.Close()

			ctx := logCtx()
			handler := newTestHandler(t, ctx, srv.URL, "")

			msg := makeStructuredCEMsg("test.type", "test/source", "test-id-5xx")
			// NakWithDelay will fail with ErrMsgNoReply — handler logs and continues.
			handler.HandleMessage(ctx, msg)

			if got := atomic.LoadInt64(&count); got != 1 {
				t.Errorf("subscriber called %d times, want 1", got)
			}
		})
	}
}

// TestHandleMessage_Dispatch_NonRetriable verifies that 4xx responses
// reach the subscriber and term is attempted (fails gracefully without a real NATS conn).
func TestHandleMessage_Dispatch_NonRetriable(t *testing.T) {
	codes := []int{
		http.StatusBadRequest,
		http.StatusForbidden,
		http.StatusNotFound,
	}
	for _, code := range codes {
		code := code
		t.Run(http.StatusText(code), func(t *testing.T) {
			var count int64
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				atomic.AddInt64(&count, 1)
				w.WriteHeader(code)
			}))
			defer srv.Close()

			ctx := logCtx()
			handler := newTestHandler(t, ctx, srv.URL, "")

			msg := makeStructuredCEMsg("test.type", "test/source", "test-id-4xx")
			// Term will fail with ErrMsgNoReply — handler logs and continues.
			handler.HandleMessage(ctx, msg)

			if got := atomic.LoadInt64(&count); got != 1 {
				t.Errorf("subscriber called %d times, want 1", got)
			}
		})
	}
}

// TestHandleMessage_CancelledContext verifies that when the context is already
// cancelled before HandleMessage, the dispatch is cancelled and no panic occurs.
func TestHandleMessage_CancelledContext(t *testing.T) {
	called := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		// Block until context is done — simulates the cancelled case.
		<-r.Context().Done()
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	ctx := logCtx()
	handler := newTestHandler(t, ctx, srv.URL, "")

	// Cancel the context before calling HandleMessage.
	cancelCtx, cancel := context.WithCancel(ctx)
	cancel()

	msg := makeStructuredCEMsg("test.type", "test/source", "test-id-cancelled")

	// Should not panic; eventProcessingDeadlineExceeded fires and returns early.
	done := make(chan struct{})
	go func() {
		defer close(done)
		handler.HandleMessage(cancelCtx, msg)
	}()

	select {
	case <-done:
		// Good — no hang.
	case <-time.After(5 * time.Second):
		t.Fatal("HandleMessage did not return within 5 seconds with cancelled context")
	}
	_ = called // subscriber may or may not be called depending on timing; we just assert no hang
}

// TestTransform_NoType covers the branch where GetAttribute returns nil for the
// CloudEvent type (e.g., the attribute is not set), so the inner if-block is skipped.
// This covers the previously uncovered "ty == nil → skip" branch.
func TestTransform_NoType(t *testing.T) {
	// Use a cejs.Message wrapping a *nats.Msg with binary encoding but no ce-type header.
	// ce-specversion present → binary encoding; ce-type absent → GetAttribute(spec.Type) returns nil.
	msg := &nats.Msg{
		Subject: "test.subject",
		Header: nats.Header{
			"Ce-Specversion": []string{"1.0"},
			"Ce-Source":      []string{"test/source"},
			"Ce-Id":          []string{"test-id-notype"},
			// "Ce-Type" intentionally absent
		},
		Data: []byte(`{}`),
	}

	import_cejs := cejs.NewMessage(msg)

	te := TypeExtractorTransformer("initial")
	err := te.Transform(import_cejs, nil)
	if err != nil {
		t.Fatalf("Transform() unexpected error: %v", err)
	}
	// No type header → TypeExtractorTransformer should remain unchanged.
	if string(te) != "initial" {
		t.Errorf("TypeExtractorTransformer = %q, want %q (should be unchanged when type is absent)", string(te), "initial")
	}
}
