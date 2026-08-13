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
	"fmt"
	"sync"
	"testing"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	"knative.dev/pkg/logging"

	eventingv1 "knative.dev/eventing/pkg/apis/eventing/v1"
	"knative.dev/eventing/pkg/eventfilter"
)

type lifecycleRecorder struct {
	mu      sync.Mutex
	actions []string
}

func (r *lifecycleRecorder) record(action string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.actions = append(r.actions, action)
}

func (r *lifecycleRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.actions...)
}

type blockingPullSubscription struct {
	recorder      *lifecycleRecorder
	fetchStarted  chan struct{}
	fetchCanceled chan error
	releaseFetch  chan struct{}
	unsubscribed  chan struct{}
}

func (s *blockingPullSubscription) Fetch(ctx context.Context, _ int) ([]*nats.Msg, error) {
	s.recorder.record("fetch-start")
	close(s.fetchStarted)
	<-ctx.Done()
	s.recorder.record("fetch-context-canceled")
	s.fetchCanceled <- ctx.Err()
	<-s.releaseFetch
	s.recorder.record("fetch-return")
	return nil, ctx.Err()
}

func (s *blockingPullSubscription) Unsubscribe() error {
	s.recorder.record("unsubscribe")
	close(s.unsubscribed)
	return nil
}

type lifecycleFilter struct {
	recorder *lifecycleRecorder
	cleaned  chan struct{}
}

type shutdownPullSubscription struct {
	recorder     *lifecycleRecorder
	unsubscribed chan struct{}
	err          error
	once         sync.Once
}

func (*shutdownPullSubscription) Fetch(context.Context, int) ([]*nats.Msg, error) {
	return nil, nats.ErrTimeout
}

func (s *shutdownPullSubscription) Unsubscribe() error {
	s.recorder.record("unsubscribe")
	s.once.Do(func() { close(s.unsubscribed) })
	return s.err
}

func (*lifecycleFilter) Filter(context.Context, cloudevents.Event) eventfilter.FilterResult {
	return eventfilter.PassFilter
}

func (f *lifecycleFilter) Cleanup() {
	f.recorder.record("cleanup")
	close(f.cleaned)
}

func TestConsumerManagerConfigDefaults(t *testing.T) {
	// Verify default values
	if DefaultFetchBatchSize != 10 {
		t.Errorf("DefaultFetchBatchSize = %v, want 10", DefaultFetchBatchSize)
	}

	if DefaultFetchTimeout != 200*time.Millisecond {
		t.Errorf("DefaultFetchTimeout = %v, want 200ms", DefaultFetchTimeout)
	}
}

func TestConsumerManagerConfig(t *testing.T) {
	tests := []struct {
		name               string
		config             *ConsumerManagerConfig
		wantFetchBatchSize int
		wantFetchTimeout   time.Duration
	}{
		{
			name:               "nil config uses defaults",
			config:             nil,
			wantFetchBatchSize: DefaultFetchBatchSize,
			wantFetchTimeout:   DefaultFetchTimeout,
		},
		{
			name:               "empty config uses defaults",
			config:             &ConsumerManagerConfig{},
			wantFetchBatchSize: DefaultFetchBatchSize,
			wantFetchTimeout:   DefaultFetchTimeout,
		},
		{
			name: "zero values use defaults",
			config: &ConsumerManagerConfig{
				FetchBatchSize: 0,
				FetchTimeout:   0,
			},
			wantFetchBatchSize: DefaultFetchBatchSize,
			wantFetchTimeout:   DefaultFetchTimeout,
		},
		{
			name: "custom batch size only",
			config: &ConsumerManagerConfig{
				FetchBatchSize: 20,
				FetchTimeout:   0,
			},
			wantFetchBatchSize: 20,
			wantFetchTimeout:   DefaultFetchTimeout,
		},
		{
			name: "custom timeout only",
			config: &ConsumerManagerConfig{
				FetchBatchSize: 0,
				FetchTimeout:   1 * time.Second,
			},
			wantFetchBatchSize: DefaultFetchBatchSize,
			wantFetchTimeout:   1 * time.Second,
		},
		{
			name: "both custom values",
			config: &ConsumerManagerConfig{
				FetchBatchSize: 50,
				FetchTimeout:   2 * time.Second,
			},
			wantFetchBatchSize: 50,
			wantFetchTimeout:   2 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// We can't easily test NewConsumerManager without a real NATS connection,
			// so we test the config application logic directly
			fetchBatchSize := DefaultFetchBatchSize
			fetchTimeout := DefaultFetchTimeout

			if tt.config != nil {
				if tt.config.FetchBatchSize > 0 {
					fetchBatchSize = tt.config.FetchBatchSize
				}
				if tt.config.FetchTimeout > 0 {
					fetchTimeout = tt.config.FetchTimeout
				}
			}

			if fetchBatchSize != tt.wantFetchBatchSize {
				t.Errorf("fetchBatchSize = %v, want %v", fetchBatchSize, tt.wantFetchBatchSize)
			}

			if fetchTimeout != tt.wantFetchTimeout {
				t.Errorf("fetchTimeout = %v, want %v", fetchTimeout, tt.wantFetchTimeout)
			}
		})
	}
}

func TestGetSubscriptionCount(t *testing.T) {
	ctx := logging.WithLogger(context.Background(), logging.FromContext(context.TODO()))

	tests := []struct {
		name  string
		count int
	}{
		{"empty map", 0},
		{"one entry", 1},
		{"three entries", 3},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cm := &ConsumerManager{
				logger:        logging.FromContext(ctx),
				subscriptions: make(map[string]*TriggerSubscription),
			}
			for i := 0; i < tc.count; i++ {
				uid := fmt.Sprintf("uid-%d", i)
				cm.subscriptions[uid] = &TriggerSubscription{}
			}
			if got := cm.GetSubscriptionCount(); got != tc.count {
				t.Errorf("GetSubscriptionCount() = %d, want %d", got, tc.count)
			}
		})
	}
}

func TestHasSubscription(t *testing.T) {
	ctx := logging.WithLogger(context.Background(), logging.FromContext(context.TODO()))

	cm := &ConsumerManager{
		logger:        logging.FromContext(ctx),
		subscriptions: make(map[string]*TriggerSubscription),
	}
	cm.subscriptions["existing-uid"] = &TriggerSubscription{}

	if !cm.HasSubscription("existing-uid") {
		t.Error("HasSubscription() = false for existing UID, want true")
	}
	if cm.HasSubscription("missing-uid") {
		t.Error("HasSubscription() = true for missing UID, want false")
	}
}

func TestConsumerManagerClose(t *testing.T) {
	ctx := logging.WithLogger(context.Background(), logging.FromContext(context.TODO()))

	cm := &ConsumerManager{
		logger:        logging.FromContext(ctx),
		subscriptions: make(map[string]*TriggerSubscription),
	}

	err := cm.Close()
	if err != nil {
		t.Errorf("Close() unexpected error on empty subscriptions: %v", err)
	}
}

func TestUnsubscribeTrigger_NotFound(t *testing.T) {
	ctx := logging.WithLogger(context.Background(), logging.FromContext(context.TODO()))

	cm := &ConsumerManager{
		logger:        logging.FromContext(ctx),
		subscriptions: make(map[string]*TriggerSubscription),
	}

	err := cm.UnsubscribeTrigger("non-existent-uid")
	if err != nil {
		t.Errorf("UnsubscribeTrigger() unexpected error for non-existent UID: %v", err)
	}
}

// TestUnsubscribeTriggerWaitsForFetchLoopBeforeTeardown covers the lifecycle
// race where unsubscribe begins while Fetch is blocked and inflight is still
// zero. The fetch loop must be fully stopped before dispatch cancellation and
// Wait, otherwise a late inflight.Add can race with teardown.
func TestUnsubscribeTriggerWaitsForFetchLoopBeforeTeardown(t *testing.T) {
	ctx := logging.WithLogger(context.Background(), zap.NewNop().Sugar())
	recorder := &lifecycleRecorder{}
	releaseFetch := make(chan struct{})
	releaseInflight := make(chan struct{})
	defer func() {
		select {
		case <-releaseFetch:
		default:
			close(releaseFetch)
		}
		select {
		case <-releaseInflight:
		default:
			close(releaseInflight)
		}
	}()

	pullSub := &blockingPullSubscription{
		recorder:      recorder,
		fetchStarted:  make(chan struct{}),
		fetchCanceled: make(chan error, 1),
		releaseFetch:  releaseFetch,
		unsubscribed:  make(chan struct{}),
	}
	filter := &lifecycleFilter{
		recorder: recorder,
		cleaned:  make(chan struct{}),
	}
	handler := &TriggerHandler{config: &handlerConfig{filter: filter}}

	fetchCtx, fetchCancel := context.WithCancel(ctx)
	defer fetchCancel()
	dispatchCtx, cancelDispatch := context.WithCancel(ctx)
	defer cancelDispatch()
	dispatchCanceled := make(chan struct{})
	triggerUID := "lifecycle-trigger-uid"
	done := make(chan struct{})
	sub := &TriggerSubscription{
		trigger: &eventingv1.Trigger{ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "lifecycle-trigger",
			UID:       "lifecycle-trigger-uid",
		}},
		subscription: pullSub,
		handler:      handler,
		dispatchCtx:  dispatchCtx,
		cancel:       fetchCancel,
		done:         done,
		dispatchCancel: func() {
			recorder.record("dispatch-cancel")
			cancelDispatch()
			close(dispatchCanceled)
		},
	}
	manager := &ConsumerManager{
		logger: logging.FromContext(ctx),
		ctx:    ctx,
		subscriptions: map[string]*TriggerSubscription{
			triggerUID: sub,
		},
	}

	go manager.fetchLoop(
		fetchCtx,
		dispatchCtx,
		done,
		&sub.inflight,
		pullSub,
		handler,
		0,
		1,
		time.Hour,
		nil,
		manager.logger,
	)
	select {
	case <-pullSub.fetchStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("fetch loop did not enter Fetch")
	}

	unsubscribeResult := make(chan error, 1)
	go func() {
		unsubscribeResult <- manager.UnsubscribeTrigger(triggerUID)
	}()

	select {
	case err := <-pullSub.fetchCanceled:
		if err != context.Canceled {
			t.Fatalf("Fetch context error = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Fetch context was not canceled by unsubscribe")
	}

	// Fetch has observed cancellation but deliberately has not returned, so the
	// fetch-loop done gate is still open. No teardown action may cross it.
	select {
	case <-done:
		t.Fatal("fetch loop reported done before blocked Fetch returned")
	default:
	}
	select {
	case <-dispatchCanceled:
		t.Fatal("dispatch context was canceled before fetch loop stopped")
	case <-pullSub.unsubscribed:
		t.Fatal("pull subscription was unsubscribed before fetch loop stopped")
	case <-filter.cleaned:
		t.Fatal("handler was cleaned before fetch loop stopped")
	case err := <-unsubscribeResult:
		t.Fatalf("UnsubscribeTrigger returned before fetch loop stopped: %v", err)
	default:
	}

	// Model a dispatch admitted by the fetch generation that is only now
	// finishing. The counter was zero when unsubscribe began; Add is safe here
	// because unsubscribe cannot start Wait until the fetch-loop done gate closes.
	sub.inflight.Add(1)
	recorder.record("inflight-add")
	dispatchObservedCancel := make(chan struct{})
	go func() {
		<-dispatchCtx.Done()
		recorder.record("dispatch-observed-cancel")
		close(dispatchObservedCancel)
		<-releaseInflight
		recorder.record("inflight-done")
		sub.inflight.Done()
	}()

	close(releaseFetch)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("fetch loop did not stop after Fetch returned")
	}
	select {
	case <-dispatchObservedCancel:
	case <-time.After(5 * time.Second):
		t.Fatal("dispatch context was not canceled after fetch loop stopped")
	}

	// Once fetch is done, unsubscribe cancels dispatches and waits for inflight
	// work. The pull subscription and handler must remain live during that wait.
	select {
	case <-pullSub.unsubscribed:
		t.Fatal("pull subscription was unsubscribed before inflight completed")
	case <-filter.cleaned:
		t.Fatal("handler was cleaned before inflight completed")
	case err := <-unsubscribeResult:
		t.Fatalf("UnsubscribeTrigger returned before inflight completed: %v", err)
	default:
	}

	close(releaseInflight)
	select {
	case err := <-unsubscribeResult:
		if err != nil {
			t.Fatalf("UnsubscribeTrigger() error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("UnsubscribeTrigger did not return after inflight completed")
	}
	select {
	case <-pullSub.unsubscribed:
	default:
		t.Fatal("pull subscription was not unsubscribed")
	}
	select {
	case <-filter.cleaned:
	default:
		t.Fatal("handler was not cleaned")
	}

	wantActions := []string{
		"fetch-start",
		"fetch-context-canceled",
		"inflight-add",
		"fetch-return",
		"dispatch-cancel",
		"dispatch-observed-cancel",
		"inflight-done",
		"unsubscribe",
		"cleanup",
	}
	gotActions := recorder.snapshot()
	if fmt.Sprint(gotActions) != fmt.Sprint(wantActions) {
		t.Fatalf("lifecycle actions = %v, want %v", gotActions, wantActions)
	}
}

func TestConsumerManagerShutdownNaturallyDrainsAndIsIdempotent(t *testing.T) {
	ctx := logging.WithLogger(context.Background(), zap.NewNop().Sugar())
	recorder := &lifecycleRecorder{}
	producerStopped := make(chan struct{})
	releaseInflight := make(chan struct{})
	done := make(chan struct{})
	close(done)
	pullSub := &shutdownPullSubscription{
		recorder:     recorder,
		unsubscribed: make(chan struct{}),
	}
	filter := &lifecycleFilter{recorder: recorder, cleaned: make(chan struct{})}
	sub := &TriggerSubscription{
		subscription: pullSub,
		handler:      &TriggerHandler{config: &handlerConfig{filter: filter}},
		done:         done,
		cancel: func() {
			recorder.record("fetch-cancel")
			close(producerStopped)
		},
		dispatchCancel: func() {
			recorder.record("dispatch-cancel")
		},
	}
	sub.inflight.Add(1)
	go func() {
		<-releaseInflight
		recorder.record("inflight-done")
		sub.inflight.Done()
	}()
	manager := &ConsumerManager{
		logger:        logging.FromContext(ctx),
		subscriptions: map[string]*TriggerSubscription{"uid": sub},
		shutdownDone:  make(chan struct{}),
	}

	const callers = 20
	results := make(chan error, callers)
	go func() { results <- manager.Shutdown(context.Background()) }()
	select {
	case <-producerStopped:
	case <-time.After(5 * time.Second):
		t.Fatal("Shutdown did not stop the fetch producer")
	}
	for range callers - 1 {
		go func() { results <- manager.Shutdown(context.Background()) }()
	}

	// Natural drain waits without canceling dispatches or tearing resources down.
	select {
	case <-pullSub.unsubscribed:
		t.Fatal("subscription was torn down before inflight naturally drained")
	case <-filter.cleaned:
		t.Fatal("handler was cleaned before inflight naturally drained")
	default:
	}
	for _, action := range recorder.snapshot() {
		if action == "dispatch-cancel" {
			t.Fatal("natural shutdown canceled an in-flight dispatch")
		}
	}

	close(releaseInflight)
	for range callers {
		select {
		case err := <-results:
			if err != nil {
				t.Errorf("Shutdown() error = %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("concurrent Shutdown caller did not return")
		}
	}
	if got := manager.GetSubscriptionCount(); got != 0 {
		t.Errorf("subscription count = %d, want 0", got)
	}
	wantActions := "[fetch-cancel inflight-done unsubscribe cleanup]"
	if got := fmt.Sprint(recorder.snapshot()); got != wantActions {
		t.Errorf("shutdown actions = %s, want %s", got, wantActions)
	}
	if err := manager.Shutdown(context.Background()); err != nil {
		t.Errorf("repeated Shutdown() error = %v", err)
	}
	if got := fmt.Sprint(recorder.snapshot()); got != wantActions {
		t.Errorf("repeated Shutdown changed actions to %s, want %s", got, wantActions)
	}
}

func TestConsumerManagerShutdownDeadlineCancelsThenDrains(t *testing.T) {
	ctx := logging.WithLogger(context.Background(), zap.NewNop().Sugar())
	recorder := &lifecycleRecorder{}
	done := make(chan struct{})
	close(done)
	dispatchCtx, cancelDispatch := context.WithCancel(context.Background())
	pullSub := &shutdownPullSubscription{
		recorder:     recorder,
		unsubscribed: make(chan struct{}),
	}
	filter := &lifecycleFilter{recorder: recorder, cleaned: make(chan struct{})}
	var dispatchCancelOnce sync.Once
	sub := &TriggerSubscription{
		subscription: pullSub,
		handler:      &TriggerHandler{config: &handlerConfig{filter: filter}},
		done:         done,
		dispatchCancel: func() {
			dispatchCancelOnce.Do(func() { recorder.record("dispatch-cancel") })
			cancelDispatch()
		},
	}
	sub.inflight.Add(1)
	go func() {
		<-dispatchCtx.Done()
		recorder.record("dispatch-observed-cancel")
		sub.inflight.Done()
	}()
	manager := &ConsumerManager{
		logger:        logging.FromContext(ctx),
		subscriptions: map[string]*TriggerSubscription{"uid": sub},
		shutdownDone:  make(chan struct{}),
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := manager.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	wantActions := "[dispatch-cancel dispatch-observed-cancel unsubscribe cleanup]"
	if got := fmt.Sprint(recorder.snapshot()); got != wantActions {
		t.Errorf("forced shutdown actions = %s, want %s", got, wantActions)
	}
}

func TestConsumerManagerShutdownTimeoutIsStableAndIdempotent(t *testing.T) {
	ctx := logging.WithLogger(context.Background(), zap.NewNop().Sugar())
	recorder := &lifecycleRecorder{}
	done := make(chan struct{})
	close(done)
	pullSub := &shutdownPullSubscription{
		recorder:     recorder,
		unsubscribed: make(chan struct{}),
	}
	filter := &lifecycleFilter{recorder: recorder, cleaned: make(chan struct{})}
	var dispatchCancelOnce sync.Once
	sub := &TriggerSubscription{
		subscription: pullSub,
		handler:      &TriggerHandler{config: &handlerConfig{filter: filter}},
		done:         done,
		dispatchCancel: func() {
			dispatchCancelOnce.Do(func() { recorder.record("dispatch-cancel") })
		},
	}
	sub.inflight.Add(1) // Deliberately ignores dispatch cancellation until after timeout.
	manager := &ConsumerManager{
		logger:        logging.FromContext(ctx),
		subscriptions: map[string]*TriggerSubscription{"uid": sub},
		shutdownDone:  make(chan struct{}),
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := manager.Shutdown(shutdownCtx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown() error = %v, want context deadline exceeded", err)
	}
	if got := fmt.Sprint(recorder.snapshot()); got != "[dispatch-cancel]" {
		t.Errorf("timed-out shutdown actions = %s, want only dispatch cancellation", got)
	}
	select {
	case <-pullSub.unsubscribed:
		t.Fatal("timed-out shutdown unsubscribed while inflight was still running")
	default:
	}
	select {
	case <-filter.cleaned:
		t.Fatal("timed-out shutdown cleaned handler while inflight was still running")
	default:
	}
	if err := manager.Shutdown(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("repeated Shutdown() error = %v, want stable deadline error", err)
	}
	if got := fmt.Sprint(recorder.snapshot()); got != "[dispatch-cancel]" {
		t.Errorf("repeated Shutdown changed actions to %s", got)
	}

	// The caller returned promptly, but ownership is retained until the
	// dispatch eventually exits; teardown must then complete exactly once.
	sub.inflight.Done()
	select {
	case <-pullSub.unsubscribed:
	case <-time.After(5 * time.Second):
		t.Fatal("eventual cleanup did not unsubscribe after inflight completed")
	}
	select {
	case <-filter.cleaned:
	case <-time.After(5 * time.Second):
		t.Fatal("eventual cleanup did not clean handler after inflight completed")
	}
	wantActions := "[dispatch-cancel unsubscribe cleanup]"
	if got := fmt.Sprint(recorder.snapshot()); got != wantActions {
		t.Errorf("eventual cleanup actions = %s, want %s", got, wantActions)
	}
	if got := manager.GetSubscriptionCount(); got != 0 {
		t.Errorf("subscription count after eventual cleanup = %d, want 0", got)
	}
}

func TestConsumerManagerCloseCancelsInflightImmediately(t *testing.T) {
	ctx := logging.WithLogger(context.Background(), zap.NewNop().Sugar())
	recorder := &lifecycleRecorder{}
	done := make(chan struct{})
	close(done)
	dispatchCtx, cancelDispatch := context.WithCancel(context.Background())
	pullSub := &shutdownPullSubscription{
		recorder:     recorder,
		unsubscribed: make(chan struct{}),
	}
	filter := &lifecycleFilter{recorder: recorder, cleaned: make(chan struct{})}
	sub := &TriggerSubscription{
		subscription: pullSub,
		handler:      &TriggerHandler{config: &handlerConfig{filter: filter}},
		done:         done,
		dispatchCancel: func() {
			recorder.record("dispatch-cancel")
			cancelDispatch()
		},
	}
	sub.inflight.Add(1)
	go func() {
		<-dispatchCtx.Done()
		recorder.record("dispatch-observed-cancel")
		sub.inflight.Done()
	}()
	manager := &ConsumerManager{
		logger:        logging.FromContext(ctx),
		subscriptions: map[string]*TriggerSubscription{"uid": sub},
		shutdownDone:  make(chan struct{}),
	}

	if err := manager.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	wantActions := "[dispatch-cancel dispatch-observed-cancel unsubscribe cleanup]"
	if got := fmt.Sprint(recorder.snapshot()); got != wantActions {
		t.Errorf("Close actions = %s, want %s", got, wantActions)
	}
}

func TestConsumerManagerShutdownStopsAllProducersBeforeWaiting(t *testing.T) {
	ctx := logging.WithLogger(context.Background(), zap.NewNop().Sugar())
	firstDone := make(chan struct{})
	secondDone := make(chan struct{})
	firstCanceled := make(chan struct{})
	secondCanceled := make(chan struct{})
	newSubscription := func(done chan struct{}, canceled chan struct{}) *TriggerSubscription {
		return &TriggerSubscription{
			subscription: &shutdownPullSubscription{
				recorder:     &lifecycleRecorder{},
				unsubscribed: make(chan struct{}),
			},
			handler: &TriggerHandler{},
			done:    done,
			cancel:  func() { close(canceled) },
		}
	}
	subscriptions := []*TriggerSubscription{
		newSubscription(firstDone, firstCanceled),
		newSubscription(secondDone, secondCanceled),
	}
	manager := &ConsumerManager{logger: logging.FromContext(ctx)}
	result := make(chan error, 1)
	go func() { result <- manager.shutdown(context.Background(), subscriptions, false) }()

	select {
	case <-firstCanceled:
	case <-time.After(5 * time.Second):
		t.Fatal("first fetch producer was not canceled")
	}
	select {
	case <-secondCanceled:
	case <-time.After(5 * time.Second):
		t.Fatal("second fetch producer was not canceled before waiting for first done")
	}
	close(firstDone)
	close(secondDone)
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("shutdown() error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown did not complete after producers stopped")
	}
}

func TestConsumerManagerShutdownRejectsNewLifecycleOperations(t *testing.T) {
	ctx := logging.WithLogger(context.Background(), zap.NewNop().Sugar())
	manager := &ConsumerManager{
		logger:        logging.FromContext(ctx),
		subscriptions: make(map[string]*TriggerSubscription),
		shutdownDone:  make(chan struct{}),
	}
	if err := manager.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	if err := manager.UnsubscribeTrigger("missing"); err != ErrConsumerManagerClosed {
		t.Errorf("UnsubscribeTrigger after shutdown = %v, want %v", err, ErrConsumerManagerClosed)
	}
	if err := manager.SubscribeTrigger(nil, nil, duckv1.Addressable{}, nil, nil, nil, nil); err != ErrConsumerManagerClosed {
		t.Errorf("SubscribeTrigger after shutdown = %v, want %v", err, ErrConsumerManagerClosed)
	}
}

func TestDefaultMaxConcurrency(t *testing.T) {
	if DefaultMaxConcurrency != 20 {
		t.Errorf("DefaultMaxConcurrency = %v, want 20", DefaultMaxConcurrency)
	}
}

func TestAnnotationConstants(t *testing.T) {
	tests := []struct {
		name string
		got  string
		want string
	}{
		{
			name: "TriggerMaxConcurrencyAnnotation",
			got:  TriggerMaxConcurrencyAnnotation,
			want: "natsjetstream.eventing.knative.dev/max-concurrency",
		},
		{
			name: "TriggerFetchBatchSizeAnnotation",
			got:  TriggerFetchBatchSizeAnnotation,
			want: "natsjetstream.eventing.knative.dev/fetch-batch-size",
		},
		{
			name: "TriggerFetchTimeoutAnnotation",
			got:  TriggerFetchTimeoutAnnotation,
			want: "natsjetstream.eventing.knative.dev/fetch-timeout",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Errorf("%s = %q, want %q", tt.name, tt.got, tt.want)
			}
		})
	}
}

func TestConsumerManagerConfig_MaxConcurrency(t *testing.T) {
	tests := []struct {
		name               string
		config             *ConsumerManagerConfig
		wantMaxConcurrency int
	}{
		{
			name:               "nil config uses default",
			config:             nil,
			wantMaxConcurrency: DefaultMaxConcurrency,
		},
		{
			name:               "zero MaxConcurrency uses default",
			config:             &ConsumerManagerConfig{MaxConcurrency: 0},
			wantMaxConcurrency: DefaultMaxConcurrency,
		},
		{
			name:               "positive MaxConcurrency is used",
			config:             &ConsumerManagerConfig{MaxConcurrency: 50},
			wantMaxConcurrency: 50,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			maxConcurrency := DefaultMaxConcurrency

			if tt.config != nil {
				if tt.config.MaxConcurrency > 0 {
					maxConcurrency = tt.config.MaxConcurrency
				}
			}

			if maxConcurrency != tt.wantMaxConcurrency {
				t.Errorf("maxConcurrency = %v, want %v", maxConcurrency, tt.wantMaxConcurrency)
			}
		})
	}
}

func TestParseTriggerAnnotationInt(t *testing.T) {
	logger := zap.NewNop().Sugar()

	tests := []struct {
		name        string
		annotations map[string]string
		key         string
		defaultVal  int
		want        int
	}{
		{
			name:        "absent key returns default",
			annotations: map[string]string{},
			key:         "some-key",
			defaultVal:  10,
			want:        10,
		},
		{
			name:        "empty string returns default",
			annotations: map[string]string{"some-key": ""},
			key:         "some-key",
			defaultVal:  10,
			want:        10,
		},
		{
			name:        "valid positive int is parsed",
			annotations: map[string]string{"some-key": "42"},
			key:         "some-key",
			defaultVal:  10,
			want:        42,
		},
		{
			name:        "zero returns default",
			annotations: map[string]string{"some-key": "0"},
			key:         "some-key",
			defaultVal:  10,
			want:        10,
		},
		{
			name:        "negative returns default",
			annotations: map[string]string{"some-key": "-5"},
			key:         "some-key",
			defaultVal:  10,
			want:        10,
		},
		{
			name:        "non-numeric returns default",
			annotations: map[string]string{"some-key": "abc"},
			key:         "some-key",
			defaultVal:  10,
			want:        10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseTriggerAnnotationInt(tt.annotations, tt.key, tt.defaultVal, logger)
			if got != tt.want {
				t.Errorf("parseTriggerAnnotationInt() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseTriggerAnnotationDuration(t *testing.T) {
	logger := zap.NewNop().Sugar()

	tests := []struct {
		name        string
		annotations map[string]string
		key         string
		defaultVal  time.Duration
		want        time.Duration
	}{
		{
			name:        "absent key returns default",
			annotations: map[string]string{},
			key:         "some-key",
			defaultVal:  200 * time.Millisecond,
			want:        200 * time.Millisecond,
		},
		{
			name:        "empty string returns default",
			annotations: map[string]string{"some-key": ""},
			key:         "some-key",
			defaultVal:  200 * time.Millisecond,
			want:        200 * time.Millisecond,
		},
		{
			name:        "valid duration is parsed",
			annotations: map[string]string{"some-key": "500ms"},
			key:         "some-key",
			defaultVal:  200 * time.Millisecond,
			want:        500 * time.Millisecond,
		},
		{
			name:        "zero duration returns default",
			annotations: map[string]string{"some-key": "0s"},
			key:         "some-key",
			defaultVal:  200 * time.Millisecond,
			want:        200 * time.Millisecond,
		},
		{
			name:        "negative duration returns default",
			annotations: map[string]string{"some-key": "-1s"},
			key:         "some-key",
			defaultVal:  200 * time.Millisecond,
			want:        200 * time.Millisecond,
		},
		{
			name:        "non-duration string returns default",
			annotations: map[string]string{"some-key": "abc"},
			key:         "some-key",
			defaultVal:  200 * time.Millisecond,
			want:        200 * time.Millisecond,
		},
		{
			name:        "nil annotations map returns default without panic",
			annotations: nil,
			key:         "some-key",
			defaultVal:  200 * time.Millisecond,
			want:        200 * time.Millisecond,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseTriggerAnnotationDuration(tt.annotations, tt.key, tt.defaultVal, logger)
			if got != tt.want {
				t.Errorf("parseTriggerAnnotationDuration() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDynamicBatchSizeCapping(t *testing.T) {
	tests := []struct {
		name           string
		capacity       int
		occupied       int
		fetchBatchSize int
		wantBatchSize  int
	}{
		{
			name:           "all free and batchSize fits within capacity",
			capacity:       20,
			occupied:       0,
			fetchBatchSize: 10,
			wantBatchSize:  10,
		},
		{
			name:           "all free but batchSize exceeds capacity",
			capacity:       5,
			occupied:       0,
			fetchBatchSize: 10,
			wantBatchSize:  5,
		},
		{
			name:           "partially occupied and batchSize fits within available",
			capacity:       20,
			occupied:       5,
			fetchBatchSize: 10,
			wantBatchSize:  10,
		},
		{
			name:           "partially occupied and batchSize exceeds available",
			capacity:       20,
			occupied:       15,
			fetchBatchSize: 10,
			wantBatchSize:  5,
		},
		{
			name:           "one slot free",
			capacity:       20,
			occupied:       19,
			fetchBatchSize: 10,
			wantBatchSize:  1,
		},
		{
			name:           "all occupied returns zero",
			capacity:       20,
			occupied:       20,
			fetchBatchSize: 10,
			wantBatchSize:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sem := make(chan struct{}, tt.capacity)
			for i := 0; i < tt.occupied; i++ {
				sem <- struct{}{}
			}

			available := cap(sem) - len(sem)
			batchSize := tt.fetchBatchSize
			if available < batchSize {
				batchSize = available
			}

			if batchSize != tt.wantBatchSize {
				t.Errorf("batchSize = %v, want %v (available=%v, fetchBatchSize=%v)",
					batchSize, tt.wantBatchSize, available, tt.fetchBatchSize)
			}
		})
	}
}
