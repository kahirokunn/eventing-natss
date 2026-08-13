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
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

type consumerShutdownFunc func(context.Context) error

func (f consumerShutdownFunc) Shutdown(ctx context.Context) error {
	return f(ctx)
}

type runtimeActionRecorder struct {
	mu      sync.Mutex
	actions []string
}

func (r *runtimeActionRecorder) record(action string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.actions = append(r.actions, action)
}

func (r *runtimeActionRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.actions...)
}

type runtimeNATSConnection struct {
	mu           sync.RWMutex
	status       nats.Status
	recorder     *runtimeActionRecorder
	drainErr     error
	closeOnDrain bool
	drainCalls   atomic.Int32
	closeCalls   atomic.Int32
}

func (c *runtimeNATSConnection) Status() nats.Status {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.status
}

func (c *runtimeNATSConnection) setStatus(status nats.Status) {
	c.mu.Lock()
	c.status = status
	c.mu.Unlock()
}

func (c *runtimeNATSConnection) Drain() error {
	c.drainCalls.Add(1)
	if c.recorder != nil {
		c.recorder.record("nats-drain")
	}
	if c.drainErr == nil {
		c.setStatus(nats.DRAINING_SUBS)
		if c.closeOnDrain {
			c.setStatus(nats.CLOSED)
		}
	}
	return c.drainErr
}

func (c *runtimeNATSConnection) Close() {
	c.closeCalls.Add(1)
	if c.recorder != nil {
		c.recorder.record("nats-close")
	}
	c.setStatus(nats.CLOSED)
}

func runtimeProbeStatus(handler http.HandlerFunc) int {
	recorder := httptest.NewRecorder()
	handler(recorder, httptest.NewRequest(http.MethodGet, "/", nil))
	return recorder.Code
}

func TestRuntimeReadinessReflectsNATSState(t *testing.T) {
	signalCtx, cancelSignal := context.WithCancel(context.Background())
	defer cancelSignal()
	runtime := NewRuntime(signalCtx)
	if got := runtimeProbeStatus(runtime.ReadinessHandler()); got != http.StatusServiceUnavailable {
		t.Fatalf("starting runtime readiness = %d, want %d", got, http.StatusServiceUnavailable)
	}

	conn := &runtimeNATSConnection{status: nats.CONNECTED}
	runtime.Attach(consumerShutdownFunc(func(context.Context) error { return nil }), conn)
	for _, test := range []struct {
		status nats.Status
		want   int
	}{
		{status: nats.CONNECTED, want: http.StatusOK},
		{status: nats.CONNECTING, want: http.StatusServiceUnavailable},
		{status: nats.DISCONNECTED, want: http.StatusServiceUnavailable},
		{status: nats.RECONNECTING, want: http.StatusServiceUnavailable},
		{status: nats.DRAINING_SUBS, want: http.StatusServiceUnavailable},
		{status: nats.DRAINING_PUBS, want: http.StatusServiceUnavailable},
		{status: nats.CLOSED, want: http.StatusServiceUnavailable},
	} {
		t.Run(test.status.String(), func(t *testing.T) {
			conn.setStatus(test.status)
			if got := runtimeProbeStatus(runtime.ReadinessHandler()); got != test.want {
				t.Errorf("readiness for NATS %s = %d, want %d", test.status, got, test.want)
			}
		})
	}

	conn.setStatus(nats.CONNECTED)
	cancelSignal()
	if got := runtimeProbeStatus(runtime.ReadinessHandler()); got != http.StatusServiceUnavailable {
		t.Errorf("shutting-down runtime readiness = %d, want %d", got, http.StatusServiceUnavailable)
	}
}

func TestRuntimeLivenessAllowsReconnectButRejectsClosedAndShutdown(t *testing.T) {
	signalCtx, cancelSignal := context.WithCancel(context.Background())
	runtime := NewRuntime(signalCtx)
	conn := &runtimeNATSConnection{status: nats.RECONNECTING}
	runtime.Attach(consumerShutdownFunc(func(context.Context) error { return nil }), conn)

	if got := runtimeProbeStatus(runtime.LivenessHandler()); got != http.StatusOK {
		t.Errorf("reconnecting liveness = %d, want %d", got, http.StatusOK)
	}
	conn.setStatus(nats.CLOSED)
	if got := runtimeProbeStatus(runtime.LivenessHandler()); got != http.StatusInternalServerError {
		t.Errorf("closed liveness = %d, want %d", got, http.StatusInternalServerError)
	}
	conn.setStatus(nats.CONNECTED)
	cancelSignal()
	if got := runtimeProbeStatus(runtime.LivenessHandler()); got != http.StatusInternalServerError {
		t.Errorf("shutdown liveness = %d, want %d", got, http.StatusInternalServerError)
	}
}

func TestRuntimeShutdownOrderAndReadiness(t *testing.T) {
	recorder := &runtimeActionRecorder{}
	consumerStarted := make(chan struct{})
	releaseConsumer := make(chan struct{})
	var consumerCalls atomic.Int32
	consumer := consumerShutdownFunc(func(context.Context) error {
		consumerCalls.Add(1)
		recorder.record("consumer-shutdown")
		close(consumerStarted)
		<-releaseConsumer
		recorder.record("consumer-done")
		return nil
	})
	conn := &runtimeNATSConnection{
		status:       nats.CONNECTED,
		recorder:     recorder,
		closeOnDrain: true,
	}
	runtime := NewRuntime(context.Background())
	runtime.Attach(consumer, conn)

	shutdownResult := make(chan error, 1)
	go func() { shutdownResult <- runtime.Shutdown(context.Background()) }()
	select {
	case <-consumerStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("consumer shutdown did not start")
	}
	if got := runtimeProbeStatus(runtime.ReadinessHandler()); got != http.StatusServiceUnavailable {
		t.Errorf("readiness during shutdown = %d, want %d", got, http.StatusServiceUnavailable)
	}
	if got := conn.drainCalls.Load(); got != 0 {
		t.Fatalf("NATS Drain called %d times before consumer shutdown completed", got)
	}

	close(releaseConsumer)
	select {
	case err := <-shutdownResult:
		if err != nil {
			t.Fatalf("Shutdown() error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Shutdown did not complete")
	}
	select {
	case <-runtime.Done():
	default:
		t.Fatal("Done was not closed when Shutdown returned")
	}
	if got := consumerCalls.Load(); got != 1 {
		t.Errorf("consumer Shutdown calls = %d, want 1", got)
	}
	if got := conn.drainCalls.Load(); got != 1 {
		t.Errorf("NATS Drain calls = %d, want 1", got)
	}
	if got := conn.closeCalls.Load(); got != 0 {
		t.Errorf("NATS Close calls = %d, want 0 after successful drain", got)
	}
	want := "[consumer-shutdown consumer-done nats-drain]"
	if got := fmt.Sprint(recorder.snapshot()); got != want {
		t.Errorf("shutdown actions = %s, want %s", got, want)
	}
}

func TestRuntimeShutdownIsConcurrentAndIdempotent(t *testing.T) {
	var consumerCalls atomic.Int32
	consumer := consumerShutdownFunc(func(context.Context) error {
		consumerCalls.Add(1)
		return nil
	})
	conn := &runtimeNATSConnection{status: nats.CONNECTED, closeOnDrain: true}
	runtime := NewRuntime(context.Background())
	runtime.Attach(consumer, conn)

	const callers = 20
	start := make(chan struct{})
	results := make(chan error, callers)
	var callersDone sync.WaitGroup
	callersDone.Add(callers)
	for range callers {
		go func() {
			defer callersDone.Done()
			<-start
			results <- runtime.Shutdown(context.Background())
		}()
	}
	close(start)
	callersDone.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Errorf("Shutdown() error = %v", err)
		}
	}
	if got := consumerCalls.Load(); got != 1 {
		t.Errorf("consumer Shutdown calls = %d, want 1", got)
	}
	if got := conn.drainCalls.Load(); got != 1 {
		t.Errorf("NATS Drain calls = %d, want 1", got)
	}
	if err := runtime.Shutdown(context.Background()); err != nil {
		t.Errorf("repeated Shutdown() error = %v", err)
	}
	if got := consumerCalls.Load(); got != 1 {
		t.Errorf("consumer Shutdown calls after repeat = %d, want 1", got)
	}
}

func TestRuntimeShutdownJoinsErrorsAndClosesAfterDrainFailure(t *testing.T) {
	consumerErr := errors.New("consumer shutdown failed")
	drainErr := errors.New("NATS drain failed")
	recorder := &runtimeActionRecorder{}
	consumer := consumerShutdownFunc(func(context.Context) error {
		recorder.record("consumer-shutdown")
		return consumerErr
	})
	conn := &runtimeNATSConnection{
		status:   nats.CONNECTED,
		recorder: recorder,
		drainErr: drainErr,
	}
	runtime := NewRuntime(context.Background())
	runtime.Attach(consumer, conn)

	err := runtime.Shutdown(context.Background())
	if !errors.Is(err, consumerErr) || !errors.Is(err, drainErr) {
		t.Fatalf("Shutdown() error = %v, want joined consumer and drain errors", err)
	}
	if got := conn.closeCalls.Load(); got != 1 {
		t.Errorf("NATS Close calls = %d, want 1", got)
	}
	want := "[consumer-shutdown nats-drain nats-close]"
	if got := fmt.Sprint(recorder.snapshot()); got != want {
		t.Errorf("shutdown actions = %s, want %s", got, want)
	}
}

func TestRuntimeShutdownDeadlineForcesNATSClose(t *testing.T) {
	if DefaultShutdownTimeout != 40*time.Second {
		t.Fatalf("DefaultShutdownTimeout = %v, want 40s", DefaultShutdownTimeout)
	}
	consumer := consumerShutdownFunc(func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	})
	conn := &runtimeNATSConnection{status: nats.CONNECTED}
	runtime := NewRuntime(context.Background())
	runtime.Attach(consumer, conn)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := runtime.Shutdown(shutdownCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("caller Shutdown() error = %v, want its context deadline", err)
	}
	select {
	case <-runtime.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("runtime did not finish after shutdown deadline")
	}
	err := runtime.Shutdown(context.Background())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("completed Shutdown() error = %v, want context deadline exceeded", err)
	}
	if got := conn.drainCalls.Load(); got != 1 {
		t.Errorf("NATS Drain calls = %d, want 1", got)
	}
	if got := conn.closeCalls.Load(); got != 1 {
		t.Errorf("NATS Close calls = %d, want 1 after deadline", got)
	}
}

func TestRuntimeShutdownBeforeAttachStillCleansLateResources(t *testing.T) {
	runtime := NewRuntime(context.Background())
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	err := runtime.Shutdown(shutdownCtx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown() before Attach error = %v, want context deadline exceeded", err)
	}

	consumerCalled := make(chan struct{})
	consumer := consumerShutdownFunc(func(context.Context) error {
		close(consumerCalled)
		return nil
	})
	conn := &runtimeNATSConnection{status: nats.CONNECTED, closeOnDrain: true}
	runtime.Attach(consumer, conn)

	select {
	case <-consumerCalled:
	case <-time.After(time.Second):
		t.Fatal("late-attached consumer was not shut down after shutdown had already started")
	}
	select {
	case <-runtime.Done():
	case <-time.After(time.Second):
		t.Fatal("late-attached resources did not finish shutdown")
	}
	if got := conn.drainCalls.Load(); got != 1 {
		t.Errorf("late-attached NATS Drain calls = %d, want 1", got)
	}
}
