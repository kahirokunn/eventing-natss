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
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

const (
	// DefaultShutdownTimeout leaves five seconds of the Pod termination grace
	// period for kubelet and process-level cleanup.
	DefaultShutdownTimeout = 40 * time.Second
	natsDrainReserve       = 5 * time.Second
)

type consumerShutdowner interface {
	Shutdown(context.Context) error
}

type natsConnection interface {
	Status() nats.Status
	Drain() error
	Close()
}

type runtimeState uint8

const (
	runtimeStarting runtimeState = iota
	runtimeRunning
	runtimeStopping
	runtimeStopped
)

// Runtime owns the filter's ConsumerManager and NATS connection so process
// shutdown and readiness reflect the data plane rather than only the controller
// work queue.
type Runtime struct {
	signalCtx context.Context

	mu       sync.RWMutex
	state    runtimeState
	consumer consumerShutdowner
	conn     natsConnection
	err      error

	attached chan struct{}
	attach   sync.Once
	shutdown sync.Once
	done     chan struct{}
}

func NewRuntime(signalCtx context.Context) *Runtime {
	return &Runtime{
		signalCtx: signalCtx,
		state:     runtimeStarting,
		attached:  make(chan struct{}),
		done:      make(chan struct{}),
	}
}

// Attach transfers ownership of the consumer manager and NATS connection to
// the Runtime. A shutdown that arrived during controller construction waits for
// this handoff rather than leaking resources created after the signal.
func (r *Runtime) Attach(consumer consumerShutdowner, conn natsConnection) {
	r.attach.Do(func() {
		r.mu.Lock()
		r.consumer = consumer
		r.conn = conn
		if r.state == runtimeStarting {
			r.state = runtimeRunning
		}
		r.mu.Unlock()
		close(r.attached)
	})
}

// ReadinessHandler reports ready only while the runtime is accepting work and
// the NATS connection is fully connected. Draining connections are not ready.
func (r *Runtime) ReadinessHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		select {
		case <-r.signalCtx.Done():
			http.Error(w, "filter is shutting down", http.StatusServiceUnavailable)
			return
		default:
		}

		r.mu.RLock()
		state, conn := r.state, r.conn
		r.mu.RUnlock()
		if state != runtimeRunning || conn == nil {
			http.Error(w, "filter runtime is not running", http.StatusServiceUnavailable)
			return
		}
		if status := conn.Status(); status != nats.CONNECTED {
			http.Error(w, fmt.Sprintf("NATS connection is %s", status), http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}
}

// LivenessHandler keeps the process alive during recoverable NATS reconnects,
// but asks kubelet to restart a terminally closed connection. It also preserves
// sharedmain's default behavior of failing once SIGTERM is received.
func (r *Runtime) LivenessHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		select {
		case <-r.signalCtx.Done():
			http.Error(w, "filter is shutting down", http.StatusInternalServerError)
			return
		default:
		}

		r.mu.RLock()
		conn := r.conn
		r.mu.RUnlock()
		if conn != nil && conn.Status() == nats.CLOSED {
			http.Error(w, "NATS connection is closed", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}
}

// Shutdown is idempotent. The first caller starts shutdown; all callers wait
// for that same result or their own context deadline.
func (r *Runtime) Shutdown(ctx context.Context) error {
	r.shutdown.Do(func() {
		r.mu.Lock()
		r.state = runtimeStopping
		r.mu.Unlock()
		go r.runShutdown(ctx)
	})

	select {
	case <-r.done:
		r.mu.RLock()
		defer r.mu.RUnlock()
		return r.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *Runtime) runShutdown(ctx context.Context) {
	defer close(r.done)

	// Wait even if the initiating caller's context expires. If shutdown races
	// controller construction, a later Attach must still close the resources;
	// it will receive the already-canceled context and take the forced path.
	<-r.attached

	r.mu.RLock()
	consumer, conn := r.consumer, r.conn
	r.mu.RUnlock()

	consumerCtx, cancelConsumer := reserveDeadline(ctx, natsDrainReserve)
	consumerErr := consumer.Shutdown(consumerCtx)
	cancelConsumer()

	drainErr := drainNATS(ctx, conn)
	r.finish(errors.Join(consumerErr, drainErr))
}

func (r *Runtime) finish(err error) {
	r.mu.Lock()
	r.err = err
	r.state = runtimeStopped
	r.mu.Unlock()
}

func reserveDeadline(ctx context.Context, reserve time.Duration) (context.Context, context.CancelFunc) {
	deadline, ok := ctx.Deadline()
	if !ok {
		return context.WithCancel(ctx)
	}
	consumerDeadline := deadline.Add(-reserve)
	if consumerDeadline.Before(time.Now()) {
		consumerDeadline = time.Now()
	}
	return context.WithDeadline(ctx, consumerDeadline)
}

func drainNATS(ctx context.Context, conn natsConnection) error {
	if conn == nil {
		return nil
	}
	if conn.Status() == nats.CLOSED {
		return nil
	}
	if err := conn.Drain(); err != nil {
		if errors.Is(err, nats.ErrConnectionClosed) {
			return nil
		}
		conn.Close()
		return err
	}

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if conn.Status() == nats.CLOSED {
			return nil
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			conn.Close()
			return ctx.Err()
		}
	}
}

// Done is closed after consumer and NATS shutdown have completed or timed out.
func (r *Runtime) Done() <-chan struct{} {
	return r.done
}
