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

package utils

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"knative.dev/eventing/pkg/kncloudevents"
)

const defaultDeadLetterTimeout = 30 * time.Second

type deliveryContextKey struct{}

type deliveryContext struct {
	lifecycle context.Context
	ackWait   time.Duration
}

// WithDeliveryContext preserves the consumer lifetime and AckWait before a
// subscriber request gets its own deadline. Repeated calls retain that parent.
func WithDeliveryContext(ctx context.Context, ackWait time.Duration) context.Context {
	if _, ok := ctx.Value(deliveryContextKey{}).(deliveryContext); ok {
		return ctx
	}
	return context.WithValue(ctx, deliveryContextKey{}, deliveryContext{ctx, ackWait})
}

// DeadLetterRetryConfig gives the dead letter sink an independent retry budget
// without changing the shared subscriber configuration.
func DeadLetterRetryConfig(config *kncloudevents.RetryConfig) kncloudevents.RetryConfig {
	result := kncloudevents.NoRetries()
	if config != nil {
		result = *config
	}
	result.CheckRetry = kncloudevents.SelectiveRetry
	if result.Backoff == nil {
		result.Backoff = kncloudevents.NoRetries().Backoff
	}
	if result.RequestTimeout <= 0 {
		result.RequestTimeout = defaultDeadLetterTimeout
	}
	return result
}

// StartDeadLetter keeps a message pending throughout the dead letter retries.
// Its context retains tracing independently of the subscriber's AckWait guard.
// Consumer shutdown allows one AckWait for in-flight work to finish; an explicit
// caller deadline still stops it immediately. stop must run before the handler exits.
func StartDeadLetter(ctx context.Context, msg *nats.Msg) (context.Context, func(), error) {
	delivery, ok := ctx.Value(deliveryContextKey{}).(deliveryContext)
	if !ok {
		delivery = deliveryContext{ctx, defaultDeadLetterTimeout}
	}
	if err := delivery.lifecycle.Err(); err != nil {
		return nil, nil, err
	}
	if delivery.ackWait <= 0 {
		delivery.ackWait = defaultDeadLetterTimeout
	}

	dlsCtx, cancel := context.WithCancelCause(context.WithoutCancel(ctx))
	// InProgress is asynchronous: the server does not acknowledge progress
	// notifications, so passing nats.Context would wait for a reply forever.
	if err := msg.InProgress(); err != nil {
		cancel(err)
		return nil, nil, err
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(max(delivery.ackWait/3, time.Nanosecond))
		defer ticker.Stop()
		lifecycleDone := delivery.lifecycle.Done()
		var shutdownTimer *time.Timer
		var shutdownDone <-chan time.Time
		defer func() {
			if shutdownTimer != nil {
				shutdownTimer.Stop()
			}
		}()
		for {
			select {
			case <-dlsCtx.Done():
				return
			case <-lifecycleDone:
				if delivery.lifecycle.Err() == context.DeadlineExceeded {
					cancel(context.Cause(delivery.lifecycle))
					return
				}
				lifecycleDone = nil
				shutdownTimer = time.NewTimer(delivery.ackWait)
				shutdownDone = shutdownTimer.C
			case <-shutdownDone:
				cancel(context.Cause(delivery.lifecycle))
				return
			case <-ticker.C:
				if err := msg.InProgress(); err != nil {
					cancel(fmt.Errorf("extend AckWait during dead letter delivery: %w", err))
					return
				}
			}
		}
	}()
	return dlsCtx, func() {
		cancel(nil)
		<-done
	}, nil
}
