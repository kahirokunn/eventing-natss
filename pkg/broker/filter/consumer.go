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
	"math/rand/v2"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	duckv1 "knative.dev/pkg/apis/duck/v1"
	"knative.dev/pkg/logging"

	eventingv1 "knative.dev/eventing/pkg/apis/eventing/v1"
	"knative.dev/eventing/pkg/eventingtls"
	"knative.dev/eventing/pkg/kncloudevents"

	brokeroidc "knative.dev/eventing-natss/pkg/broker/oidc"
	brokerutils "knative.dev/eventing-natss/pkg/broker/utils"
)

// otelScope is the OTel instrumentation scope used for the broker filter's
// tracer and meter. Channel parity: dispatch.duration histogram is named the
// same as the channel's (kn.eventing.dispatch.duration) so they aggregate.
const otelScope = "knative.dev/eventing-natss/pkg/broker/filter"

// latencyBounds are the explicit histogram bucket boundaries (in seconds) for
// the dispatch.duration metric. They must match upstream eventing's channel
// dispatcher (pkg/channel/fanout) so the shared kn.eventing.dispatch.duration
// metric aggregates cleanly across channels and brokers.
var latencyBounds = []float64{0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10}

const (
	// DefaultFetchBatchSize is the default number of messages to fetch in each batch
	DefaultFetchBatchSize = 10
	// DefaultFetchTimeout is the default timeout for fetching messages
	DefaultFetchTimeout = 200 * time.Millisecond
	// DefaultMaxConcurrency is the default per-trigger maximum number of messages
	// dispatched concurrently. Should be >= FetchBatchSize so a full batch always
	// has available slots and is never left fetched-but-unprocessed with its
	// AckWait ticking. Individual triggers can override this via the
	// TriggerMaxConcurrencyAnnotation annotation.
	DefaultMaxConcurrency = 20

	// TriggerMaxConcurrencyAnnotation is the annotation key on a Trigger that
	// overrides the per-trigger dispatch concurrency limit. Must be a positive
	// integer; absent or invalid values fall back to DefaultMaxConcurrency (or
	// the value set via CONSUMER_MAX_CONCURRENCY on the filter deployment).
	TriggerMaxConcurrencyAnnotation = "natsjetstream.eventing.knative.dev/max-concurrency"

	// TriggerFetchBatchSizeAnnotation is the annotation key on a Trigger that
	// overrides the number of messages fetched from JetStream in each pull
	// request. Must be a positive integer; absent or invalid values fall back
	// to DefaultFetchBatchSize (or CONSUMER_FETCH_BATCH_SIZE on the filter
	// deployment).
	TriggerFetchBatchSizeAnnotation = "natsjetstream.eventing.knative.dev/fetch-batch-size"

	// TriggerFetchTimeoutAnnotation is the annotation key on a Trigger that
	// overrides how long a fetch request waits for messages before returning
	// empty. Must be a valid Go duration string (e.g. "500ms", "1s"); absent
	// or invalid values fall back to DefaultFetchTimeout (or
	// CONSUMER_FETCH_TIMEOUT on the filter deployment).
	TriggerFetchTimeoutAnnotation = "natsjetstream.eventing.knative.dev/fetch-timeout"

	forcedDispatchGracePeriod = 2 * time.Second
)

var ErrConsumerManagerClosed = errors.New("consumer manager is shutting down")

// ConsumerManagerConfig holds configuration for the ConsumerManager
type ConsumerManagerConfig struct {
	// StreamName is the only Broker stream this per-Broker filter may consume.
	StreamName string

	// FetchBatchSize is the number of messages to fetch in each batch.
	// Defaults to DefaultFetchBatchSize if not set.
	FetchBatchSize int

	// FetchTimeout is the timeout for fetching messages.
	// Defaults to DefaultFetchTimeout if not set.
	FetchTimeout time.Duration

	// MaxConcurrency is the default per-trigger maximum number of messages
	// dispatched concurrently. Individual triggers can override this via the
	// TriggerMaxConcurrencyAnnotation annotation.
	// Defaults to DefaultMaxConcurrency if not set.
	MaxConcurrency int

	// AudienceTokenSource returns a token for a resolved destination audience.
	// The production source uses the exact-name TokenRequest permission granted
	// to this Broker's operational filter identity.
	AudienceTokenSource func(context.Context, string) (string, error)
}

// ConsumerManager manages JetStream consumer subscriptions for triggers
type ConsumerManager struct {
	logger          *zap.SugaredLogger
	ctx             context.Context
	cancel          context.CancelFunc
	recoveryCtx     context.Context
	recoveryCancel  context.CancelFunc
	recoveryPending sync.Map
	recoverySignal  chan struct{}
	recoveryDone    chan struct{}
	recoveryWorkers sync.WaitGroup

	js   nats.JetStreamContext
	conn *nats.Conn

	// Configuration
	fetchBatchSize        int
	fetchTimeout          time.Duration
	defaultMaxConcurrency int
	streamName            string

	// Event dispatcher
	dispatcher *kncloudevents.Dispatcher

	// Observability instruments. Resolved from the global OTel providers in
	// NewConsumerManager; passed to each TriggerHandler. The inflight gauge
	// is registered once and walks m.subscriptions on each collection cycle.
	tracer           trace.Tracer
	dispatchDuration metric.Float64Histogram
	processDuration  metric.Float64Histogram
	tokenSource      audienceTokenSource

	// Map of trigger UID to subscription
	subscriptions                map[string]*TriggerSubscription
	readinessMu                  sync.RWMutex
	tokenFailures                map[string]map[string]struct{}
	tokenValidationGenerations   map[string]int64
	tokenRequirements            func() ([]tokenReadinessRequirement, bool)
	audienceConfigurationInvalid bool
	mu                           sync.RWMutex
	closing                      bool
	shutdownDone                 chan struct{}
	shutdownErr                  error
}

type tokenReadinessRequirement struct {
	triggerUID    string
	generation    int64
	resolved      bool
	authenticated bool
}

type tokenRecoveryKey struct {
	triggerUID string
	audience   string
}

type pullSubscription interface {
	Fetch(ctx context.Context, batch int) ([]*nats.Msg, error)
	Unsubscribe() error
}

type natsPullSubscription struct {
	*nats.Subscription
}

func (s natsPullSubscription) Fetch(ctx context.Context, batch int) ([]*nats.Msg, error) {
	return s.Subscription.Fetch(batch, nats.Context(ctx))
}

// TriggerSubscription holds the subscription and handler for a trigger
type TriggerSubscription struct {
	trigger        *eventingv1.Trigger
	subscription   pullSubscription
	handler        *TriggerHandler
	streamName     string
	consumerName   string
	ackWait        time.Duration
	fetchBatchSize int
	fetchTimeout   time.Duration
	maxConcurrency int
	// sem is a per-trigger counting semaphore. A slot is acquired before
	// spawning each dispatch goroutine and released when it exits, bounding
	// the number of concurrent in-flight HTTP calls for this trigger and
	// providing backpressure to its fetch loop. Replaced wholesale when
	// max-concurrency changes; old in-flight goroutines keep releasing into
	// the channel they captured.
	sem chan struct{}
	// dispatchCtx parents the per-message context used for each in-flight
	// HTTP call. It lives for the subscription's full lifetime so that
	// restarting the fetch loop on an annotation change does not cancel
	// in-progress dispatches.
	dispatchCtx    context.Context
	dispatchCancel context.CancelFunc
	// cancel stops the current fetch loop only. A new fetch loop with new
	// parameters can be started after done closes.
	cancel context.CancelFunc
	// done is closed by the current fetch loop as soon as it returns.
	// Restart waits on this before starting a new fetch loop on the same
	// pull subscription.
	done chan struct{}
	// inflight tracks every dispatch goroutine spawned by any fetch loop
	// for this subscription. unsubscribeLocked waits on it so the NATS
	// subscription and trigger handler are not torn down while a dispatch
	// goroutine is still using them (msg.Ack, h.filter.Filter, etc.).
	inflight            sync.WaitGroup
	tokenPaused         bool
	configurationPaused bool
}

// NewConsumerManager creates a new consumer manager
func NewConsumerManager(ctx context.Context, conn *nats.Conn, js nats.JetStreamContext, config *ConsumerManagerConfig) *ConsumerManager {
	logger := logging.FromContext(ctx)
	runCtx, runCancel := context.WithCancel(ctx)
	recoveryCtx, recoveryCancel := context.WithCancel(runCtx)

	// Destination tokens are attached by TriggerHandler so subscriber and DLS
	// failures remain distinguishable from ordinary HTTP delivery failures.
	dispatcher := kncloudevents.NewDispatcher(eventingtls.ClientConfig{}, nil)

	// Apply defaults
	fetchBatchSize := DefaultFetchBatchSize
	fetchTimeout := DefaultFetchTimeout
	maxConcurrency := DefaultMaxConcurrency
	streamName := ""
	var tokenSource audienceTokenSource

	if config != nil {
		streamName = config.StreamName
		if config.FetchBatchSize > 0 {
			fetchBatchSize = config.FetchBatchSize
		}
		if config.FetchTimeout > 0 {
			fetchTimeout = config.FetchTimeout
		}
		if config.MaxConcurrency > 0 {
			maxConcurrency = config.MaxConcurrency
		}
		tokenSource = config.AudienceTokenSource
	}

	// Resolve tracer + meter from the global OTel providers. When no real
	// provider is registered these are no-ops, so this is safe to call
	// unconditionally regardless of how the host wires observability.
	tracer := otel.GetTracerProvider().Tracer(otelScope)
	meter := otel.GetMeterProvider().Meter(otelScope)
	dispatchDuration, err := meter.Float64Histogram(
		"kn.eventing.dispatch.duration",
		metric.WithDescription("The duration to dispatch the event"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(latencyBounds...),
	)
	if err != nil {
		logger.Warnw("failed to create dispatch duration histogram; metric will be skipped", zap.Error(err))
		dispatchDuration = nil
	}

	processDuration, err := meter.Float64Histogram(
		"kn.eventing.broker.filter.process.duration",
		metric.WithDescription("The duration of pre-dispatch processing (message decode and filter evaluation)"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(latencyBounds...),
	)
	if err != nil {
		logger.Warnw("failed to create process duration histogram; metric will be skipped", zap.Error(err))
		processDuration = nil
	}

	cm := &ConsumerManager{
		logger:                     logger,
		ctx:                        runCtx,
		cancel:                     runCancel,
		recoveryCtx:                recoveryCtx,
		recoveryCancel:             recoveryCancel,
		recoverySignal:             make(chan struct{}, 1),
		recoveryDone:               make(chan struct{}),
		js:                         js,
		conn:                       conn,
		fetchBatchSize:             fetchBatchSize,
		fetchTimeout:               fetchTimeout,
		defaultMaxConcurrency:      maxConcurrency,
		streamName:                 streamName,
		dispatcher:                 dispatcher,
		tracer:                     tracer,
		dispatchDuration:           dispatchDuration,
		processDuration:            processDuration,
		tokenSource:                tokenSource,
		subscriptions:              make(map[string]*TriggerSubscription),
		tokenFailures:              make(map[string]map[string]struct{}),
		tokenValidationGenerations: make(map[string]int64),
		shutdownDone:               make(chan struct{}),
	}
	go cm.runTokenRecovery()

	// Observable gauge: in-flight dispatches per trigger. len(sem) is the
	// number of currently-held semaphore slots, which equals the number of
	// dispatch goroutines this trigger has in flight. Read under m.mu so the
	// subscription map is stable while the callback iterates.
	if _, err := meter.Int64ObservableGauge(
		"kn.eventing.broker.filter.dispatches.inflight",
		metric.WithDescription("Current number of in-flight dispatch goroutines per trigger"),
		metric.WithInt64Callback(func(_ context.Context, obs metric.Int64Observer) error {
			cm.mu.RLock()
			defer cm.mu.RUnlock()
			for _, sub := range cm.subscriptions {
				obs.Observe(int64(len(sub.sem)), metric.WithAttributes(
					attribute.String("kn.trigger.name", sub.trigger.Name),
					attribute.String("kn.trigger.namespace", sub.trigger.Namespace),
				))
			}
			return nil
		}),
	); err != nil {
		logger.Warnw("failed to register inflight observable gauge; metric will be skipped", zap.Error(err))
	}

	return cm
}

// Ready reports whether every reconciled Trigger can obtain all tokens
// required by its current destinations.
func (m *ConsumerManager) Ready() bool {
	ready := true
	if m.tokenRequirements != nil {
		requirements, synced := m.tokenRequirements()
		if !synced {
			return false
		}
		m.readinessMu.RLock()
		if len(m.tokenFailures) != 0 {
			m.readinessMu.RUnlock()
			return false
		}
		for _, requirement := range requirements {
			if !requirement.resolved {
				// Trigger resolution is gated on Broker readiness. An unresolved
				// Trigger cannot have a subscription or send an unauthenticated
				// request, so excluding it here breaks the initial Broker/filter
				// readiness cycle without weakening authenticated delivery.
				continue
			}
			if requirement.authenticated && m.tokenValidationGenerations[requirement.triggerUID] != requirement.generation {
				m.readinessMu.RUnlock()
				return false
			}
		}
		m.readinessMu.RUnlock()
	} else {
		m.readinessMu.RLock()
		ready = len(m.tokenFailures) == 0
		m.readinessMu.RUnlock()
	}
	if !ready {
		return false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.audienceConfigurationInvalid {
		return false
	}
	for _, sub := range m.subscriptions {
		if sub.configurationPaused {
			return false
		}
	}
	return true
}

func (m *ConsumerManager) setAudienceConfigurationValid(valid bool) bool {
	invalid := !valid
	m.mu.Lock()
	defer m.mu.Unlock()
	changed := m.audienceConfigurationInvalid != invalid
	if !changed {
		return false
	}
	m.audienceConfigurationInvalid = invalid
	if valid {
		return true
	}
	for _, sub := range m.subscriptions {
		if !sub.configurationPaused {
			sub.configurationPaused = true
			if sub.cancel != nil {
				sub.cancel()
			}
		}
	}
	return false
}

func (m *ConsumerManager) startFetchLoopLocked(sub *TriggerSubscription) {
	fetchCtx, fetchCancel := context.WithCancel(m.ctx)
	sub.cancel = fetchCancel
	sub.done = make(chan struct{})
	logger := m.logger.With(
		zap.String("trigger", sub.trigger.Name),
		zap.String("namespace", sub.trigger.Namespace),
	)
	go m.fetchLoop(fetchCtx, sub.dispatchCtx, sub.done, &sub.inflight, sub.subscription, sub.handler, sub.ackWait, sub.fetchBatchSize, sub.fetchTimeout, sub.sem, logger)
}

func (m *ConsumerManager) setTokenReadinessRequirements(requirements func() ([]tokenReadinessRequirement, bool)) {
	m.tokenRequirements = requirements
}

func (m *ConsumerManager) markDestinationTokensValidated(triggerUID string, generation int64) {
	m.readinessMu.Lock()
	defer m.readinessMu.Unlock()
	m.tokenValidationGenerations[triggerUID] = generation
}

func (m *ConsumerManager) clearDestinationTokenValidation(triggerUID string) {
	m.readinessMu.Lock()
	defer m.readinessMu.Unlock()
	delete(m.tokenValidationGenerations, triggerUID)
}

func (m *ConsumerManager) markDestinationTokenFailure(triggerUID, audience string) bool {
	m.readinessMu.Lock()
	defer m.readinessMu.Unlock()
	failures := m.tokenFailures[triggerUID]
	if failures == nil {
		failures = make(map[string]struct{})
		m.tokenFailures[triggerUID] = failures
	}
	_, found := failures[audience]
	failures[audience] = struct{}{}
	return !found
}

func (m *ConsumerManager) clearDestinationTokenFailure(triggerUID, audience string) bool {
	m.readinessMu.Lock()
	defer m.readinessMu.Unlock()
	failures := m.tokenFailures[triggerUID]
	delete(failures, audience)
	if len(failures) == 0 {
		delete(m.tokenFailures, triggerUID)
		return true
	}
	return false
}

func (m *ConsumerManager) clearDestinationTokenFailures(triggerUID string) {
	m.readinessMu.Lock()
	defer m.readinessMu.Unlock()
	delete(m.tokenFailures, triggerUID)
	delete(m.tokenValidationGenerations, triggerUID)
}

func (m *ConsumerManager) destinationTokensReady(triggerUID string) bool {
	m.readinessMu.RLock()
	defer m.readinessMu.RUnlock()
	return len(m.tokenFailures[triggerUID]) == 0
}

func (m *ConsumerManager) hasDestinationTokenFailure(triggerUID string) bool {
	m.readinessMu.RLock()
	defer m.readinessMu.RUnlock()
	return len(m.tokenFailures[triggerUID]) != 0
}

func (m *ConsumerManager) hasDestinationTokenFailureFor(triggerUID, audience string) bool {
	m.readinessMu.RLock()
	defer m.readinessMu.RUnlock()
	_, found := m.tokenFailures[triggerUID][audience]
	return found
}

// pauseTriggerForToken stops only the fetch producer. It must not unsubscribe
// synchronously from a dispatch goroutine because unsubscribe waits for that
// same goroutine. A background recovery loop restarts fetching after the
// destination token becomes available again.
func (m *ConsumerManager) reportDestinationTokenFailure(triggerUID, audience string) {
	if !m.markDestinationTokenFailure(triggerUID, audience) {
		return
	}
	m.recoveryPending.Store(tokenRecoveryKey{triggerUID: triggerUID, audience: audience}, struct{}{})
	select {
	case m.recoverySignal <- struct{}{}:
	default:
	}
}

func (m *ConsumerManager) runTokenRecovery() {
	defer close(m.recoveryDone)
	for {
		select {
		case <-m.recoveryCtx.Done():
			return
		case <-m.recoverySignal:
			m.recoveryPending.Range(func(key, _ interface{}) bool {
				recovery, ok := key.(tokenRecoveryKey)
				if !ok || !m.recoveryPending.CompareAndDelete(key, struct{}{}) {
					return true
				}
				m.recoveryWorkers.Add(1)
				go func() {
					defer m.recoveryWorkers.Done()
					m.pauseTriggerForToken(recovery.triggerUID, recovery.audience)
				}()
				return true
			})
		}
	}
}

func (m *ConsumerManager) pauseTriggerForToken(triggerUID, audience string) {
	m.mu.Lock()
	sub, found := m.subscriptions[triggerUID]
	if !found || m.closing {
		m.mu.Unlock()
		return
	}
	if !sub.tokenPaused {
		sub.tokenPaused = true
		if sub.cancel != nil {
			sub.cancel()
		}
	}
	done := sub.done
	m.mu.Unlock()

	m.resumeTriggerAfterToken(triggerUID, audience, sub, done)
}

func (m *ConsumerManager) resumeTriggerAfterToken(triggerUID, audience string, sub *TriggerSubscription, done <-chan struct{}) {
	select {
	case <-done:
	case <-m.recoveryCtx.Done():
		return
	}
	backoff := time.Second
	for {
		m.mu.RLock()
		current := !m.closing && m.subscriptions[triggerUID] == sub
		m.mu.RUnlock()
		if !current {
			return
		}
		if !m.hasDestinationTokenFailureFor(triggerUID, audience) {
			break
		}
		token, err := m.tokenSource(m.recoveryCtx, audience)
		if err == nil && strings.TrimSpace(token) != "" {
			break
		}
		jittered := backoff - backoff/4 + time.Duration(rand.Int64N(int64(backoff/2)))
		timer := time.NewTimer(jittered)
		select {
		case <-timer.C:
		case <-m.recoveryCtx.Done():
			timer.Stop()
			return
		}
		if backoff < 30*time.Second {
			backoff *= 2
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}
		}
	}
	m.clearDestinationTokenFailure(triggerUID, audience)

	m.mu.Lock()
	if m.closing {
		m.mu.Unlock()
		return
	}
	if m.subscriptions[triggerUID] != sub {
		m.mu.Unlock()
		return
	}
	if !sub.tokenPaused {
		m.mu.Unlock()
		return
	}
	if !m.destinationTokensReady(triggerUID) {
		m.mu.Unlock()
		return
	}
	sub.tokenPaused = false
	if sub.configurationPaused {
		m.mu.Unlock()
		return
	}
	m.startFetchLoopLocked(sub)
	m.mu.Unlock()
}

func (m *ConsumerManager) validateDestinationTokens(ctx context.Context, triggerUID string, subscriber duckv1.Addressable, deadLetterSink *duckv1.Addressable) error {
	destinations := []*duckv1.Addressable{&subscriber, deadLetterSink}
	current := make(map[string]struct{}, len(destinations))
	var firstErr error
	for _, destination := range destinations {
		if destination == nil || destination.Audience == nil || *destination.Audience == "" {
			continue
		}
		audience := *destination.Audience
		current[audience] = struct{}{}
		if m.tokenSource == nil {
			m.markDestinationTokenFailure(triggerUID, audience)
			if firstErr == nil {
				firstErr = fmt.Errorf("%w (%s)", ErrOIDCTokenUnavailable, brokeroidc.AudienceKey(audience))
			}
			continue
		}
		token, err := m.tokenSource(ctx, audience)
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		if err != nil || strings.TrimSpace(token) == "" {
			m.markDestinationTokenFailure(triggerUID, audience)
			if firstErr == nil {
				firstErr = fmt.Errorf("%w (%s)", ErrOIDCTokenUnavailable, brokeroidc.AudienceKey(audience))
			}
			continue
		}
		m.clearDestinationTokenFailure(triggerUID, audience)
	}
	m.readinessMu.Lock()
	for audience := range m.tokenFailures[triggerUID] {
		if _, found := current[audience]; !found {
			delete(m.tokenFailures[triggerUID], audience)
		}
	}
	if len(m.tokenFailures[triggerUID]) == 0 {
		delete(m.tokenFailures, triggerUID)
	}
	m.readinessMu.Unlock()
	m.resumePausedTriggerAfterRequirementsChanged(ctx, triggerUID)
	return firstErr
}

func (m *ConsumerManager) resumePausedTriggerAfterRequirementsChanged(ctx context.Context, triggerUID string) {
	m.mu.RLock()
	sub := m.subscriptions[triggerUID]
	if sub == nil || !sub.tokenPaused || m.closing {
		m.mu.RUnlock()
		return
	}
	done := sub.done
	m.mu.RUnlock()

	if done != nil {
		select {
		case <-done:
		case <-ctx.Done():
			return
		}
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closing || m.subscriptions[triggerUID] != sub || !sub.tokenPaused || !m.destinationTokensReady(triggerUID) {
		return
	}
	sub.tokenPaused = false
	if !sub.configurationPaused {
		m.startFetchLoopLocked(sub)
	}
}

// parseTriggerAnnotationInt reads key from annotations as a positive int,
// returning defaultVal (with a warning log) when absent, non-numeric, or <= 0.
func parseTriggerAnnotationInt(annotations map[string]string, key string, defaultVal int, logger *zap.SugaredLogger) int {
	ann := annotations[key]
	if ann == "" {
		return defaultVal
	}
	n, err := strconv.Atoi(ann)
	if err != nil || n <= 0 {
		logger.Warnw("invalid annotation value, using default",
			zap.String("key", key),
			zap.String("annotation", ann),
			zap.Int("default", defaultVal),
		)
		return defaultVal
	}
	return n
}

// parseTriggerAnnotationDuration reads key from annotations as a positive
// duration, returning defaultVal (with a warning log) when absent, unparseable, or <= 0.
func parseTriggerAnnotationDuration(annotations map[string]string, key string, defaultVal time.Duration, logger *zap.SugaredLogger) time.Duration {
	ann := annotations[key]
	if ann == "" {
		return defaultVal
	}
	d, err := time.ParseDuration(ann)
	if err != nil || d <= 0 {
		logger.Warnw("invalid annotation value, using default",
			zap.String("key", key),
			zap.String("annotation", ann),
			zap.Duration("default", defaultVal),
		)
		return defaultVal
	}
	return d
}

// SubscribeTrigger creates a pull-based subscription for a trigger's consumer
func (m *ConsumerManager) SubscribeTrigger(
	trigger *eventingv1.Trigger,
	broker *eventingv1.Broker,
	subscriber duckv1.Addressable,
	brokerIngressURL *duckv1.Addressable,
	deadLetterSink *duckv1.Addressable,
	retryConfig *kncloudevents.RetryConfig,
	noRetryConfig *kncloudevents.RetryConfig,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closing {
		return ErrConsumerManagerClosed
	}

	triggerUID := string(trigger.UID)
	logger := m.logger.With(
		zap.String("trigger", trigger.Name),
		zap.String("namespace", trigger.Namespace),
		zap.String("trigger_uid", triggerUID),
	)

	// Check if we already have a subscription for this trigger.
	// All handler fields are safe to update in place — the NATS pull
	// subscription is bound to (stream, consumer) which are derived from
	// the immutable (broker, trigger UID) and never change. The fetch loop,
	// however, captures its parameters at start, so a change to any of the
	// three fetch-related annotations requires restarting it.
	if existing, ok := m.subscriptions[triggerUID]; ok {
		existing.handler.Update(trigger, subscriber, brokerIngressURL, deadLetterSink, retryConfig, noRetryConfig)

		newBatch := parseTriggerAnnotationInt(trigger.Annotations, TriggerFetchBatchSizeAnnotation, m.fetchBatchSize, logger)
		newTimeout := parseTriggerAnnotationDuration(trigger.Annotations, TriggerFetchTimeoutAnnotation, m.fetchTimeout, logger)
		newMaxConc := parseTriggerAnnotationInt(trigger.Annotations, TriggerMaxConcurrencyAnnotation, m.defaultMaxConcurrency, logger)
		parametersChanged := newBatch != existing.fetchBatchSize ||
			newTimeout != existing.fetchTimeout ||
			newMaxConc != existing.maxConcurrency

		if existing.configurationPaused {
			if parametersChanged {
				existing.fetchBatchSize = newBatch
				existing.fetchTimeout = newTimeout
				existing.maxConcurrency = newMaxConc
				existing.sem = make(chan struct{}, newMaxConc)
			}
			if m.audienceConfigurationInvalid {
				logger.Debugw("updated trigger subscription while the OIDC audience set remains invalid")
				return nil
			}
			if existing.done != nil {
				<-existing.done
			}
			existing.configurationPaused = false
			if !existing.tokenPaused {
				m.startFetchLoopLocked(existing)
			}
			logger.Debugw("resumed trigger subscription after validating the OIDC audience set")
			return nil
		}

		if !parametersChanged {
			logger.Debugw("trigger subscription updated in place")
			return nil
		}

		logger.Infow("fetch parameters changed, restarting fetch loop",
			zap.Int("old_batch_size", existing.fetchBatchSize),
			zap.Int("new_batch_size", newBatch),
			zap.Duration("old_fetch_timeout", existing.fetchTimeout),
			zap.Duration("new_fetch_timeout", newTimeout),
			zap.Int("old_max_concurrency", existing.maxConcurrency),
			zap.Int("new_max_concurrency", newMaxConc),
		)

		newSem := make(chan struct{}, newMaxConc)
		if existing.tokenPaused {
			// The recovery loop owns the next fetch-loop start while destination
			// credentials are unavailable. Update the captured parameters now,
			// but do not start a second producer behind its back.
			existing.fetchBatchSize = newBatch
			existing.fetchTimeout = newTimeout
			existing.maxConcurrency = newMaxConc
			existing.sem = newSem
			logger.Debugw("updated paused trigger subscription")
			return nil
		}

		// Stop the current fetch loop and wait until it has stopped calling
		// Fetch. Two goroutines must not overlap on the same pull subscription.
		// Canceling the fetch context also interrupts an in-progress Fetch.
		// In-flight dispatches use existing.dispatchCtx and keep running
		// uninterrupted.
		existing.cancel()
		<-existing.done

		fetchCtx, fetchCancel := context.WithCancel(m.ctx)
		newDone := make(chan struct{})

		existing.fetchBatchSize = newBatch
		existing.fetchTimeout = newTimeout
		existing.maxConcurrency = newMaxConc
		existing.sem = newSem
		existing.cancel = fetchCancel
		existing.done = newDone

		go m.fetchLoop(fetchCtx, existing.dispatchCtx, newDone, &existing.inflight, existing.subscription, existing.handler, existing.ackWait, newBatch, newTimeout, newSem, logger)
		return nil
	}

	// Create the trigger handler
	handler, err := NewTriggerHandler(
		m.ctx,
		trigger,
		subscriber,
		brokerIngressURL,
		deadLetterSink,
		retryConfig,
		noRetryConfig,
		m.dispatcher,
		m.tracer,
		m.dispatchDuration,
		m.processDuration,
	)
	if err != nil {
		return fmt.Errorf("failed to create trigger handler: %w", err)
	}
	if m.tokenSource != nil {
		handler.setAudienceTokenSource(func(ctx context.Context, audience string) (string, error) {
			token, err := m.tokenSource(ctx, audience)
			if (err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)) ||
				(err == nil && strings.TrimSpace(token) == "") {
				m.reportDestinationTokenFailure(triggerUID, audience)
			}
			return token, err
		})
	}

	// Derive stream and consumer names
	streamName := brokerutils.BrokerStreamName(broker)
	if m.streamName != "" && streamName != m.streamName {
		handler.Cleanup()
		return fmt.Errorf("trigger stream %s does not match filter stream %s", streamName, m.streamName)
	}
	consumerName := brokerutils.TriggerConsumerName(triggerUID)

	// Get consumer info (also verifies consumer exists)
	consumerInfo, err := m.js.ConsumerInfo(streamName, consumerName)
	if err != nil {
		handler.Cleanup()
		if errors.Is(err, nats.ErrConsumerNotFound) {
			return fmt.Errorf("consumer %s not found in stream %s: trigger controller may not have reconciled yet", consumerName, streamName)
		}
		return fmt.Errorf("failed to get consumer info: %w", err)
	}
	ackWait := consumerInfo.Config.AckWait

	// Resolve per-trigger fetch parameters.
	// Trigger annotations take precedence; absent or invalid values fall back
	// to the manager defaults set via env vars on the filter deployment.
	fetchBatchSize := parseTriggerAnnotationInt(trigger.Annotations, TriggerFetchBatchSizeAnnotation, m.fetchBatchSize, logger)
	fetchTimeout := parseTriggerAnnotationDuration(trigger.Annotations, TriggerFetchTimeoutAnnotation, m.fetchTimeout, logger)
	maxConcurrency := parseTriggerAnnotationInt(trigger.Annotations, TriggerMaxConcurrencyAnnotation, m.defaultMaxConcurrency, logger)

	sem := make(chan struct{}, maxConcurrency)

	// Get the filter subject from the consumer's configuration
	filterSubject := brokerutils.BrokerPublishSubjectName(broker.Namespace, broker.Name) + ".>"

	logger.Infow("creating pull subscription for trigger consumer",
		zap.String("stream", streamName),
		zap.String("consumer", consumerName),
		zap.String("filter_subject", filterSubject),
	)

	// Create pull subscription bound to the existing consumer
	sub, err := m.js.PullSubscribe(
		filterSubject,
		consumerName,
		nats.Bind(streamName, consumerName),
	)
	if err != nil {
		handler.Cleanup()
		return fmt.Errorf("failed to create pull subscription: %w", err)
	}

	// Two cancellable contexts: dispatchCtx survives fetch-loop restart and
	// parents per-message msgCtx; fetchCtx controls the current fetch loop only.
	dispatchCtx, dispatchCancel := context.WithCancel(m.ctx)
	fetchCtx, fetchCancel := context.WithCancel(m.ctx)
	done := make(chan struct{})

	// Store the subscription
	ts := &TriggerSubscription{
		trigger:        trigger,
		subscription:   natsPullSubscription{Subscription: sub},
		handler:        handler,
		streamName:     streamName,
		consumerName:   consumerName,
		ackWait:        ackWait,
		fetchBatchSize: fetchBatchSize,
		fetchTimeout:   fetchTimeout,
		maxConcurrency: maxConcurrency,
		sem:            sem,
		dispatchCtx:    dispatchCtx,
		dispatchCancel: dispatchCancel,
		cancel:         fetchCancel,
		done:           done,
	}
	m.subscriptions[triggerUID] = ts
	configurationValid := !m.audienceConfigurationInvalid
	if !configurationValid {
		ts.configurationPaused = true
		fetchCancel()
		close(done)
		logger.Debugw("trigger subscription remains paused while the OIDC audience set is invalid")
		return nil
	}

	logger.Infow("starting fetch loop",
		zap.Int("fetch_batch_size", fetchBatchSize),
		zap.Duration("fetch_timeout", fetchTimeout),
		zap.Int("max_concurrency", maxConcurrency),
	)

	// Start the message fetch loop
	go m.fetchLoop(fetchCtx, dispatchCtx, done, &ts.inflight, ts.subscription, handler, ackWait, fetchBatchSize, fetchTimeout, sem, logger)

	logger.Infow("successfully started pull subscription for trigger consumer")
	return nil
}

// fetchLoop continuously fetches messages from the pull consumer and dispatches
// them concurrently. Before each fetch it checks how many semaphore slots are
// free and requests at most that many messages. This guarantees every fetched
// message acquires its slot within microseconds — no message sits fetched-but-
// unprocessed with JetStream's AckWait clock already running. When all slots
// are occupied the loop waits one fetchTimeout before re-checking, leaving
// messages safely in the stream. Each dispatch goroutine carries a context
// deadline equal to the consumer's AckWait so that the outbound HTTP call is
// cancelled before JetStream redelivers the message.
//
// Two contexts govern lifetime:
//   - ctx controls the fetch loop itself. Cancel it to stop fetching (used by
//     unsubscribe and restart-on-annotation-change).
//   - dispatchCtx parents each in-flight msgCtx. It survives a fetch-loop
//     restart so a parameter change does not abort in-progress dispatches.
//
// Spawned dispatches are tracked on the subscription-scoped inflight
// WaitGroup, not a local one. unsubscribeLocked waits on it to drain in-flight
// dispatches (across any fetch-loop generation) before tearing down the NATS
// subscription and trigger handler. The fetch loop itself does not wait on
// dispatches — it closes done and returns as soon as it stops calling Fetch,
// so a restart can start a new fetch loop without delay.
func (m *ConsumerManager) fetchLoop(
	ctx context.Context,
	dispatchCtx context.Context,
	done chan struct{},
	inflight *sync.WaitGroup,
	sub pullSubscription,
	handler *TriggerHandler,
	ackWait time.Duration,
	fetchBatchSize int,
	fetchTimeout time.Duration,
	sem chan struct{},
	logger *zap.SugaredLogger,
) {
	defer close(done)

	for {
		select {
		case <-ctx.Done():
			logger.Debugw("fetch loop stopped")
			return
		default:
		}

		// Determine how many messages to request this round.
		// When a semaphore is configured, cap the request to the number of
		// free slots so every returned message can acquire its slot
		// immediately — keeping our AckWait context aligned with JetStream's
		// delivery clock. fetchLoop is the only goroutine that acquires slots,
		// so cap(sem)-len(sem) is a stable lower bound: in-flight goroutines
		// can only release slots between here and the acquire, never consume
		// new ones.
		batchSize := fetchBatchSize
		if sem != nil {
			available := cap(sem) - len(sem)
			if available == 0 {
				// All slots occupied. Wait one fetch interval so messages
				// remain in the stream, then re-check.
				select {
				case <-time.After(fetchTimeout):
				case <-ctx.Done():
					return
				}
				continue
			}
			if available < batchSize {
				batchSize = available
			}
		}

		requestCtx, cancelFetch := context.WithTimeout(ctx, fetchTimeout)
		msgs, err := sub.Fetch(requestCtx, batchSize)
		cancelFetch()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, context.Canceled) {
				return
			}
			if errors.Is(err, nats.ErrTimeout) || errors.Is(err, context.DeadlineExceeded) {
				continue
			}
			if errors.Is(err, nats.ErrConnectionClosed) || errors.Is(err, nats.ErrConsumerDeleted) || errors.Is(err, nats.ErrBadSubscription) {
				logger.Warnw("subscription closed, stopping fetch loop", zap.Error(err))
				return
			}
			logger.Errorw("error fetching messages", zap.Error(err))
			select {
			case <-time.After(200 * time.Millisecond):
			case <-ctx.Done():
				return
			}
			continue
		}
		if ctx.Err() != nil {
			return
		}

		for _, msg := range msgs {
			msg := msg
			if ctx.Err() != nil {
				return
			}

			// Acquire a semaphore slot. Because batchSize was capped to the
			// number of free slots above, this send is non-blocking in the
			// steady state. The ctx.Done case handles clean shutdown.
			if sem != nil {
				select {
				case sem <- struct{}{}:
				case <-ctx.Done():
					return
				}
			}

			inflight.Add(1)
			go func() {
				defer func() {
					if sem != nil {
						<-sem
					}
					inflight.Done()
				}()

				var msgCtx context.Context
				var cancel context.CancelFunc
				if ackWait > 0 {
					msgCtx, cancel = context.WithTimeout(dispatchCtx, ackWait)
				} else {
					msgCtx, cancel = context.WithCancel(dispatchCtx)
				}
				defer cancel()

				handler.HandleMessage(msgCtx, msg)
			}()
		}
	}
}

// UnsubscribeTrigger removes a subscription for a trigger
func (m *ConsumerManager) UnsubscribeTrigger(triggerUID string) error {
	return m.unsubscribeTrigger(triggerUID, true)
}

func (m *ConsumerManager) unsubscribeTrigger(triggerUID string, clearTokenFailure bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closing {
		return ErrConsumerManagerClosed
	}

	return m.unsubscribeLocked(triggerUID, clearTokenFailure)
}

// unsubscribeLocked removes a subscription (must be called with lock held)
func (m *ConsumerManager) unsubscribeLocked(triggerUID string, clearTokenFailure bool) error {
	sub, ok := m.subscriptions[triggerUID]
	if !ok {
		if clearTokenFailure {
			m.clearDestinationTokenFailures(triggerUID)
		}
		return nil
	}

	logger := m.logger.With(
		zap.String("trigger", sub.trigger.Name),
		zap.String("namespace", sub.trigger.Namespace),
	)

	logger.Infow("unsubscribing from trigger consumer")

	// Stop the fetch producer first.
	if sub.cancel != nil {
		sub.cancel()
	}
	// done is the happens-before barrier that guarantees no future inflight.Add
	// can race the Wait below.
	if sub.done != nil {
		<-sub.done
	}
	// Only after the producer is stopped, cancel the fixed set of dispatches.
	if sub.dispatchCancel != nil {
		sub.dispatchCancel()
	}

	// Wait for every dispatch goroutine — across any fetch-loop generation —
	// to exit before tearing down the NATS subscription and trigger handler.
	// Without this wait, in-flight goroutines could race with Unsubscribe
	// (msg.Ack on a closed subscription) and with handler.Cleanup (concurrent
	// h.filter.Filter vs h.filter.Cleanup). Bounded by ackWait via msgCtx;
	// resolves in milliseconds when the HTTP client respects ctx cancellation.
	sub.inflight.Wait()

	// Unsubscribe from the pull consumer
	if err := sub.subscription.Unsubscribe(); err != nil {
		logger.Warnw("failed to unsubscribe", zap.Error(err))
	}

	// Cleanup the handler
	sub.handler.Cleanup()

	// Remove from map
	delete(m.subscriptions, triggerUID)
	if clearTokenFailure {
		m.clearDestinationTokenFailures(triggerUID)
	}

	return nil
}

// Close preserves the original immediate-close behavior: stop fetchers,
// cancel dispatches, then wait for them before tearing resources down.
func (m *ConsumerManager) Close() error {
	return m.shutdownWithMode(context.Background(), true)
}

// Shutdown stops fetching new messages, gives in-flight dispatches the
// supplied deadline to finish, and only cancels dispatches during the final
// forcedDispatchGracePeriod. It is safe to call concurrently or repeatedly.
func (m *ConsumerManager) Shutdown(ctx context.Context) error {
	return m.shutdownWithMode(ctx, false)
}

func (m *ConsumerManager) shutdownWithMode(ctx context.Context, forceImmediately bool) error {
	m.mu.Lock()
	if m.shutdownDone == nil {
		m.shutdownDone = make(chan struct{})
	}
	if m.closing {
		done := m.shutdownDone
		m.mu.Unlock()
		select {
		case <-done:
			m.mu.RLock()
			defer m.mu.RUnlock()
			return m.shutdownErr
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	m.closing = true
	subscriptions := make([]*TriggerSubscription, 0, len(m.subscriptions))
	for _, sub := range m.subscriptions {
		subscriptions = append(subscriptions, sub)
	}
	m.mu.Unlock()
	if m.recoveryCancel != nil {
		m.recoveryCancel()
	}
	if m.recoveryDone != nil {
		<-m.recoveryDone
	}
	m.recoveryWorkers.Wait()

	err := m.shutdown(ctx, subscriptions, forceImmediately)

	m.mu.Lock()
	m.shutdownErr = err
	close(m.shutdownDone)
	m.mu.Unlock()
	return err
}

func (m *ConsumerManager) shutdown(ctx context.Context, subscriptions []*TriggerSubscription, forceImmediately bool) error {
	m.logger.Infow("closing consumer manager", zap.Int("subscription_count", len(subscriptions)))
	if m.cancel != nil {
		defer m.cancel()
	}

	// Stop every producer before observing inflight WaitGroups. Closing done is
	// the barrier after which no goroutine can call inflight.Add.
	for _, sub := range subscriptions {
		if sub.cancel != nil {
			sub.cancel()
		}
	}
	for _, sub := range subscriptions {
		if sub.done == nil {
			continue
		}
		select {
		case <-sub.done:
		case <-ctx.Done():
			m.cancelDispatches(subscriptions)
			m.finalizeWhenStopped(subscriptions, true)
			return ctx.Err()
		}
	}
	if forceImmediately {
		m.cancelDispatches(subscriptions)
	}

	inflightDone := make(chan struct{})
	go func() {
		for _, sub := range subscriptions {
			sub.inflight.Wait()
		}
		close(inflightDone)
	}()

	// Reserve a short interval at the end of the deadline for cancel-aware HTTP
	// handlers to exit. Shutdown without a deadline waits for a completely
	// natural drain and never aborts a dispatch.
	var force <-chan time.Time
	var timer *time.Timer
	if deadline, ok := ctx.Deadline(); ok && !forceImmediately {
		forceAt := time.Until(deadline.Add(-forcedDispatchGracePeriod))
		if forceAt < 0 {
			forceAt = 0
		}
		timer = time.NewTimer(forceAt)
		force = timer.C
		defer timer.Stop()
	}

	select {
	case <-inflightDone:
	case <-force:
		m.cancelDispatches(subscriptions)
		select {
		case <-inflightDone:
		case <-ctx.Done():
			m.finalizeWhenStopped(subscriptions, true)
			return ctx.Err()
		}
	case <-ctx.Done():
		m.cancelDispatches(subscriptions)
		m.finalizeWhenStopped(subscriptions, true)
		return ctx.Err()
	}

	return m.finalizeSubscriptions(subscriptions)
}

func (m *ConsumerManager) finalizeWhenStopped(subscriptions []*TriggerSubscription, dispatchesCanceled bool) {
	go func() {
		for _, sub := range subscriptions {
			if sub.done != nil {
				<-sub.done
			}
		}
		if !dispatchesCanceled {
			m.cancelDispatches(subscriptions)
		}
		for _, sub := range subscriptions {
			sub.inflight.Wait()
		}
		if err := m.finalizeSubscriptions(subscriptions); err != nil {
			m.logger.Warnw("failed to finish asynchronous consumer cleanup", zap.Error(err))
		}
	}()
}

func (m *ConsumerManager) finalizeSubscriptions(subscriptions []*TriggerSubscription) error {
	var errs []error
	for _, sub := range subscriptions {
		if err := sub.subscription.Unsubscribe(); err != nil {
			errs = append(errs, err)
		}
		sub.handler.Cleanup()
	}

	m.mu.Lock()
	clear(m.subscriptions)
	m.mu.Unlock()
	return errors.Join(errs...)
}

func (m *ConsumerManager) cancelDispatches(subscriptions []*TriggerSubscription) {
	for _, sub := range subscriptions {
		if sub.dispatchCancel != nil {
			sub.dispatchCancel()
		}
	}
}

// GetSubscriptionCount returns the number of active subscriptions
func (m *ConsumerManager) GetSubscriptionCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.subscriptions)
}

// HasSubscription checks if a subscription exists for a trigger
func (m *ConsumerManager) HasSubscription(triggerUID string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.subscriptions[triggerUID]
	return ok
}
