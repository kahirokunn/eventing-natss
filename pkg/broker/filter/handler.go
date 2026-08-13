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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	cejs "github.com/cloudevents/sdk-go/protocol/nats_jetstream/v2"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/binding"
	"github.com/cloudevents/sdk-go/v2/binding/spec"
	"github.com/cloudevents/sdk-go/v2/protocol"
	cehttp "github.com/cloudevents/sdk-go/v2/protocol/http"
	"github.com/cloudevents/sdk-go/v2/types"
	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	duckv1 "knative.dev/pkg/apis/duck/v1"
	"knative.dev/pkg/logging"

	brokeroidc "knative.dev/eventing-natss/pkg/broker/oidc"
	jsutils "knative.dev/eventing-natss/pkg/channel/jetstream/utils"
	"knative.dev/eventing-natss/pkg/tracing"
	eventingduckv1 "knative.dev/eventing/pkg/apis/duck/v1"
	eventingv1 "knative.dev/eventing/pkg/apis/eventing/v1"
	"knative.dev/eventing/pkg/eventfilter"
	"knative.dev/eventing/pkg/eventfilter/attributes"
	"knative.dev/eventing/pkg/eventfilter/subscriptionsapi"
	"knative.dev/eventing/pkg/kncloudevents"
	eventingutils "knative.dev/eventing/pkg/utils"
)

var retryMax int32 = 3
var retryTimeout = "PT1S"
var retryBackoffDelay = "PT0.5S"

var ErrOIDCTokenUnavailable = errors.New("OIDC token is unavailable")

type audienceTokenSource func(context.Context, string) (string, error)

// TypeExtractorTransformer extracts the CloudEvent type from a message.
// Copied from channel/jetstream/dispatcher to avoid importing that package
// which registers channel informers.
type TypeExtractorTransformer string

func (a *TypeExtractorTransformer) Transform(reader binding.MessageMetadataReader, _ binding.MessageMetadataWriter) error {
	_, ty := reader.GetAttribute(spec.Type)
	if ty != nil {
		tyParsed, err := types.ToString(ty)
		if err != nil {
			return err
		}
		*a = TypeExtractorTransformer(tyParsed)
	}
	return nil
}

var retryDelivery = eventingduckv1.BackoffPolicyLinear
var defaultRetry, _ = kncloudevents.RetryConfigFromDeliverySpec(eventingduckv1.DeliverySpec{
	Retry:         &retryMax,
	Timeout:       &retryTimeout,
	BackoffPolicy: &retryDelivery,
	BackoffDelay:  &retryBackoffDelay,
})

// TriggerHandler handles message dispatch for a single trigger
type TriggerHandler struct {
	logger *zap.SugaredLogger
	ctx    context.Context

	// Dispatcher for sending events
	dispatcher *kncloudevents.Dispatcher

	configMu sync.RWMutex
	config   *handlerConfig

	// Observability. tracer is used to wrap each dispatch in a span;
	// dispatchDuration records the per-dispatch HTTP wall time; processDuration
	// records the pre-dispatch work (message decode + filter evaluation).
	tracer           trace.Tracer
	dispatchDuration metric.Float64Histogram
	processDuration  metric.Float64Histogram
	tokenSource      audienceTokenSource
}

// setAudienceTokenSource keeps the public constructor compatible while
// allowing the runtime to inject its context-aware TokenRequest source.
func (h *TriggerHandler) setAudienceTokenSource(source func(context.Context, string) (string, error)) {
	h.tokenSource = source
}

type handlerConfig struct {
	trigger          *eventingv1.Trigger
	subscriber       duckv1.Addressable
	filter           eventfilter.Filter
	brokerIngressURL *duckv1.Addressable
	retryConfig      *kncloudevents.RetryConfig
	noRetryConfig    *kncloudevents.RetryConfig
	deadLetterSink   *duckv1.Addressable
}

// NewTriggerHandler creates a new handler for a trigger
func NewTriggerHandler(
	ctx context.Context,
	trigger *eventingv1.Trigger,
	subscriber duckv1.Addressable,
	brokerIngressURL *duckv1.Addressable,
	deadLetterSink *duckv1.Addressable,
	retryConfig *kncloudevents.RetryConfig,
	noRetryConfig *kncloudevents.RetryConfig,
	dispatcher *kncloudevents.Dispatcher,
	tracer trace.Tracer,
	dispatchDuration metric.Float64Histogram,
	processDuration metric.Float64Histogram,
) (*TriggerHandler, error) {
	logger := logging.FromContext(ctx).With(
		zap.String("trigger", trigger.Name),
		zap.String("namespace", trigger.Namespace),
	)

	// Fall back to the global tracer if none was provided, matching
	// kncloudevents.NewDispatcher's behavior. dispatchDuration is allowed to
	// stay nil — the histogram record sites already guard.
	if tracer == nil {
		tracer = otel.GetTracerProvider().Tracer("knative.dev/eventing-natss/pkg/broker/filter")
	}

	handler := &TriggerHandler{
		logger:           logger,
		ctx:              ctx,
		dispatcher:       dispatcher,
		tracer:           tracer,
		dispatchDuration: dispatchDuration,
		processDuration:  processDuration,
	}
	handler.config = newHandlerConfig(logger, trigger, subscriber, brokerIngressURL, deadLetterSink, retryConfig, noRetryConfig)
	return handler, nil
}

func newHandlerConfig(
	logger *zap.SugaredLogger,
	trigger *eventingv1.Trigger,
	subscriber duckv1.Addressable,
	brokerIngressURL *duckv1.Addressable,
	deadLetterSink *duckv1.Addressable,
	retryConfig *kncloudevents.RetryConfig,
	noRetryConfig *kncloudevents.RetryConfig,
) *handlerConfig {
	trigger = trigger.DeepCopy()
	return &handlerConfig{
		trigger:          trigger,
		subscriber:       *subscriber.DeepCopy(),
		filter:           buildTriggerFilter(logger, trigger),
		brokerIngressURL: deepCopyAddressable(brokerIngressURL),
		deadLetterSink:   deepCopyAddressable(deadLetterSink),
		retryConfig:      copyRetryConfig(retryConfig),
		noRetryConfig:    copyRetryConfig(noRetryConfig),
	}
}

func deepCopyAddressable(address *duckv1.Addressable) *duckv1.Addressable {
	if address == nil {
		return nil
	}
	return address.DeepCopy()
}

func copyRetryConfig(config *kncloudevents.RetryConfig) *kncloudevents.RetryConfig {
	if config == nil {
		return nil
	}
	copy := *config
	if config.BackoffDelay != nil {
		value := *config.BackoffDelay
		copy.BackoffDelay = &value
	}
	if config.BackoffPolicy != nil {
		value := *config.BackoffPolicy
		copy.BackoffPolicy = &value
	}
	if config.RetryAfterMaxDuration != nil {
		value := *config.RetryAfterMaxDuration
		copy.RetryAfterMaxDuration = &value
	}
	return &copy
}

// Update replaces all mutable trigger configuration as one snapshot. The old
// filter is cleaned up only after evaluations using it have completed.
func (h *TriggerHandler) Update(
	trigger *eventingv1.Trigger,
	subscriber duckv1.Addressable,
	brokerIngressURL *duckv1.Addressable,
	deadLetterSink *duckv1.Addressable,
	retryConfig *kncloudevents.RetryConfig,
	noRetryConfig *kncloudevents.RetryConfig,
) {
	next := newHandlerConfig(h.logger, trigger, subscriber, brokerIngressURL, deadLetterSink, retryConfig, noRetryConfig)

	h.configMu.Lock()
	previous := h.config
	h.config = next
	h.configMu.Unlock()

	if previous != nil && previous.filter != nil {
		previous.filter.Cleanup()
	}
}

// HandleMessage processes a NATS message, applies filter, and dispatches to subscriber.
// ctx should carry an AckWait deadline so that the outbound HTTP call is cancelled
// before JetStream redelivers the message; use context.WithTimeout(parent, ackWait).
func (h *TriggerHandler) HandleMessage(ctx context.Context, msg *nats.Msg) {
	logger := h.logger.With(zap.String("msg_id", msg.Header.Get(nats.MsgIdHdr)))
	ctx = logging.WithLogger(ctx, logger)

	h.doHandle(ctx, msg)
}

// doHandle processes the message synchronously
func (h *TriggerHandler) doHandle(ctx context.Context, msg *nats.Msg) {
	logger := logging.FromContext(ctx)
	h.configMu.RLock()
	config := h.config
	configLocked := true
	unlockConfig := func() {
		if configLocked {
			h.configMu.RUnlock()
			configLocked = false
		}
	}
	defer unlockConfig()
	if config == nil {
		return
	}

	// Record the pre-dispatch processing time (message decode + filter eval).
	// recordProcess is idempotent: it is called explicitly just before dispatch
	// and via defer, so any early return (unknown encoding, decode failure,
	// filtered out) is captured exactly once and dispatch time is never
	// included. It reads the ctx variable at call time, so the span context set
	// up below is used once available.
	processStart := time.Now()
	processRecorded := false
	recordProcess := func() {
		if processRecorded || h.processDuration == nil {
			return
		}
		processRecorded = true
		h.processDuration.Record(ctx, time.Since(processStart).Seconds(),
			metric.WithAttributes(
				attribute.String("kn.trigger.name", config.trigger.Name),
				attribute.String("kn.trigger.namespace", config.trigger.Namespace),
			))
	}
	defer recordProcess()

	// Convert NATS message to CloudEvents message
	message := cejs.NewMessage(msg)
	if message.ReadEncoding() == binding.EncodingUnknown {
		logger.Errorw("received a message with unknown encoding")
		unlockConfig()
		if err := msg.Term(); err != nil {
			logger.Errorw("failed to terminate message", zap.Error(err))
		}
		return
	}

	// Convert to CloudEvent for filtering
	event, err := binding.ToEvent(ctx, message)
	if err != nil {
		logger.Errorw("failed to convert message to CloudEvent", zap.Error(err))
		unlockConfig()
		if err := msg.Term(); err != nil {
			logger.Errorw("failed to terminate message", zap.Error(err))
		}
		return
	}

	// Adopt the producer's trace context from the CloudEvent extensions and
	// start a span that covers filter eval + dispatch + ack/nak/term. Using a
	// stable span name keeps trace-UI aggregation usable; per-trigger details
	// live in attributes so they can be filtered without fragmenting names.
	ctx = tracing.ParseSpanContext(ctx, event)
	ctx, span := h.tracer.Start(ctx, "broker.filter.dispatch")
	defer span.End()
	span.SetAttributes(
		attribute.String("kn.trigger.name", config.trigger.Name),
		attribute.String("kn.trigger.namespace", config.trigger.Namespace),
		attribute.String("kn.trigger.uid", string(config.trigger.UID)),
		attribute.String("messaging.destination.name", config.subscriber.URL.String()),
		attribute.String("cloudevents.event.id", event.ID()),
		attribute.String("cloudevents.event.type", event.Type()),
		attribute.String("cloudevents.event.source", event.Source()),
	)

	// Apply filter
	if config.filter != nil {
		filterResult := config.filter.Filter(ctx, *event)
		if filterResult == eventfilter.FailFilter {
			span.SetAttributes(attribute.String("filter.outcome", "fail"))
			logger.Debugw("event filtered out",
				zap.String("type", event.Type()),
				zap.String("source", event.Source()),
			)
			// Ack the message since it was intentionally filtered
			unlockConfig()
			if err := msg.Ack(); err != nil {
				logger.Errorw("failed to ack filtered message", zap.Error(err))
			}
			return
		}
		span.SetAttributes(attribute.String("filter.outcome", "pass"))
	}
	// The immutable config remains valid after the lock is released. Update
	// waits only for filter evaluation before swapping and cleaning the old
	// filter, not for outbound HTTP or NATS acknowledgement latency.
	unlockConfig()

	// Dispatch to subscriber
	logger.Debugw("dispatching event to subscriber",
		zap.String("subscriber", config.subscriber.URL.String()),
		zap.String("type", event.Type()),
		zap.String("source", event.Source()),
		zap.String("id", event.ID()),
	)

	// Pre-dispatch work is done; record it before the HTTP call so dispatch
	// time (tracked separately by dispatchDuration) is excluded.
	recordProcess()

	dispatchInfo, err := h.dispatchEvent(ctx, event, msg, config)
	if dispatchInfo != nil && dispatchInfo.ResponseCode != 0 {
		span.SetAttributes(attribute.Int("http.response.status_code", dispatchInfo.ResponseCode))
	}
	if eventProcessingDeadlineExceeded(err) {
		span.SetStatus(codes.Error, "dispatch deadline exceeded")
		logger.Warnw("dispatch context expired before subscriber response, message will be redelivered by JetStream", zap.Error(err))
		return
	} else if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		logger.Errorw("failed to dispatch event",
			zap.Error(err),
			zap.Int("response_code", dispatchResponseCode(dispatchInfo)),
		)
		return
	}

	logger.Debugw("event dispatched successfully",
		zap.Int("response_code", dispatchResponseCode(dispatchInfo)),
	)
}

// dispatchEvent sends the event to the subscriber and handles ack/nack
func (h *TriggerHandler) dispatchEvent(ctx context.Context, event *cloudevents.Event, msg *nats.Msg, config *handlerConfig) (*kncloudevents.DispatchInfo, error) {
	logger := logging.FromContext(ctx)

	additionalHeaders := tracing.ConvertEventToHttpHeader(event)
	subscriberHeaders, authErr := h.headersForDestination(ctx, config.subscriber, additionalHeaders)
	if authErr != nil {
		return &kncloudevents.DispatchInfo{}, authErr
	}
	te := TypeExtractorTransformer("")

	// Get retry number from message metadata
	retryNumber := 1
	if meta, err := msg.Metadata(); err == nil {
		retryNumber = int(meta.NumDelivered)
	}

	// Determine if this is the last try
	maxRetries := 0
	if config.retryConfig != nil {
		maxRetries = config.retryConfig.RetryMax
	}
	lastTry := retryNumber > maxRetries

	// Dispatch the message to trigger's destination
	dispatchInfo, err := h.dispatcher.SendEvent(ctx, *event, config.subscriber,
		kncloudevents.WithHeader(subscriberHeaders),
		kncloudevents.WithTransformers(&te),
		kncloudevents.WithRetryConfig(config.noRetryConfig),
	)

	// Record HTTP wall time with low-cardinality labels. Duration is left at
	// kncloudevents.NoDuration (-1) when the request never made it onto the
	// wire (e.g. request build / client creation failure); skip those.
	if h.dispatchDuration != nil && dispatchInfo != nil && dispatchInfo.Duration >= 0 {
		h.dispatchDuration.Record(ctx, dispatchInfo.Duration.Seconds(),
			metric.WithAttributes(
				attribute.String("kn.trigger.name", config.trigger.Name),
				attribute.String("kn.trigger.namespace", config.trigger.Namespace),
			))
	}

	// Context deadline/cancellation means the AckWait guard fired before we got
	// a subscriber response. Don't ack/nack/term the message — JetStream will
	// redeliver it automatically once its own AckWait timer expires.
	if eventProcessingDeadlineExceeded(err) {
		return dispatchInfo, err
	}
	if errors.Is(err, ErrOIDCTokenUnavailable) {
		return dispatchInfo, err
	}
	if dispatchInfo == nil {
		dispatchInfo = &kncloudevents.DispatchInfo{}
	}

	result := determineNatsResult(dispatchInfo.ResponseCode, err)

	// Extract pass-through headers (tracing, knative-*, x-b3-*, etc.) from
	// the subscriber's response so they are available in every code path.
	responseHeaders := eventingutils.PassThroughHeaders(dispatchInfo.ResponseHeader)
	responseHeaders.Del("Authorization")

	// Decorate the active span with the delivery attempt. The span was started
	// in doHandle; SpanFromContext returns the same one. Per-branch nats.result
	// is set inside the switch so each label reflects the path actually taken
	// (e.g. NACK on lastTry with no DLS does NOT visit the dead-letter sink).
	span := trace.SpanFromContext(ctx)
	span.SetAttributes(attribute.Int("delivery.attempt", retryNumber))

	// Handle ack/nack/term based on result
	switch {
	case protocol.IsACK(result):
		span.SetAttributes(attribute.String("nats.result", "ack"))
		// If the subscriber returned a CloudEvent response, forward it to broker ingress for re-routing.
		if config.brokerIngressURL != nil {
			responseEvent, parseErr := responseToEvent(ctx, dispatchInfo)
			if parseErr != nil {
				logger.Warnw("failed to parse response event from subscriber", zap.Error(parseErr))
			} else if responseEvent != nil {
				replyDispatchInfo, replyErr := h.dispatcher.SendEvent(ctx, *responseEvent, *config.brokerIngressURL,
					kncloudevents.WithRetryConfig(&defaultRetry),
					kncloudevents.WithHeader(responseHeaders),
					kncloudevents.WithTransformers(&te),
				)
				if replyErr != nil {
					logger.Errorw("failed to send reply to broker ingress",
						zap.Error(replyErr),
						zap.Int("response_code", dispatchResponseCode(replyDispatchInfo)),
					)
				}
			}
		}
		if err := msg.Ack(nats.Context(ctx)); err != nil {
			logger.Errorw("failed to ack message", zap.Error(err))
		}
	case protocol.IsNACK(result):
		if lastTry {
			if config.deadLetterSink != nil {
				dlsHeaders, authErr := h.headersForDestination(ctx, *config.deadLetterSink, additionalHeaders)
				if authErr != nil {
					return dispatchInfo, authErr
				}
				// Send to dead letter sink
				dlsDispatchInfo, dlsErr := h.dispatcher.SendEvent(ctx, *event, *config.deadLetterSink,
					kncloudevents.WithRetryConfig(&defaultRetry),
					kncloudevents.WithHeader(dlsHeaders),
					kncloudevents.WithTransformers(&te),
				)
				if dlsErr != nil {
					logger.Errorw("failed to send to dead letter sink",
						zap.Error(dlsErr),
						zap.Int("response_code", dispatchResponseCode(dlsDispatchInfo)),
					)
				}
				span.SetAttributes(attribute.String("nats.result", "ack_after_dls"))
			} else {
				span.SetAttributes(attribute.String("nats.result", "ack_retries_exhausted"))
			}

			// Ack after DLS attempt
			if err := msg.Ack(nats.Context(ctx)); err != nil {
				logger.Errorw("failed to ack message after last retry", zap.Error(err))
			}
		} else {
			span.SetAttributes(attribute.String("nats.result", "nak"))
			// Nack for retry
			nakDelay := jsutils.CalculateNakDelayForRetryNumber(retryNumber, config.retryConfig)
			if err := msg.NakWithDelay(nakDelay, nats.Context(ctx)); err != nil {
				logger.Errorw("failed to nack message", zap.Error(err))
			}
		}
	default:
		// Terminate - non-retriable error
		if lastTry && config.deadLetterSink != nil {
			dlsHeaders, authErr := h.headersForDestination(ctx, *config.deadLetterSink, additionalHeaders)
			if authErr != nil {
				return dispatchInfo, authErr
			}
			// Send to dead letter sink
			dlsDispatchInfo, dlsErr := h.dispatcher.SendEvent(ctx, *event, *config.deadLetterSink,
				kncloudevents.WithRetryConfig(&defaultRetry),
				kncloudevents.WithHeader(dlsHeaders),
				kncloudevents.WithTransformers(&te),
			)
			if dlsErr != nil {
				logger.Errorw("failed to send to dead letter sink",
					zap.Error(dlsErr),
					zap.Int("response_code", dispatchResponseCode(dlsDispatchInfo)),
				)
			}
			span.SetAttributes(attribute.String("nats.result", "term_after_dls"))
		} else {
			span.SetAttributes(attribute.String("nats.result", "term"))
		}

		if err := msg.Term(nats.Context(ctx)); err != nil {
			logger.Errorw("failed to term message", zap.Error(err))
		}
	}

	return dispatchInfo, err
}

func (h *TriggerHandler) headersForDestination(ctx context.Context, destination duckv1.Addressable, base http.Header) (http.Header, error) {
	headers := base.Clone()
	if headers == nil {
		headers = make(http.Header)
	}
	if destination.Audience == nil || *destination.Audience == "" {
		headers.Del("Authorization")
		return headers, nil
	}
	if h.tokenSource == nil {
		return nil, fmt.Errorf("%w (%s)", ErrOIDCTokenUnavailable, brokeroidc.AudienceKey(*destination.Audience))
	}
	token, err := h.tokenSource(ctx, *destination.Audience)
	token = strings.TrimSpace(token)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		return nil, fmt.Errorf("%w (%s)", ErrOIDCTokenUnavailable, brokeroidc.AudienceKey(*destination.Audience))
	}
	if token == "" {
		return nil, fmt.Errorf("%w (%s)", ErrOIDCTokenUnavailable, brokeroidc.AudienceKey(*destination.Audience))
	}
	headers.Set("Authorization", "Bearer "+token)
	return headers, nil
}

func dispatchResponseCode(info *kncloudevents.DispatchInfo) int {
	if info == nil {
		return 0
	}
	return info.ResponseCode
}

func eventProcessingDeadlineExceeded(err error) bool {
	return err != nil && (errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled))
}

// responseToEvent parses the subscriber's HTTP response into a CloudEvent.
// Returns (nil, nil) if the response body is empty (e.g. 202 Accepted or empty 200).
func responseToEvent(ctx context.Context, di *kncloudevents.DispatchInfo) (*cloudevents.Event, error) {
	if len(di.ResponseBody) == 0 {
		return nil, nil
	}
	resp := &http.Response{
		StatusCode: di.ResponseCode,
		Header:     di.ResponseHeader,
		Body:       io.NopCloser(bytes.NewReader(di.ResponseBody)),
	}
	msg := cehttp.NewMessageFromHttpResponse(resp)
	defer msg.Finish(nil)
	if msg.ReadEncoding() == binding.EncodingUnknown {
		return nil, nil
	}
	return binding.ToEvent(ctx, msg)
}

// Cleanup releases resources
func (h *TriggerHandler) Cleanup() {
	h.configMu.Lock()
	config := h.config
	h.config = nil
	h.configMu.Unlock()
	if config != nil && config.filter != nil {
		config.filter.Cleanup()
	}
}

func determineNatsResult(responseCode int, err error) protocol.Result {
	result := protocol.ResultACK
	if err != nil {
		code := responseCode
		if code/100 == 5 || code == http.StatusTooManyRequests || code == http.StatusRequestTimeout {
			// Retriable error, effectively this is nats protocol NACK
			result = protocol.NewReceipt(false, "%w", err)
		} else {
			// Non-retriable error
			result = err
		}
	}
	return result
}

// buildTriggerFilter builds a filter from the trigger spec.
// Priority:
// 1. trigger.Spec.Filters (new subscriptions API filters) - if defined
// 2. trigger.Spec.Filter (legacy attributes filter) - if defined
// 3. nil (pass all events) - if neither is defined
func buildTriggerFilter(logger *zap.SugaredLogger, trigger *eventingv1.Trigger) eventfilter.Filter {
	switch {
	case len(trigger.Spec.Filters) > 0:
		// Use new subscriptions API filters
		logger.Debugw("using subscriptions API filters",
			zap.Any("filters", trigger.Spec.Filters),
		)
		return subscriptionsapi.CreateSubscriptionsAPIFilters(logger.Desugar(), trigger.Spec.Filters)
	case trigger.Spec.Filter != nil && trigger.Spec.Filter.Attributes != nil:
		// Use legacy attributes filter
		logger.Debugw("using legacy attributes filter",
			zap.Any("filter", trigger.Spec.Filter),
		)
		return attributes.NewAttributesFilter(trigger.Spec.Filter.Attributes)
	default:
		// No filter defined, pass all events
		logger.Debugw("no filter defined, passing all events")
		return nil
	}
}
