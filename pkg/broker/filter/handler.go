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

	brokerutils "knative.dev/eventing-natss/pkg/broker/utils"
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

	// Trigger configuration
	trigger    *eventingv1.Trigger
	subscriber duckv1.Addressable
	filter     eventfilter.Filter

	// Broker ingress URL for reply events
	brokerIngressURL *duckv1.Addressable

	// Dispatcher for sending events
	dispatcher *kncloudevents.Dispatcher

	// Retry configuration
	retryConfig         *kncloudevents.RetryConfig
	noRetryConfig       *kncloudevents.RetryConfig
	durationRetryConfig *brokerutils.DurationRetryConfig

	// Dead letter sink
	deadLetterSink *duckv1.Addressable

	subscription *nats.Subscription

	// Observability. tracer is used to wrap each dispatch in a span;
	// dispatchDuration records the per-dispatch HTTP wall time; processDuration
	// records the pre-dispatch work (message decode + filter evaluation).
	tracer               trace.Tracer
	dispatchDuration     metric.Float64Histogram
	processDuration      metric.Float64Histogram
	dispatchAttempts     metric.Int64Counter
	now                  func() time.Time
	deadLetterRetryDelay time.Duration
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

	return &TriggerHandler{
		logger:               logger,
		ctx:                  ctx,
		trigger:              trigger,
		subscriber:           subscriber,
		filter:               buildTriggerFilter(logger, trigger),
		brokerIngressURL:     brokerIngressURL,
		dispatcher:           dispatcher,
		retryConfig:          retryConfig,
		noRetryConfig:        noRetryConfig,
		deadLetterSink:       deadLetterSink,
		tracer:               tracer,
		dispatchDuration:     dispatchDuration,
		processDuration:      processDuration,
		now:                  time.Now,
		deadLetterRetryDelay: brokerutils.DeadLetterRetryDelay,
	}, nil
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
				attribute.String("kn.trigger.name", h.trigger.Name),
				attribute.String("kn.trigger.namespace", h.trigger.Namespace),
			))
	}
	defer recordProcess()

	// Convert NATS message to CloudEvents message
	message := cejs.NewMessage(msg)
	if message.ReadEncoding() == binding.EncodingUnknown {
		logger.Errorw("received a message with unknown encoding")
		if err := msg.Term(); err != nil {
			logger.Errorw("failed to terminate message", zap.Error(err))
		}
		return
	}

	// Convert to CloudEvent for filtering
	event, err := binding.ToEvent(ctx, message)
	if err != nil {
		logger.Errorw("failed to convert message to CloudEvent", zap.Error(err))
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
		attribute.String("kn.trigger.name", h.trigger.Name),
		attribute.String("kn.trigger.namespace", h.trigger.Namespace),
		attribute.String("kn.trigger.uid", string(h.trigger.UID)),
		attribute.String("messaging.destination.name", h.subscriber.URL.String()),
		attribute.String("cloudevents.event.id", event.ID()),
		attribute.String("cloudevents.event.type", event.Type()),
		attribute.String("cloudevents.event.source", event.Source()),
	)

	// Apply filter
	if h.filter != nil {
		filterResult := h.filter.Filter(ctx, *event)
		if filterResult == eventfilter.FailFilter {
			span.SetAttributes(attribute.String("filter.outcome", "fail"))
			logger.Debugw("event filtered out",
				zap.String("type", event.Type()),
				zap.String("source", event.Source()),
			)
			// Ack the message since it was intentionally filtered
			if err := msg.Ack(); err != nil {
				logger.Errorw("failed to ack filtered message", zap.Error(err))
			}
			return
		}
		span.SetAttributes(attribute.String("filter.outcome", "pass"))
	}

	// Dispatch to subscriber
	logger.Debugw("dispatching event to subscriber",
		zap.String("subscriber", h.subscriber.URL.String()),
		zap.String("type", event.Type()),
		zap.String("source", event.Source()),
		zap.String("id", event.ID()),
	)

	// Pre-dispatch work is done; record it before the HTTP call so dispatch
	// time (tracked separately by dispatchDuration) is excluded.
	recordProcess()

	dispatchInfo, err := h.dispatchEvent(ctx, event, msg)
	if dispatchInfo != nil && dispatchInfo.ResponseCode != 0 {
		span.SetAttributes(attribute.Int("http.response.status_code", dispatchInfo.ResponseCode))
	}
	if eventProcessingDeadlineExceeded(err) {
		span.SetStatus(codes.Error, "dispatch deadline exceeded")
		logger.Warnw("dispatch context expired before subscriber response; message was scheduled for redelivery", zap.Error(err))
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
func (h *TriggerHandler) dispatchEvent(ctx context.Context, event *cloudevents.Event, msg *nats.Msg) (*kncloudevents.DispatchInfo, error) {
	logger := logging.FromContext(ctx)

	additionalHeaders := tracing.ConvertEventToHttpHeader(event)
	te := TypeExtractorTransformer("")

	retryNumber, messageAge := h.deliveryState(msg)

	// Count mode retains Knative's DeliverySpec.Retry behavior. Duration mode
	// ignores that count and uses the original JetStream publish timestamp.
	maxRetries := 0
	if h.retryConfig != nil {
		maxRetries = h.retryConfig.RetryMax
	}
	lastTry := retryNumber > maxRetries
	span := trace.SpanFromContext(ctx)
	span.SetAttributes(attribute.Int("delivery.attempt", retryNumber))

	if h.durationRetryConfig != nil && messageAge >= h.durationRetryConfig.MaxDuration {
		span.SetAttributes(attribute.Bool("delivery.deadline_exceeded", true))
		h.routeToDeadLetter(ctx, event, msg, additionalHeaders, &te, span)
		return &kncloudevents.DispatchInfo{}, nil
	}

	// Dispatch the message to trigger's destination
	dispatchInfo, err := h.dispatcher.SendEvent(ctx, *event, h.subscriber,
		kncloudevents.WithHeader(additionalHeaders),
		kncloudevents.WithTransformers(&te),
		kncloudevents.WithRetryConfig(h.noRetryConfig),
	)

	// Record HTTP wall time with low-cardinality labels. Duration is left at
	// kncloudevents.NoDuration (-1) when the request never made it onto the
	// wire (e.g. request build / client creation failure); skip those.
	if h.dispatchDuration != nil && dispatchInfo != nil && dispatchInfo.Duration >= 0 {
		h.dispatchDuration.Record(ctx, dispatchInfo.Duration.Seconds(),
			metric.WithAttributes(
				attribute.String("kn.trigger.name", h.trigger.Name),
				attribute.String("kn.trigger.namespace", h.trigger.Namespace),
			))
	}

	responseCode := dispatchResponseCode(dispatchInfo)
	result := determineNatsResult(responseCode, err)
	h.recordDispatchAttempt(ctx, responseCode, subscriberDeliveryResult(result))

	// Extract pass-through headers (tracing, knative-*, x-b3-*, etc.) from
	// the subscriber's response so they are available in every code path.
	responseHeaders := http.Header(nil)
	if dispatchInfo != nil {
		responseHeaders = eventingutils.PassThroughHeaders(dispatchInfo.ResponseHeader)
	}

	// Handle ack/nack/term based on result
	switch {
	case protocol.IsACK(result):
		span.SetAttributes(attribute.String("nats.result", "ack"))
		// If the subscriber returned a CloudEvent response, forward it to broker ingress for re-routing.
		if h.brokerIngressURL != nil {
			responseEvent, parseErr := responseToEvent(ctx, dispatchInfo)
			if parseErr != nil {
				logger.Warnw("failed to parse response event from subscriber", zap.Error(parseErr))
			} else if responseEvent != nil {
				replyDispatchInfo, replyErr := h.dispatcher.SendEvent(ctx, *responseEvent, *h.brokerIngressURL,
					kncloudevents.WithRetryConfig(&defaultRetry),
					kncloudevents.WithHeader(responseHeaders),
					kncloudevents.WithTransformers(&te),
				)
				if replyErr != nil {
					logger.Errorw("failed to send reply to broker ingress",
						zap.Error(replyErr),
						zap.Int("response_code", replyDispatchInfo.ResponseCode),
					)
				}
			}
		}
		if err := msg.Ack(); err != nil {
			logger.Errorw("failed to ack message", zap.Error(err))
		}
	case protocol.IsNACK(result):
		if h.durationRetryConfig != nil {
			_, currentAge := h.deliveryState(msg)
			if currentAge >= h.durationRetryConfig.MaxDuration {
				span.SetAttributes(attribute.Bool("delivery.deadline_exceeded", true))
				h.routeToDeadLetter(ctx, event, msg, additionalHeaders, &te, span)
				break
			}
			remaining := h.durationRetryConfig.MaxDuration - currentAge
			nakDelay := calculateDurationNakDelay(retryNumber, h.retryConfig, h.durationRetryConfig.MaxBackoff, remaining)
			span.SetAttributes(attribute.String("nats.result", "nak"))
			h.nakWithDelay(msg, nakDelay, "subscriber delivery failed")
			break
		}

		if lastTry {
			if h.deadLetterSink != nil {
				h.routeToDeadLetter(ctx, event, msg, additionalHeaders, &te, span)
			} else {
				span.SetAttributes(attribute.String("nats.result", "ack_retries_exhausted"))
				if err := msg.Ack(); err != nil {
					logger.Errorw("failed to ack message after retries exhausted", zap.Error(err))
				}
			}
		} else {
			span.SetAttributes(attribute.String("nats.result", "nak"))
			// Use the same Knative backoff function for count- and duration-based
			// delivery so both modes apply DeliverySpec.backoffMax consistently.
			nakDelay := jsutils.CalculateNakDelayForRetryNumber(retryNumber, h.retryConfig)
			h.nakWithDelay(msg, nakDelay, "subscriber delivery failed")
		}
	default:
		// Permanent subscriber responses go to DLS immediately; retrying the
		// same 400/404/413 cannot make the request valid.
		if h.deadLetterSink != nil {
			h.routeToDeadLetter(ctx, event, msg, additionalHeaders, &te, span)
		} else {
			span.SetAttributes(attribute.String("nats.result", "term"))
			if err := msg.Term(); err != nil {
				logger.Errorw("failed to term message", zap.Error(err))
			}
		}
	}

	return dispatchInfo, err
}

func (h *TriggerHandler) routeToDeadLetter(ctx context.Context, event *cloudevents.Event, msg *nats.Msg, headers http.Header, te *TypeExtractorTransformer, span trace.Span) {
	logger := logging.FromContext(ctx)
	if h.deadLetterSink == nil {
		span.SetAttributes(attribute.String("nats.result", "term_without_dls"))
		if err := msg.Term(); err != nil {
			logger.Errorw("failed to terminate message without a dead letter sink", zap.Error(err))
		}
		return
	}

	// Subscriber timeouts cancel ctx at AckWait. DLS still needs an independent
	// attempt, so retain trace values while detaching cancellation.
	dlsTimeout := 2 * time.Minute
	if h.noRetryConfig != nil && h.noRetryConfig.RequestTimeout > 0 {
		dlsTimeout = h.noRetryConfig.RequestTimeout
	}
	dlsCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), dlsTimeout)
	defer cancel()
	dlsDispatchInfo, dlsErr := h.dispatcher.SendEvent(dlsCtx, *event, *h.deadLetterSink,
		kncloudevents.WithRetryConfig(h.noRetryConfig),
		kncloudevents.WithHeader(headers),
		kncloudevents.WithTransformers(te),
	)
	dlsStatus := dispatchResponseCode(dlsDispatchInfo)
	if dlsErr == nil && dlsStatus >= http.StatusOK && dlsStatus < http.StatusMultipleChoices {
		h.recordDispatchAttempt(dlsCtx, dlsStatus, "dead_letter_success")
		span.SetAttributes(attribute.String("nats.result", "ack_after_dls"))
		if err := msg.Ack(); err != nil {
			logger.Errorw("failed to ack message after dead letter delivery", zap.Error(err))
		}
		return
	}

	h.recordDispatchAttempt(dlsCtx, dlsStatus, "dead_letter_failure")
	span.SetAttributes(attribute.String("nats.result", "nak_after_dls_failure"))
	logger.Errorw("failed to send to dead letter sink; retaining original message",
		zap.Error(dlsErr),
		zap.Int("response_code", dlsStatus),
	)
	delay := h.deadLetterRetryDelay
	if delay <= 0 {
		delay = brokerutils.DeadLetterRetryDelay
	}
	h.nakWithDelay(msg, delay, "dead letter delivery failed")
}

func (h *TriggerHandler) deliveryState(msg *nats.Msg) (int, time.Duration) {
	retryNumber := 1
	var published time.Time
	if meta, err := msg.Metadata(); err == nil {
		retryNumber = int(meta.NumDelivered)
		published = meta.Timestamp
	}
	if published.IsZero() {
		return retryNumber, 0
	}
	now := time.Now
	if h.now != nil {
		now = h.now
	}
	age := now().Sub(published)
	if age < 0 {
		age = 0
	}
	return retryNumber, age
}

func (h *TriggerHandler) nakWithDelay(msg *nats.Msg, delay time.Duration, reason string) {
	if err := msg.NakWithDelay(delay); err != nil {
		h.logger.Errorw("failed to nack message", zap.Error(err), zap.Duration("delay", delay), zap.String("reason", reason))
	}
}

func (h *TriggerHandler) recordDispatchAttempt(ctx context.Context, status int, result string) {
	if h.dispatchAttempts == nil {
		return
	}
	h.dispatchAttempts.Add(ctx, 1, metric.WithAttributes(
		attribute.String("kn.trigger.name", h.trigger.Name),
		attribute.String("kn.trigger.namespace", h.trigger.Namespace),
		attribute.Int("http.response.status_code", status),
		attribute.String("delivery.result", result),
	))
}

func subscriberDeliveryResult(result protocol.Result) string {
	switch {
	case protocol.IsACK(result):
		return "subscriber_success"
	case protocol.IsNACK(result):
		return "subscriber_retry"
	default:
		return "subscriber_permanent"
	}
}

func dispatchResponseCode(info *kncloudevents.DispatchInfo) int {
	if info == nil {
		return 0
	}
	return info.ResponseCode
}

func calculateDurationNakDelay(attempt int, config *kncloudevents.RetryConfig, maxBackoff, remaining time.Duration) time.Duration {
	delay := time.Second
	if config != nil && config.Backoff != nil {
		delay = config.Backoff(attempt, nil)
	}

	if maxBackoff > 0 && delay > maxBackoff {
		delay = maxBackoff
	}
	if remaining > 0 && delay > remaining {
		delay = remaining
	}
	return max(delay, time.Millisecond)
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
	if h.filter != nil {
		h.filter.Cleanup()
	}
}

func determineNatsResult(responseCode int, err error) protocol.Result {
	if err == nil && responseCode >= http.StatusOK && responseCode < http.StatusMultipleChoices {
		return protocol.ResultACK
	}
	if responseCode == 0 || responseCode/100 == 5 || responseCode == http.StatusTooManyRequests || responseCode == http.StatusRequestTimeout {
		if err == nil {
			err = fmt.Errorf("retryable HTTP response %d", responseCode)
		}
		return protocol.NewReceipt(false, "%w", err)
	}
	if err != nil {
		return err
	}
	return fmt.Errorf("permanent HTTP response %d", responseCode)
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
