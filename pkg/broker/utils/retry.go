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
	"fmt"
	"time"

	"github.com/rickb777/date/period"
	eventingv1 "knative.dev/eventing/pkg/apis/eventing/v1"
)

const (
	// RetryMaxDurationAnnotation enables duration-based delivery. While it is
	// present, JetStream keeps redelivering until the original publish time plus
	// this duration, instead of stopping after DeliverySpec.Retry attempts.
	RetryMaxDurationAnnotation = "natsjetstream.eventing.knative.dev/retry-max-duration"

	DefaultRetryMaxBackoff = 10 * time.Minute
	DeadLetterRetryDelay   = 10 * time.Minute
)

// DurationRetryConfig controls duration-based subscriber delivery.
type DurationRetryConfig struct {
	MaxDuration time.Duration
	MaxBackoff  time.Duration
}

// EffectiveDurationRetryConfig resolves the duration annotation with a Trigger
// value taking precedence over a Broker value. The effective DeliverySpec uses
// Knative's whole-spec precedence and may override the default backoff cap.
func EffectiveDurationRetryConfig(trigger *eventingv1.Trigger, broker *eventingv1.Broker) (*DurationRetryConfig, error) {
	maxDurationValue := effectiveAnnotation(trigger, broker, RetryMaxDurationAnnotation)

	if maxDurationValue == "" {
		return nil, nil
	}

	maxDuration, err := time.ParseDuration(maxDurationValue)
	if err != nil || maxDuration <= 0 {
		return nil, fmt.Errorf("invalid %s value %q: must be a positive Go duration", RetryMaxDurationAnnotation, maxDurationValue)
	}

	maxBackoff := DefaultRetryMaxBackoff
	if delivery := EffectiveDelivery(trigger, broker); delivery != nil && delivery.BackoffMax != nil {
		maxPeriod, parseErr := period.Parse(*delivery.BackoffMax)
		if parseErr != nil || maxPeriod.IsZero() || maxPeriod.IsNegative() {
			return nil, fmt.Errorf("invalid spec.delivery.backoffMax value %q: must be a positive ISO 8601 duration", *delivery.BackoffMax)
		}
		maxBackoff, _ = maxPeriod.Duration()
	}

	return &DurationRetryConfig{MaxDuration: maxDuration, MaxBackoff: maxBackoff}, nil
}

func effectiveAnnotation(trigger *eventingv1.Trigger, broker *eventingv1.Broker, key string) string {
	if trigger != nil && trigger.Annotations != nil {
		if value, ok := trigger.Annotations[key]; ok {
			return value
		}
	}
	if broker != nil && broker.Annotations != nil {
		return broker.Annotations[key]
	}
	return ""
}
