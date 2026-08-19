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
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	eventingduckv1 "knative.dev/eventing/pkg/apis/duck/v1"
	eventingv1 "knative.dev/eventing/pkg/apis/eventing/v1"
)

func TestEffectiveDurationRetryConfig(t *testing.T) {
	tests := []struct {
		name            string
		trigger         map[string]string
		broker          map[string]string
		triggerDelivery *eventingduckv1.DeliverySpec
		brokerDelivery  *eventingduckv1.DeliverySpec
		want            *DurationRetryConfig
		wantErr         bool
	}{
		{name: "disabled"},
		{
			name: "broker configuration",
			broker: map[string]string{
				RetryMaxDurationAnnotation: "1440h",
			},
			brokerDelivery: &eventingduckv1.DeliverySpec{BackoffMax: stringPointer("PT10M")},
			want:           &DurationRetryConfig{MaxDuration: 60 * 24 * time.Hour, MaxBackoff: 10 * time.Minute},
		},
		{
			name: "trigger overrides broker",
			trigger: map[string]string{
				RetryMaxDurationAnnotation: "2h",
			},
			broker: map[string]string{
				RetryMaxDurationAnnotation: "1h",
			},
			triggerDelivery: &eventingduckv1.DeliverySpec{BackoffMax: stringPointer("PT30S")},
			brokerDelivery:  &eventingduckv1.DeliverySpec{BackoffMax: stringPointer("PT1M")},
			want:            &DurationRetryConfig{MaxDuration: 2 * time.Hour, MaxBackoff: 30 * time.Second},
		},
		{
			name:   "default max backoff",
			broker: map[string]string{RetryMaxDurationAnnotation: "1h"},
			want:   &DurationRetryConfig{MaxDuration: time.Hour, MaxBackoff: DefaultRetryMaxBackoff},
		},
		{
			name:           "backoff max without duration leaves duration mode disabled",
			brokerDelivery: &eventingduckv1.DeliverySpec{BackoffMax: stringPointer("PT1M")},
		},
		{
			name:            "trigger delivery replaces broker backoff max wholesale",
			broker:          map[string]string{RetryMaxDurationAnnotation: "1h"},
			triggerDelivery: &eventingduckv1.DeliverySpec{Retry: int32Pointer(1)},
			brokerDelivery:  &eventingduckv1.DeliverySpec{BackoffMax: stringPointer("PT1M")},
			want:            &DurationRetryConfig{MaxDuration: time.Hour, MaxBackoff: DefaultRetryMaxBackoff},
		},
		{name: "invalid duration", broker: map[string]string{RetryMaxDurationAnnotation: "forever"}, wantErr: true},
		{
			name:           "non-positive backoff max",
			broker:         map[string]string{RetryMaxDurationAnnotation: "1h"},
			brokerDelivery: &eventingduckv1.DeliverySpec{BackoffMax: stringPointer("PT0S")},
			wantErr:        true,
		},
		{
			name:           "invalid backoff max",
			broker:         map[string]string{RetryMaxDurationAnnotation: "1h"},
			brokerDelivery: &eventingduckv1.DeliverySpec{BackoffMax: stringPointer("forever")},
			wantErr:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			trigger := &eventingv1.Trigger{
				ObjectMeta: metav1.ObjectMeta{Annotations: tt.trigger},
				Spec:       eventingv1.TriggerSpec{Delivery: tt.triggerDelivery},
			}
			broker := &eventingv1.Broker{
				ObjectMeta: metav1.ObjectMeta{Annotations: tt.broker},
				Spec:       eventingv1.BrokerSpec{Delivery: tt.brokerDelivery},
			}
			got, err := EffectiveDurationRetryConfig(trigger, broker)
			if (err != nil) != tt.wantErr {
				t.Fatalf("EffectiveDurationRetryConfig() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if got == nil || tt.want == nil {
				if got != tt.want {
					t.Fatalf("EffectiveDurationRetryConfig() = %#v, want %#v", got, tt.want)
				}
				return
			}
			if *got != *tt.want {
				t.Errorf("EffectiveDurationRetryConfig() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func stringPointer(value string) *string {
	return &value
}

func int32Pointer(value int32) *int32 {
	return &value
}
