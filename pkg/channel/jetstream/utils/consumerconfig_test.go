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

	eventingduckv1 "knative.dev/eventing/pkg/apis/duck/v1"
	"knative.dev/eventing/pkg/kncloudevents"
)

func TestCalculateNakDelayForRetryNumber(t *testing.T) {
	policy := eventingduckv1.BackoffPolicyExponential
	delay := "PT1S"
	backoffMax := "PT10S"
	config, err := kncloudevents.RetryConfigFromDeliverySpec(eventingduckv1.DeliverySpec{
		BackoffPolicy: &policy,
		BackoffDelay:  &delay,
		BackoffMax:    &backoffMax,
	})
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name    string
		attempt int
		config  *kncloudevents.RetryConfig
		want    time.Duration
	}{
		{name: "nil config", attempt: 1, want: 0},
		{name: "first retry", attempt: 1, config: &config, want: 2 * time.Second},
		{name: "capped retry", attempt: 4, config: &config, want: 10 * time.Second},
		{name: "huge retry stays capped", attempt: int(^uint(0) >> 1), config: &config, want: 10 * time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CalculateNakDelayForRetryNumber(tt.attempt, tt.config); got != tt.want {
				t.Errorf("CalculateNakDelayForRetryNumber() = %v, want %v", got, tt.want)
			}
		})
	}
}
