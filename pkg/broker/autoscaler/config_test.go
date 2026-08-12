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

package autoscaler

import "testing"

func TestResolveSettings(t *testing.T) {
	t.Run("disabled when class is absent", func(t *testing.T) {
		got, err := ResolveSettings(nil)
		if err != nil {
			t.Fatal(err)
		}
		if got.Enabled {
			t.Fatal("KEDA should be disabled without an explicit class")
		}
	})

	t.Run("Kafka-compatible defaults", func(t *testing.T) {
		got, err := ResolveSettings(map[string]string{ClassAnnotation: KEDAClass})
		if err != nil {
			t.Fatal(err)
		}
		if !got.Enabled || got.MinScale != 0 || got.MaxScale != 50 || got.PollingInterval != 10 || got.CooldownPeriod != 30 {
			t.Fatalf("unexpected defaults: %+v", got)
		}
	})

	t.Run("overrides", func(t *testing.T) {
		got, err := ResolveSettings(map[string]string{
			ClassAnnotation:           KEDAClass,
			MinScaleAnnotation:        "1",
			MaxScaleAnnotation:        "12",
			PollingIntervalAnnotation: "5",
			CooldownPeriodAnnotation:  "60",
		})
		if err != nil {
			t.Fatal(err)
		}
		if got.MinScale != 1 || got.MaxScale != 12 || got.PollingInterval != 5 || got.CooldownPeriod != 60 {
			t.Fatalf("unexpected settings: %+v", got)
		}
	})

	invalid := []map[string]string{
		{MinScaleAnnotation: "-1"},
		{MaxScaleAnnotation: "0"},
		{MinScaleAnnotation: "3", MaxScaleAnnotation: "2"},
		{PollingIntervalAnnotation: "0"},
		{CooldownPeriodAnnotation: "-1"},
		{MaxScaleAnnotation: "many"},
	}
	for _, annotations := range invalid {
		annotations[ClassAnnotation] = KEDAClass
		if _, err := ResolveSettings(annotations); err == nil {
			t.Errorf("ResolveSettings(%v) expected an error", annotations)
		}
	}
}

func TestResolveLagThresholds(t *testing.T) {
	lag, activation, err := ResolveLagThresholds(
		map[string]string{LagThresholdAnnotation: "5"},
		map[string]string{LagThresholdAnnotation: "20", ActivationLagThresholdAnnotation: "2"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if lag != 5 || activation != 2 {
		t.Fatalf("got lag=%d activation=%d, want 5 and 2", lag, activation)
	}

	if _, _, err := ResolveLagThresholds(map[string]string{LagThresholdAnnotation: "0"}, nil); err == nil {
		t.Error("zero lag threshold should be rejected")
	}
	if _, _, err := ResolveLagThresholds(map[string]string{ActivationLagThresholdAnnotation: "-1"}, nil); err == nil {
		t.Error("negative activation lag threshold should be rejected")
	}
}

func TestValidateMonitoringConfig(t *testing.T) {
	got, err := ValidateMonitoringConfig("nats.nats-io.svc.cluster.local:8222", "", false)
	if err != nil {
		t.Fatal(err)
	}
	if got.Account != DefaultAccount {
		t.Fatalf("account = %q, want %q", got.Account, DefaultAccount)
	}

	for _, endpoint := range []string{"", "http://nats:8222", "nats-without-port"} {
		if _, err := ValidateMonitoringConfig(endpoint, "", false); err == nil {
			t.Errorf("ValidateMonitoringConfig(%q) expected an error", endpoint)
		}
	}
}
