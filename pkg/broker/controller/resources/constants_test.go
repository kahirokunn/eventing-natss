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

package resources

import (
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestFilterName(t *testing.T) {
	tests := []struct {
		brokerName string
		want       string
	}{
		{
			brokerName: "mybroker",
			want:       "mybroker-broker-filter",
		},
		{
			brokerName: "test-broker",
			want:       "test-broker-broker-filter",
		},
	}

	for _, tt := range tests {
		t.Run(tt.brokerName, func(t *testing.T) {
			got := FilterName(tt.brokerName)
			if got != tt.want {
				t.Errorf("FilterName() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFilterNameLongBrokerBoundary(t *testing.T) {
	legacyBoundary := strings.Repeat("a", 49)
	if got, want := FilterName(legacyBoundary), legacyBoundary+"-broker-filter"; got != want {
		t.Fatalf("FilterName(49 chars) = %q, want legacy name %q", got, want)
	}
	if got := FilterName(legacyBoundary); len(got) != 63 {
		t.Fatalf("len(FilterName(49 chars)) = %d, want 63 (%q)", len(got), got)
	}

	for _, length := range []int{50, 63} {
		brokerName := strings.Repeat("a", length)
		got := FilterName(brokerName)
		if len(got) > 63 {
			t.Fatalf("len(FilterName(%d chars)) = %d, want at most 63 (%q)", length, len(got), got)
		}
		if got == brokerName+"-broker-filter" {
			t.Fatalf("FilterName(%d chars) was not shortened: %q", length, got)
		}
		if again := FilterName(brokerName); again != got {
			t.Fatalf("FilterName(%d chars) is unstable: first %q, second %q", length, got, again)
		}
	}

	sharedPrefix := strings.Repeat("a", 49)
	left := FilterName(sharedPrefix + "b")
	right := FilterName(sharedPrefix + "c")
	if left == right {
		t.Fatalf("different long Broker names collided at %q", left)
	}
}

func TestBrokerLabels(t *testing.T) {
	brokerName := "test-broker"
	got := BrokerLabels(brokerName)

	want := map[string]string{
		BrokerLabelKey: brokerName,
	}

	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("BrokerLabels() mismatch (-want +got):\n%s", diff)
	}
}

func TestFilterLabels(t *testing.T) {
	brokerName := "test-broker"
	got := FilterLabels(brokerName)

	want := map[string]string{
		BrokerLabelKey: brokerName,
		RoleLabelKey:   FilterRoleLabelValue,
	}

	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("FilterLabels() mismatch (-want +got):\n%s", diff)
	}
}

func TestMergeMaps(t *testing.T) {
	tests := []struct {
		name     string
		base     map[string]string
		override map[string]string
		want     map[string]string
	}{
		{
			name:     "both empty",
			base:     map[string]string{},
			override: map[string]string{},
			want:     map[string]string{},
		},
		{
			name:     "empty override",
			base:     map[string]string{"a": "1", "b": "2"},
			override: map[string]string{},
			want:     map[string]string{"a": "1", "b": "2"},
		},
		{
			name:     "empty base",
			base:     map[string]string{},
			override: map[string]string{"c": "3"},
			want:     map[string]string{"c": "3"},
		},
		{
			name:     "no overlap",
			base:     map[string]string{"a": "1"},
			override: map[string]string{"b": "2"},
			want:     map[string]string{"a": "1", "b": "2"},
		},
		{
			name:     "required base takes precedence",
			base:     map[string]string{"a": "1", "b": "2"},
			override: map[string]string{"b": "override", "c": "3"},
			want:     map[string]string{"a": "1", "b": "2", "c": "3"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mergeMaps(tt.base, tt.override)
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("mergeMaps() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestConstants(t *testing.T) {
	// Verify constant values don't accidentally change
	if BrokerLabelKey != "eventing.knative.dev/broker" {
		t.Errorf("BrokerLabelKey = %v, want eventing.knative.dev/broker", BrokerLabelKey)
	}

	if RoleLabelKey != "eventing.knative.dev/role" {
		t.Errorf("RoleLabelKey = %v, want eventing.knative.dev/role", RoleLabelKey)
	}

	if FilterRoleLabelValue != "filter" {
		t.Errorf("FilterRoleLabelValue = %v, want filter", FilterRoleLabelValue)
	}

	if MetricsPortNumber != 9090 {
		t.Errorf("MetricsPortNumber = %v, want 9090", MetricsPortNumber)
	}
}
