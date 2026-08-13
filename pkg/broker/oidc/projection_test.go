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

package oidc

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"knative.dev/pkg/apis"

	eventingv1 "knative.dev/eventing/pkg/apis/eventing/v1"
)

func TestNormalizeAudiencesCountBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name    string
		count   int
		wantErr bool
	}{
		{name: "zero", count: 0},
		{name: "maximum", count: MaxAudiences},
		{name: "over maximum", count: MaxAudiences + 1, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := make([]string, tc.count)
			for i := range input {
				// Reverse lexical order so the successful boundary also proves
				// normalization is stable and sorted.
				input[i] = fmt.Sprintf("audience-%03d", tc.count-i)
			}
			got, err := NormalizeAudiences(input)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("NormalizeAudiences(%d unique) expected error", tc.count)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != tc.count {
				t.Fatalf("normalized audiences = %d, want %d", len(got), tc.count)
			}
			for i := 1; i < len(got); i++ {
				if got[i-1] >= got[i] {
					t.Fatalf("normalized audiences are not strictly sorted: %#v", got)
				}
			}
		})
	}

	duplicates := make([]string, MaxAudiences+1)
	for i := range duplicates {
		duplicates[i] = "same-audience"
	}
	got, err := NormalizeAudiences(duplicates)
	if err != nil {
		t.Fatalf("duplicate values must count once: %v", err)
	}
	if !reflect.DeepEqual(got, []string{"same-audience"}) {
		t.Errorf("normalized duplicates = %#v", got)
	}
}

func TestNormalizeAudiencesByteLengthBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name    string
		value   string
		wantErr bool
	}{
		{name: "4096 bytes", value: strings.Repeat("a", MaxAudienceBytes)},
		{name: "4097 bytes", value: strings.Repeat("a", MaxAudienceBytes+1), wantErr: true},
		{name: "multibyte over byte limit", value: strings.Repeat("界", MaxAudienceBytes/3+1), wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := NormalizeAudiences([]string{tc.value})
			if tc.wantErr {
				if err == nil {
					t.Fatalf("NormalizeAudiences(%d bytes) expected error", len(tc.value))
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != 1 || got[0] != tc.value {
				t.Errorf("normalized value differs at %d-byte boundary", len(tc.value))
			}
		})
	}
}

func TestAudiencesFromTriggersUsesOnlyCurrentResolvedStatus(t *testing.T) {
	subscriberAudience := "https://subscriber.example"
	dlsAudience := "https://dead-letter.example"
	staleAudience := "https://stale.example"
	unresolvedAudience := "https://unresolved.example"

	current := &eventingv1.Trigger{ObjectMeta: metav1.ObjectMeta{Generation: 2}}
	current.Status.ObservedGeneration = 2
	current.Status.SubscriberAudience = &subscriberAudience
	current.Status.DeadLetterSinkAudience = &dlsAudience
	current.Status.MarkSubscriberResolvedSucceeded()
	current.Status.MarkDeadLetterSinkResolvedSucceeded()

	stale := &eventingv1.Trigger{ObjectMeta: metav1.ObjectMeta{Generation: 3}}
	stale.Status.ObservedGeneration = 2
	stale.Status.SubscriberAudience = &staleAudience
	stale.Status.MarkSubscriberResolvedSucceeded()

	unresolved := &eventingv1.Trigger{ObjectMeta: metav1.ObjectMeta{Generation: 1}}
	unresolved.Status.ObservedGeneration = 1
	unresolved.Status.SubscriberAudience = &unresolvedAudience
	unresolved.Status.MarkSubscriberResolvedFailed("TestUnresolved", "test")
	unresolved.Status.MarkDeadLetterSinkNotConfigured()

	got, err := AudiencesFromTriggers([]*eventingv1.Trigger{nil, stale, unresolved, current})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{dlsAudience, subscriberAudience}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("audiences = %#v, want only current resolved status %#v", got, want)
	}

	// Guard the status helper assumptions: a condition must be explicitly true,
	// not merely present or inferred from a non-empty audience.
	if conditionTrue(nil) {
		t.Error("nil condition is true")
	}
	if conditionTrue(&apis.Condition{Status: corev1.ConditionFalse}) {
		t.Error("false condition is true")
	}
}

func TestAudienceKeyIsStableOpaqueFullSHA256(t *testing.T) {
	const audience = "https://tenant.example/private/path"
	got := AudienceKey(audience)
	if got != AudienceKey(audience) {
		t.Fatal("AudienceKey is not stable")
	}
	if strings.Contains(got, audience) || strings.Contains(got, "tenant") {
		t.Errorf("AudienceKey leaks raw audience: %q", got)
	}
	if !strings.HasPrefix(got, "sha256-") || len(strings.TrimPrefix(got, "sha256-")) != 64 {
		t.Errorf("AudienceKey = %q, want sha256- plus full 64-character digest", got)
	}
	if got == AudienceKey(audience+"-other") {
		t.Error("distinct audiences have the same key")
	}
}
