//go:build e2e
// +build e2e

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

package e2e

import (
	"strings"
	"testing"

	cloudevents "github.com/cloudevents/sdk-go/v2"

	"knative.dev/reconciler-test/pkg/eventshub"
)

func TestHasEventIndex(t *testing.T) {
	tests := []struct {
		name        string
		nilEvent    bool
		setIndex    bool
		rawIndex    interface{}
		wantIndex   int
		wantErrPart string
	}{
		{name: "nil event", nilEvent: true, wantErrPart: "event is nil"},
		{name: "missing extension", wantErrPart: "event has no index extension"},
		{name: "invalid integer conversion", setIndex: true, rawIndex: "not-an-integer", wantErrPart: "is not an integer"},
		{name: "integer mismatch", setIndex: true, rawIndex: int32(8), wantIndex: 7, wantErrPart: "event index 8, want 7"},
		{name: "producer integer matches", setIndex: true, rawIndex: 7, wantIndex: 7},
		{name: "canonical integer string matches", setIndex: true, rawIndex: "7", wantIndex: 7},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			info := eventshub.EventInfo{}
			if !test.nilEvent {
				event := cloudevents.NewEvent()
				if test.setIndex {
					event.SetExtension("index", test.rawIndex)
				}
				if stored := event.Extensions()["index"]; test.setIndex && stored == nil {
					t.Fatalf("SetExtension(index, %#v) did not retain the extension", test.rawIndex)
				}
				info.Event = &event
			}

			err := hasEventIndex(test.wantIndex)(info)
			if test.wantErrPart == "" {
				if err != nil {
					t.Fatalf("hasEventIndex(%d) = %v, want match", test.wantIndex, err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.wantErrPart) {
				t.Fatalf("hasEventIndex(%d) error = %v, want substring %q", test.wantIndex, err, test.wantErrPart)
			}
		})
	}
}
