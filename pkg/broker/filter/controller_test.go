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
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/client-go/tools/cache"

	"knative.dev/pkg/logging"

	eventingduckv1 "knative.dev/eventing/pkg/apis/duck/v1"
	eventingv1 "knative.dev/eventing/pkg/apis/eventing/v1"
)

func TestEnvConfig_MaxConcurrency(t *testing.T) {
	tests := []struct {
		name               string
		configMaxConcur    int
		wantMaxConcurrency int
	}{
		{
			name:               "zero uses default",
			configMaxConcur:    0,
			wantMaxConcurrency: DefaultMaxConcurrency,
		},
		{
			name:               "positive value is used",
			configMaxConcur:    40,
			wantMaxConcurrency: 40,
		},
		{
			name:               "negative uses default",
			configMaxConcur:    -1,
			wantMaxConcurrency: DefaultMaxConcurrency,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			maxConcurrency := DefaultMaxConcurrency

			if tt.configMaxConcur > 0 {
				maxConcurrency = tt.configMaxConcur
			}

			if maxConcurrency != tt.wantMaxConcurrency {
				t.Errorf("maxConcurrency = %v, want %v", maxConcurrency, tt.wantMaxConcurrency)
			}
		})
	}
}

func TestEnqueueBrokerTriggers(t *testing.T) {
	lister := newFakeTriggerLister()
	inherited := newTestTrigger("owned", "inherits", "broker")
	overridden := newTestTrigger("owned", "overrides", "broker")
	timeout := "PT5S"
	overridden.Spec.Delivery = &eventingduckv1.DeliverySpec{Timeout: &timeout}
	for _, trigger := range []*eventingv1.Trigger{inherited, overridden,
		newTestTrigger("owned", "other-broker", "other"),
		newTestTrigger("elsewhere", "other-namespace", "broker")} {
		lister.addTrigger(trigger)
	}
	r := &FilterReconciler{
		logger: logging.FromContext(logCtx()), triggerLister: lister,
		brokerScope: &BrokerScope{Namespace: "owned", Name: "broker"},
	}
	broker := newTestBroker("owned", "broker", "")
	for _, tc := range []struct {
		name   string
		object interface{}
		want   []string
	}{
		{name: "Broker delivery changes", object: broker, want: []string{"inherits", "overrides"}},
		{name: "Broker deletion", object: cache.DeletedFinalStateUnknown{Obj: broker}, want: []string{"inherits", "overrides"}},
		{name: "other Broker", object: newTestBroker("owned", "other", "")},
		{name: "other namespace", object: newTestBroker("elsewhere", "broker", "")},
		{name: "other resource", object: inherited},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var enqueued []string
			r.enqueueBrokerTriggers(tc.object, func(obj interface{}) {
				enqueued = append(enqueued, obj.(*eventingv1.Trigger).Name)
			})
			require.ElementsMatch(t, tc.want, enqueued)
		})
	}
}
