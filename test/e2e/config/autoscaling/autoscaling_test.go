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

package autoscaling

import (
	"context"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	"sigs.k8s.io/yaml"

	"knative.dev/reconciler-test/pkg/manifest"
)

func TestProducerTemplateEventTypeIsOptional(t *testing.T) {
	tests := []struct {
		name          string
		eventType     string
		wantEventType bool
	}{
		{name: "existing caller uses producer default"},
		{name: "scenario identity is explicit", eventType: "knative.natsbroker.autoscaling.initial", wantEventType: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			data := map[string]interface{}{
				"namespace":     "test-namespace",
				"producerName":  "test-producer",
				"producerCount": 30,
			}
			if test.wantEventType {
				data["producerEventType"] = test.eventType
			}
			files, err := manifest.ExecuteYAML(context.Background(), producerYamls, nil, data)
			if err != nil {
				t.Fatal(err)
			}
			rendered, found := files["producer.yaml"]
			if !found {
				t.Fatalf("producer.yaml not rendered: keys=%v", files)
			}
			job := &batchv1.Job{}
			if err := yaml.Unmarshal([]byte(rendered), job); err != nil {
				t.Fatalf("rendered producer is invalid YAML: %v\n%s", err, rendered)
			}
			if len(job.Spec.Template.Spec.Containers) != 1 {
				t.Fatalf("producer containers = %d, want 1", len(job.Spec.Template.Spec.Containers))
			}
			var eventTypes []string
			for _, env := range job.Spec.Template.Spec.Containers[0].Env {
				if env.Name == "EVENT_TYPE" {
					eventTypes = append(eventTypes, env.Value)
				}
			}
			if !test.wantEventType && len(eventTypes) != 0 {
				t.Errorf("compatibility producer EVENT_TYPE entries = %v, want none so the binary default applies", eventTypes)
			}
			if test.wantEventType && (len(eventTypes) != 1 || eventTypes[0] != test.eventType) {
				t.Errorf("explicit producer EVENT_TYPE entries = %v, want [%q]", eventTypes, test.eventType)
			}
		})
	}
}
