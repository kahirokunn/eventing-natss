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
	"embed"

	"knative.dev/reconciler-test/pkg/environment"
	"knative.dev/reconciler-test/pkg/feature"
	"knative.dev/reconciler-test/pkg/manifest"
)

//go:embed *.yaml
var yamls embed.FS

//go:embed brokers.yaml triggers.yaml
var brokerYamls embed.FS

//go:embed producer.yaml
var producerYamls embed.FS

func InstallBrokersAndTriggers() feature.StepFn {
	return func(ctx context.Context, t feature.T) {
		registerImages(ctx, t)
		if _, err := manifest.InstallYamlFS(ctx, brokerYamls, nil); err != nil {
			t.Fatal(err)
		}
	}
}

func InstallProducer(name string, count int) feature.StepFn {
	return installProducer(name, count, "")
}

// InstallProducerWithEventType installs a producer whose events have a
// scenario-specific CloudEvent type. InstallProducer remains the compatibility
// entry point for callers that rely on the producer binary's default type.
func InstallProducerWithEventType(name string, count int, eventType string) feature.StepFn {
	return installProducer(name, count, eventType)
}

func installProducer(name string, count int, eventType string) feature.StepFn {
	return func(ctx context.Context, t feature.T) {
		registerImages(ctx, t)
		if _, err := manifest.InstallYamlFS(ctx, producerYamls, map[string]interface{}{
			"producerName":      name,
			"producerCount":     count,
			"producerEventType": eventType,
		}); err != nil {
			t.Fatal(err)
		}
	}
}

func registerImages(ctx context.Context, t feature.T) {
	opt := environment.RegisterPackage(manifest.ImagesFromFS(ctx, yamls)...)
	if _, err := opt(ctx, nil); err != nil {
		t.Fatal(err)
	}
}
