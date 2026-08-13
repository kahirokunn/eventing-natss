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

package main

import (
	"context"
	"os"

	"knative.dev/pkg/configmap"
	"knative.dev/pkg/injection"
	"knative.dev/pkg/injection/sharedmain"
	"knative.dev/pkg/signals"

	"knative.dev/eventing-natss/pkg/broker/ingress"
	"knative.dev/eventing-natss/pkg/common/configloader/fsloader"
)

func main() {
	component := "natsjs-broker-ingress"

	ctx := signals.NewContext()
	ctx = configureContext(ctx, os.Getenv("NAMESPACE"))
	ctx = fsloader.WithLoader(ctx, configmap.Load)

	sharedmain.MainWithContext(ctx, component, ingress.NewController)
}

func configureContext(ctx context.Context, namespace string) context.Context {
	// Every replica actively serves ingress traffic; leader election would add
	// Lease authority and contention without gating any data-plane work.
	ctx = sharedmain.WithHADisabled(ctx)
	ctx = sharedmain.WithHealthProbesDisabled(ctx)
	if namespace != "" {
		ctx = injection.WithNamespaceScope(ctx, namespace)
	}
	return ctx
}
