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
	"log"
	"os"

	"knative.dev/pkg/injection"
	"knative.dev/pkg/injection/sharedmain"
	"knative.dev/pkg/signals"

	"knative.dev/eventing-natss/pkg/broker/filter"
)

func main() {
	component := "natsjs-broker-filter"

	signalCtx := signals.NewContext()
	runtime := filter.NewRuntime(signalCtx)
	ctx := configureContext(signalCtx, runtime, brokerNamespace())

	go func() {
		<-signalCtx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), filter.DefaultShutdownTimeout)
		defer cancel()
		_ = runtime.Shutdown(shutdownCtx)
	}()

	sharedmain.MainWithContext(ctx, component, runtime.NewController)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), filter.DefaultShutdownTimeout)
	defer cancel()
	if err := runtime.Shutdown(shutdownCtx); err != nil {
		log.Printf("filter shutdown did not complete cleanly: %v", err)
	}
}

func brokerNamespace() string {
	ns := os.Getenv("BROKER_NAMESPACE")
	if ns == "" {
		ns = os.Getenv("NAMESPACE")
	}
	return ns
}

func configureContext(ctx context.Context, runtime *filter.Runtime, namespace string) context.Context {
	ctx = sharedmain.WithHADisabled(ctx)
	ctx = injection.AddReadiness(ctx, runtime.ReadinessHandler())
	ctx = injection.AddLiveness(ctx, runtime.LivenessHandler())
	if namespace != "" {
		ctx = injection.WithNamespaceScope(ctx, namespace)
	}
	return ctx
}
