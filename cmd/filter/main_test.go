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
	"testing"

	"knative.dev/pkg/injection"
	"knative.dev/pkg/injection/sharedmain"

	"knative.dev/eventing-natss/pkg/broker/filter"
)

func TestConfigureContextDisablesLeaderElection(t *testing.T) {
	runtime := filter.NewRuntime(context.Background())
	ctx := configureContext(context.Background(), runtime, "test-namespace")
	if !sharedmain.IsHADisabled(ctx) {
		t.Fatal("filter context must disable leader election")
	}
	if got := injection.GetNamespaceScope(ctx); got != "test-namespace" {
		t.Errorf("namespace scope = %q, want %q", got, "test-namespace")
	}
}

func TestConfigureContextDoesNotAddEmptyNamespaceScope(t *testing.T) {
	runtime := filter.NewRuntime(context.Background())
	ctx := configureContext(context.Background(), runtime, "")

	if !sharedmain.IsHADisabled(ctx) {
		t.Error("configureContext did not disable high availability/Lease election")
	}
	if injection.HasNamespaceScope(ctx) {
		t.Errorf("empty namespace unexpectedly configured scope %q", injection.GetNamespaceScope(ctx))
	}
}

func TestBrokerNamespacePrecedence(t *testing.T) {
	t.Setenv("NAMESPACE", "fallback-namespace")
	t.Setenv("BROKER_NAMESPACE", "broker-namespace")
	if got := brokerNamespace(); got != "broker-namespace" {
		t.Errorf("brokerNamespace() = %q, want BROKER_NAMESPACE value", got)
	}
	t.Setenv("BROKER_NAMESPACE", "")
	if got := brokerNamespace(); got != "fallback-namespace" {
		t.Errorf("brokerNamespace() fallback = %q, want NAMESPACE value", got)
	}
}
