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
)

func TestConfigureContextDisablesHAAndScopesIngress(t *testing.T) {
	ctx := configureContext(context.Background(), "knative-eventing")
	if !sharedmain.IsHADisabled(ctx) {
		t.Error("ingress context must disable Lease-based leader election")
	}
	if got, want := injection.GetNamespaceScope(ctx), "knative-eventing"; got != want {
		t.Errorf("namespace scope = %q, want %q", got, want)
	}
}

func TestConfigureContextDoesNotScopeEmptyNamespace(t *testing.T) {
	ctx := configureContext(context.Background(), "")
	if !sharedmain.IsHADisabled(ctx) {
		t.Error("ingress context must disable Lease-based leader election")
	}
	if injection.HasNamespaceScope(ctx) {
		t.Errorf("empty namespace unexpectedly configured scope %q", injection.GetNamespaceScope(ctx))
	}
}
