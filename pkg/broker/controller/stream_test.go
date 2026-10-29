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

package controller

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/nats-io/nats.go"
	"k8s.io/apimachinery/pkg/runtime"
	kubefake "k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
	eventingv1 "knative.dev/eventing/pkg/apis/eventing/v1"

	"knative.dev/eventing-natss/pkg/broker/contract"
	brokerutils "knative.dev/eventing-natss/pkg/broker/utils"
)

func customBroker(namespace, name, stream string) *eventingv1.Broker {
	b := testBroker(namespace, name)
	b.Annotations = map[string]string{brokerutils.BrokerStreamNameAnnotation: stream}
	b.Status.InitializeConditions()
	return b
}

func requireStream(t *testing.T, js nats.JetStreamContext, name string) *nats.StreamInfo {
	t.Helper()
	info, err := js.StreamInfo(name)
	if err != nil {
		t.Fatalf("StreamInfo(%q): %v", name, err)
	}
	return info
}

func requireStreamAbsent(t *testing.T, js nats.JetStreamContext, name string) {
	t.Helper()
	if _, err := js.StreamInfo(name); !errors.Is(err, nats.ErrStreamNotFound) {
		t.Fatalf("StreamInfo(%q) = %v, want stream not found", name, err)
	}
}

func requireIngressFailure(t *testing.T, b *eventingv1.Broker, reason string) {
	t.Helper()
	cond := b.Status.GetCondition(eventingv1.BrokerConditionIngress)
	if cond == nil || !cond.IsFalse() || cond.Reason != reason {
		t.Fatalf("ingress condition = %v, want False/%s", cond, reason)
	}
}

func streamContract(b *eventingv1.Broker, name string) contract.BrokerContract {
	return contract.BrokerContract{
		UID: string(b.UID), Namespace: b.Namespace, Name: b.Name, StreamName: name,
		PublishSubject: brokerutils.BrokerPublishSubjectName(b.Namespace, b.Name),
	}
}

func brokerContractEntry(t *testing.T, r *Reconciler, b *eventingv1.Broker) (contract.BrokerContract, bool) {
	t.Helper()
	c, err := r.contractManager.GetContractFromAPI(testContext())
	if err != nil {
		t.Fatal(err)
	}
	return c.GetBroker(b.Namespace, b.Name)
}

// failConfigMaps makes the given ConfigMap verb fail until the returned flag is cleared.
func failConfigMaps(kube *kubefake.Clientset, verb string) *bool {
	fail := true
	kube.PrependReactor(verb, "configmaps", func(clienttesting.Action) (bool, runtime.Object, error) {
		if fail {
			return true, nil, fmt.Errorf("temporary contract %s failure", verb)
		}
		return false, nil, nil
	})
	return &fail
}

func TestStreamNameCollisionProtectsOtherBroker(t *testing.T) {
	r, _ := setupBrokerReconciler(t)
	a := customBroker("team-a", "broker", "SHARED_STREAM")
	b := customBroker("team-b", "broker", "SHARED_STREAM")
	ctx := testContext()
	if err := r.ReconcileKind(ctx, a); err != nil {
		t.Fatal(err)
	}
	if _, err := r.js.Publish(brokerutils.BrokerPublishSubjectName(a.Namespace, a.Name)+".events", []byte("stored event")); err != nil {
		t.Fatal(err)
	}
	if err := r.ReconcileKind(ctx, b); err == nil {
		t.Fatal("expected a stream ownership conflict")
	}
	requireIngressFailure(t, b, "StreamOwnershipFailed")
	if err := r.FinalizeKind(ctx, b); err != nil {
		t.Fatal(err)
	}
	info := requireStream(t, r.js, "SHARED_STREAM")
	if info.State.Msgs != 1 || info.Config.Metadata[brokerutils.BrokerStreamOwnerUIDMetadata] != string(a.UID) {
		t.Fatalf("another Broker's stream changed: %+v", info)
	}
	if _, exists := brokerContractEntry(t, r, b); exists {
		t.Fatal("rejected Broker was published in the ingress contract")
	}
	if _, exists := brokerContractEntry(t, r, a); !exists {
		t.Fatal("owning Broker's ingress contract was removed")
	}
}

func TestStreamNameChangesPreserveOriginalBinding(t *testing.T) {
	for _, tc := range []struct{ name, original, requested string }{
		{"rename", "ORIGINAL_STREAM", "OTHER_STREAM"},
		{"remove annotation", "ORIGINAL_STREAM", ""},
		{"invalid replacement", "ORIGINAL_STREAM", "bad.stream.name"},
		{"add annotation", "", "OTHER_STREAM"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, _ := setupBrokerReconciler(t)
			b := customBroker(testNamespace, testBrokerName, tc.original)
			ctx := testContext()
			if err := r.ReconcileKind(ctx, b); err != nil {
				t.Fatal(err)
			}
			bound := brokerutils.BrokerStreamName(b)
			if _, err := r.js.Publish(brokerutils.BrokerPublishSubjectName(b.Namespace, b.Name)+".events", []byte("stored event")); err != nil {
				t.Fatal(err)
			}
			b.Annotations[brokerutils.BrokerStreamNameAnnotation] = tc.requested
			if err := r.ReconcileKind(ctx, b); err == nil {
				t.Fatal("expected immutable stream name error")
			}
			requireIngressFailure(t, b, "StreamNameImmutable")
			if brokerutils.BrokerStreamName(b) != bound || requireStream(t, r.js, bound).State.Msgs != 1 {
				t.Fatal("original binding or stored event was lost")
			}
			if entry, _ := brokerContractEntry(t, r, b); entry.StreamName != bound {
				t.Fatalf("ingress contract no longer references %q", bound)
			}
			if err := r.FinalizeKind(ctx, b); err != nil {
				t.Fatal(err)
			}
			requireStreamAbsent(t, r.js, bound)
			if err := r.FinalizeKind(ctx, b); err != nil {
				t.Fatalf("repeated finalization: %v", err)
			}
		})
	}
}

func TestInvalidInitialStreamNameCanBeFinalized(t *testing.T) {
	for _, name := range []string{"bad.stream.name", "bad stream", "bad*stream", "bad/stream", "bad\fstream"} {
		t.Run(name, func(t *testing.T) {
			r, _ := setupBrokerReconciler(t)
			b := customBroker(testNamespace, testBrokerName, name)
			if err := r.ReconcileKind(testContext(), b); err == nil {
				t.Fatal("expected invalid name error")
			}
			requireIngressFailure(t, b, "InvalidStreamName")
			if err := r.FinalizeKind(testContext(), b); err != nil {
				t.Fatalf("uncreated stream blocked Broker deletion: %v", err)
			}
		})
	}
}

func TestStreamBindingRecoveredAfterPartialFailure(t *testing.T) {
	for _, finalize := range []bool{false, true} {
		t.Run(map[bool]string{false: "reconcile", true: "finalize"}[finalize], func(t *testing.T) {
			r, kube := setupBrokerReconciler(t)
			failContract := failConfigMaps(kube, "create")
			b := customBroker(testNamespace, testBrokerName, "ORIGINAL_STREAM")
			if err := r.ReconcileKind(testContext(), b); err == nil {
				t.Fatal("expected contract write failure")
			}
			requireStream(t, r.js, "ORIGINAL_STREAM")
			// Simulate losing the status write as well as the contract write.
			b.Status.Annotations = nil
			b.Annotations[brokerutils.BrokerStreamNameAnnotation] = "bad.changed.name"
			*failContract = false
			if !finalize {
				if err := r.ReconcileKind(testContext(), b); err == nil {
					t.Fatal("expected immutable name error after recovery")
				}
				requireIngressFailure(t, b, "StreamNameImmutable")
				if brokerutils.BrokerStreamName(b) != "ORIGINAL_STREAM" {
					t.Fatal("failed to recover original stream name")
				}
				b.Annotations[brokerutils.BrokerStreamNameAnnotation] = "ORIGINAL_STREAM"
				if err := r.ReconcileKind(testContext(), b); err != nil {
					t.Fatalf("retry after restoring annotation: %v", err)
				}
			}
			if err := r.FinalizeKind(testContext(), b); err != nil {
				t.Fatal(err)
			}
			requireStreamAbsent(t, r.js, "ORIGINAL_STREAM")
		})
	}
}

func TestLegacyStreamOwnership(t *testing.T) {
	for _, tc := range []struct {
		name, stream string
		contract     bool
		wrongUID     bool
		wrongSubject bool
		shared       bool
		owner        string
		wantAdopt    bool
	}{
		{name: "generated name", contract: true, wantAdopt: true},
		{name: "custom name", stream: "LEGACY_STREAM", contract: true, wantAdopt: true},
		{name: "manual stream", stream: "LEGACY_STREAM"},
		{name: "previous Broker UID", stream: "LEGACY_STREAM", contract: true, wrongUID: true},
		{name: "ambiguous contract", stream: "LEGACY_STREAM", contract: true, shared: true},
		{name: "contract with wrong subject", stream: "LEGACY_STREAM", contract: true, wrongSubject: true},
		{name: "foreign ownership overrides contract", stream: "LEGACY_STREAM", contract: true, owner: "another-uid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, _ := setupBrokerReconciler(t)
			b := customBroker(testNamespace, testBrokerName, tc.stream)
			name := brokerutils.RequestedBrokerStreamName(b)
			ctx := testContext()
			metadata := map[string]string{"team": "original"}
			if tc.owner != "" {
				metadata[brokerutils.BrokerStreamOwnerUIDMetadata] = tc.owner
			}
			subjects := []string{brokerutils.BrokerPublishSubjectName(b.Namespace, b.Name) + ".>", "additional.events"}
			if tc.wrongSubject {
				subjects = []string{"another.broker.>"}
			}
			info, err := r.js.AddStream(&nats.StreamConfig{
				Name: name, Subjects: subjects,
				Storage: nats.MemoryStorage, MaxMsgs: 40, MaxAge: time.Hour, Metadata: metadata,
			})
			if err != nil {
				t.Fatal(err)
			}
			before := info.Config
			if tc.contract {
				entry := streamContract(b, name)
				if tc.wrongUID {
					entry.UID = "previous-uid"
				}
				if err := r.contractManager.UpdateBroker(ctx, entry); err != nil {
					t.Fatal(err)
				}
				if tc.shared {
					entry.Name = "another-broker"
					entry.UID = "another-uid"
					if err := r.contractManager.UpdateBroker(ctx, entry); err != nil {
						t.Fatal(err)
					}
				}
			}
			err = r.ReconcileKind(ctx, b)
			if tc.wantAdopt {
				if err != nil {
					t.Fatal(err)
				}
				before.Metadata[brokerutils.BrokerStreamOwnerUIDMetadata] = string(b.UID)
			} else {
				if err == nil {
					t.Fatal("unowned stream was adopted")
				}
				requireIngressFailure(t, b, "StreamOwnershipFailed")
			}
			if diff := cmp.Diff(before, requireStream(t, r.js, name).Config); diff != "" {
				t.Fatalf("stream settings changed (-want +got):\n%s", diff)
			}
			if err := r.FinalizeKind(ctx, b); err != nil {
				t.Fatal(err)
			}
			if tc.wantAdopt {
				requireStreamAbsent(t, r.js, name)
			} else {
				requireStream(t, r.js, name)
			}
		})
	}
}

func TestOwnedStreamRequiresBrokerSubject(t *testing.T) {
	r, _ := setupBrokerReconciler(t)
	b := customBroker(testNamespace, testBrokerName, "OWNED_STREAM")
	if _, err := r.js.AddStream(&nats.StreamConfig{
		Name: "OWNED_STREAM", Subjects: []string{"another.subject.>"},
		Metadata: map[string]string{brokerutils.BrokerStreamOwnerUIDMetadata: string(b.UID)},
	}); err != nil {
		t.Fatal(err)
	}
	if err := r.ReconcileKind(testContext(), b); err == nil {
		t.Fatal("stream without the Broker subject was accepted")
	}
	requireIngressFailure(t, b, "StreamSubjectMismatch")
}

type streamAPIFailure struct {
	nats.JetStreamContext
	deleteErr error
}

func (js *streamAPIFailure) DeleteStream(name string, opts ...nats.JSOpt) error {
	if js.deleteErr != nil {
		return js.deleteErr
	}
	return js.JetStreamContext.DeleteStream(name, opts...)
}

func TestLegacyStreamFinalizationRetriesWithoutLosingBinding(t *testing.T) {
	r, _ := setupBrokerReconciler(t)
	b := customBroker(testNamespace, testBrokerName, "LEGACY_STREAM")
	ctx := testContext()
	if _, err := r.js.AddStream(&nats.StreamConfig{
		Name: "LEGACY_STREAM", Subjects: []string{brokerutils.BrokerPublishSubjectName(b.Namespace, b.Name) + ".>"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := r.contractManager.UpdateBroker(ctx, streamContract(b, "LEGACY_STREAM")); err != nil {
		t.Fatal(err)
	}
	b.Annotations[brokerutils.BrokerStreamNameAnnotation] = "CHANGED_STREAM"
	js := &streamAPIFailure{JetStreamContext: r.js, deleteErr: errors.New("temporary connection failure")}
	r.js = js
	if err := r.FinalizeKind(ctx, b); err == nil {
		t.Fatal("expected deletion error")
	}
	if _, exists := brokerContractEntry(t, r, b); !exists {
		t.Fatal("contract needed for deletion retry was removed")
	}
	b.Status.Annotations = nil
	js.deleteErr = nil
	if err := r.FinalizeKind(ctx, b); err != nil {
		t.Fatal(err)
	}
	requireStreamAbsent(t, r.js, "LEGACY_STREAM")
}

func TestStreamFinalizationRetriesContractRemoval(t *testing.T) {
	r, kube := setupBrokerReconciler(t)
	b := customBroker(testNamespace, testBrokerName, "OWNED_STREAM")
	ctx := testContext()
	if err := r.ReconcileKind(ctx, b); err != nil {
		t.Fatal(err)
	}
	failContract := failConfigMaps(kube, "update")
	if err := r.FinalizeKind(ctx, b); err == nil {
		t.Fatal("expected contract removal failure")
	}
	requireStreamAbsent(t, r.js, "OWNED_STREAM")
	*failContract = false
	if err := r.FinalizeKind(ctx, b); err != nil {
		t.Fatalf("retry after stream deletion: %v", err)
	}
	if _, exists := brokerContractEntry(t, r, b); exists {
		t.Fatal("contract was not removed on retry")
	}
}
