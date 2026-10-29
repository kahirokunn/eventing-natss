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
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/nats-io/nats.go"
	corev1 "k8s.io/api/core/v1"
	eventingv1 "knative.dev/eventing/pkg/apis/eventing/v1"
	"knative.dev/pkg/controller"

	brokerconfig "knative.dev/eventing-natss/pkg/broker/config"
	"knative.dev/eventing-natss/pkg/broker/contract"
	brokerutils "knative.dev/eventing-natss/pkg/broker/utils"
)

type brokerStream struct {
	name  string
	bound bool // name comes from an existing binding rather than the requested annotation
	info  *nats.StreamInfo
	owned bool
}

func bindStreamName(b *eventingv1.Broker, name string) {
	if b.Status.Annotations == nil {
		b.Status.Annotations = make(map[string]string)
	}
	b.Status.Annotations[brokerutils.BrokerStreamNameAnnotation] = name
}

func ownerUID(info *nats.StreamInfo) string {
	return info.Config.Metadata[brokerutils.BrokerStreamOwnerUIDMetadata]
}

func hasBrokerSubject(info *nats.StreamInfo, publishSubject string) bool {
	return slices.Contains(info.Config.Subjects, publishSubject+".>")
}

// legacyStreamName returns the stream recorded for b in the ingress contract,
// or "" unless the entry matches b and its stream is not shared.
func legacyStreamName(c *contract.Contract, b *eventingv1.Broker) string {
	entry, ok := c.GetBroker(b.Namespace, b.Name)
	if !ok || entry.UID != string(b.UID) || entry.Namespace != b.Namespace || entry.Name != b.Name ||
		entry.PublishSubject != brokerutils.BrokerPublishSubjectName(b.Namespace, b.Name) ||
		brokerutils.ValidateStreamName(entry.StreamName) != nil {
		return ""
	}
	self := contract.BrokerKey(b.Namespace, b.Name)
	for key, other := range c.Brokers {
		if key != self && other.StreamName == entry.StreamName {
			return ""
		}
	}
	return entry.StreamName
}

// loadLegacyStreamName reads the contract from the API server rather than the
// informer cache, which may not yet reflect a write from the previous reconcile.
// Callers read it lazily: the steady state never needs it.
func (r *Reconciler) loadLegacyStreamName(ctx context.Context, b *eventingv1.Broker) (string, error) {
	c, err := r.contractManager.GetContractFromAPI(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to read stream binding: %w", err)
	}
	return legacyStreamName(c, b), nil
}

// findOwnedStream returns the name of the stream carrying the Broker's subject
// if its owner metadata matches uid. A stream can outlive a failed status or
// contract write, and its UID stays authoritative if the requested name changes.
func (r *Reconciler) findOwnedStream(publishSubject, uid string) (string, error) {
	found, err := r.js.StreamNameBySubject(publishSubject + ".events")
	if errors.Is(err, nats.ErrNoMatchingStream) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("failed to find broker stream: %w", err)
	}
	info, err := r.js.StreamInfo(found)
	if errors.Is(err, nats.ErrStreamNotFound) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("failed to get stream info: %w", err)
	}
	if ownerUID(info) != uid {
		return "", nil
	}
	return found, nil
}

// getBrokerStream resolves the original binding independently of the mutable
// annotation. Unmarked streams require a unique, UID-matching legacy contract.
func (r *Reconciler) getBrokerStream(ctx context.Context, b *eventingv1.Broker) (*brokerStream, error) {
	if b.UID == "" {
		return &brokerStream{name: brokerutils.BrokerStreamName(b)}, nil
	}
	uid := string(b.UID)
	publishSubject := brokerutils.BrokerPublishSubjectName(b.Namespace, b.Name)

	var (
		legacyName   string
		legacyLoaded bool
		err          error
	)
	name := b.Status.Annotations[brokerutils.BrokerStreamNameAnnotation]
	if name == "" {
		if legacyName, err = r.loadLegacyStreamName(ctx, b); err != nil {
			return nil, err
		}
		legacyLoaded = true
		if name = legacyName; name == "" {
			if name, err = r.findOwnedStream(publishSubject, uid); err != nil {
				return nil, err
			}
		}
	}
	stream := &brokerStream{name: name, bound: name != ""}
	if !stream.bound {
		stream.name = brokerutils.RequestedBrokerStreamName(b)
	}

	if brokerutils.ValidateStreamName(stream.name) != nil {
		// No stream could have been created with this name. In particular,
		// finalization must not retry an invalid JetStream API request forever.
		return stream, nil
	}
	info, err := r.js.StreamInfo(stream.name)
	if errors.Is(err, nats.ErrStreamNotFound) {
		return stream, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get stream info: %w", err)
	}
	stream.info = info
	owner := ownerUID(info)
	if owner == "" && !legacyLoaded {
		// A bound stream whose ownership stamp was lost is still adoptable.
		if legacyName, err = r.loadLegacyStreamName(ctx, b); err != nil {
			return nil, err
		}
	}
	stream.owned = owner == uid ||
		(owner == "" && stream.name == legacyName && hasBrokerSubject(info, publishSubject))
	stream.bound = stream.bound || stream.owned
	return stream, nil
}

func (r *Reconciler) reconcileStream(ctx context.Context, b *eventingv1.Broker, cfg *brokerconfig.NatsJetStreamBrokerConfig) (string, error) {
	fail := func(reason string, err error) (string, error) {
		b.Status.MarkIngressFailed(reason, "%v", err)
		return "", err
	}
	if b.UID == "" {
		return fail("StreamOwnershipFailed", fmt.Errorf("Broker UID is required for stream ownership"))
	}
	stream, err := r.getBrokerStream(ctx, b)
	if err != nil {
		return fail("StreamInfoFailed", err)
	}
	if stream.bound {
		bindStreamName(b, stream.name)
		if requested := brokerutils.RequestedBrokerStreamName(b); requested != stream.name {
			return fail("StreamNameImmutable", fmt.Errorf("stream name cannot be changed from %q to %q; restore the original annotation", stream.name, requested))
		}
	}
	if err := brokerutils.ValidateStreamName(stream.name); err != nil {
		return fail("InvalidStreamName", err)
	}

	uid := string(b.UID)
	publishSubject := brokerutils.BrokerPublishSubjectName(b.Namespace, b.Name)
	switch {
	case stream.info == nil:
		streamConfig := brokerconfig.BuildNatsStreamConfig(stream.name, publishSubject, cfg)
		streamConfig.Metadata = map[string]string{brokerutils.BrokerStreamOwnerUIDMetadata: uid}
		stream.info, err = r.js.AddStream(streamConfig)
		if err != nil {
			controller.GetEventRecorder(ctx).Event(b, corev1.EventTypeWarning, ReasonStreamFailed, err.Error())
			return fail("StreamCreationFailed", fmt.Errorf("failed to create stream: %w", err))
		}
		controller.GetEventRecorder(ctx).Event(b, corev1.EventTypeNormal, ReasonStreamCreated, "JetStream stream created")
	case !stream.owned:
		return fail("StreamOwnershipFailed", fmt.Errorf("stream %q is not owned by Broker %s/%s (%s)", stream.name, b.Namespace, b.Name, b.UID))
	case ownerUID(stream.info) == "":
		// Legacy stream adopted via the ingress contract: record its owner.
		streamConfig := stream.info.Config
		if streamConfig.Metadata == nil {
			streamConfig.Metadata = make(map[string]string)
		}
		streamConfig.Metadata[brokerutils.BrokerStreamOwnerUIDMetadata] = uid
		stream.info, err = r.js.UpdateStream(&streamConfig)
		if err != nil {
			return fail("StreamOwnershipFailed", fmt.Errorf("failed to record stream ownership: %w", err))
		}
	}
	// Also check create/update responses: a concurrent creator or an older
	// server must not make an unowned stream appear successfully reconciled.
	if ownerUID(stream.info) != uid {
		return fail("StreamOwnershipFailed", fmt.Errorf("stream %q did not retain Broker ownership metadata", stream.name))
	}
	if !hasBrokerSubject(stream.info, publishSubject) {
		return fail("StreamSubjectMismatch", fmt.Errorf("stream %q does not contain Broker subject %q", stream.name, publishSubject+".>"))
	}
	bindStreamName(b, stream.name)
	return stream.name, nil
}
