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
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"go.uber.org/zap"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/tools/cache"

	duckv1 "knative.dev/pkg/apis/duck/v1"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/reconciler"

	eventingv1 "knative.dev/eventing/pkg/apis/eventing/v1"
	eventinglisters "knative.dev/eventing/pkg/client/listers/eventing/v1"
	"knative.dev/eventing/pkg/kncloudevents"

	"knative.dev/eventing-natss/pkg/broker/constants"
	brokeroidc "knative.dev/eventing-natss/pkg/broker/oidc"
	brokerutils "knative.dev/eventing-natss/pkg/broker/utils"
)

// FilterReconciler reconciles triggers and manages consumer subscriptions
type FilterReconciler struct {
	reconciler.LeaderAwareFuncs

	logger *zap.SugaredLogger

	triggerLister eventinglisters.TriggerLister
	brokerLister  eventinglisters.BrokerLister

	consumerManager *ConsumerManager
	brokerNamespace string
	brokerName      string
	enqueue         func(interface{})

	// triggerUIDs maps "namespace/name" keys to trigger UIDs so that
	// delete events (where the object is gone from the lister) can
	// still look up the UID to clean up the consumer subscription.
	triggerUIDs map[string]string
	mu          sync.RWMutex
}

// NewFilterReconciler creates a new filter reconciler
func NewFilterReconciler(
	ctx context.Context,
	triggerLister eventinglisters.TriggerLister,
	brokerLister eventinglisters.BrokerLister,
	consumerManager *ConsumerManager,
	brokerNamespace string,
	brokerName string,
) *FilterReconciler {
	return &FilterReconciler{
		logger:          logging.FromContext(ctx),
		triggerLister:   triggerLister,
		brokerLister:    brokerLister,
		consumerManager: consumerManager,
		brokerNamespace: brokerNamespace,
		brokerName:      brokerName,
		triggerUIDs:     make(map[string]string),
	}
}

// Reconcile implements reconciler.Interface. It is called by the controller
// work queue and handles both create/update and delete events.
func (r *FilterReconciler) Reconcile(ctx context.Context, key string) error {
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return fmt.Errorf("invalid resource key: %w", err)
	}

	trigger, err := r.triggerLister.Triggers(namespace).Get(name)
	if err != nil {
		if apierrs.IsNotFound(err) {
			// Trigger has been deleted — clean up subscription.
			cleanupErr := r.deleteTrackedTrigger(key)
			validationErr := r.refreshActiveOIDCAudienceState()
			return errors.Join(cleanupErr, validationErr)
		}
		return fmt.Errorf("failed to get trigger: %w", err)
	}
	if !r.ownsTrigger(trigger) {
		// A FilteringResourceEventHandler turns an update that changes
		// spec.broker into a delete for the old Broker. By the time this key is
		// reconciled, the shared lister already contains the new object, so
		// ownership loss must be handled like deletion rather than skipped.
		cleanupErr := r.deleteTrackedTrigger(key)
		validationErr := r.refreshActiveOIDCAudienceState()
		return errors.Join(cleanupErr, validationErr)
	}

	// A same-name Trigger can also be deleted and recreated before its queued
	// key is reconciled. Retire the previous UID before tracking the replacement
	// so its old process-local pull subscription cannot survive the recreation.
	uid := string(trigger.UID)
	r.mu.RLock()
	trackedUID, tracked := r.triggerUIDs[key]
	r.mu.RUnlock()
	if tracked && trackedUID != uid {
		if err := r.deleteTrackedTrigger(key); err != nil {
			return err
		}
	}

	// Track key→UID mapping for delete and ownership-change handling.
	r.mu.Lock()
	r.triggerUIDs[key] = uid
	r.mu.Unlock()

	return r.ReconcileTrigger(ctx, trigger)
}

// deleteTrackedTrigger unsubscribes the UID most recently observed for key.
// The compare-before-delete preserves a newer mapping if this helper is ever
// called concurrently outside the controller work queue's per-key ordering.
func (r *FilterReconciler) deleteTrackedTrigger(key string) error {
	r.mu.RLock()
	uid, ok := r.triggerUIDs[key]
	r.mu.RUnlock()
	if !ok {
		return nil
	}
	if err := r.DeleteTrigger(uid); err != nil {
		return err
	}
	r.mu.Lock()
	if r.triggerUIDs[key] == uid {
		delete(r.triggerUIDs, key)
	}
	r.mu.Unlock()
	return nil
}

// ReconcileTrigger reconciles a trigger to ensure the filter has a subscription
func (r *FilterReconciler) ReconcileTrigger(ctx context.Context, trigger *eventingv1.Trigger) error {
	logger := r.logger.With(
		zap.String("trigger", trigger.Name),
		zap.String("namespace", trigger.Namespace),
	)
	if !r.ownsTrigger(trigger) {
		logger.Debugw("trigger belongs to another broker, skipping")
		return nil
	}
	if err := r.refreshActiveOIDCAudienceState(); err != nil {
		return err
	}

	// Get the broker
	broker, err := r.brokerLister.Brokers(trigger.Namespace).Get(trigger.Spec.Broker)
	if err != nil {
		if apierrs.IsNotFound(err) {
			logger.Debugw("broker not found, skipping trigger")
			return nil
		}
		return fmt.Errorf("failed to get broker: %w", err)
	}

	// Check broker class
	if broker.GetAnnotations()[eventingv1.BrokerClassAnnotationKey] != constants.BrokerClassName {
		logger.Debugw("broker is not NatsJetStreamBroker, skipping")
		return nil
	}

	// Status is the credential authority consumed by this process. Never keep a
	// subscription while it belongs to an older generation or is unresolved.
	subscriber, deadLetterSink, destinationsResolved := resolvedTriggerDestinations(trigger)
	if !broker.IsReady() {
		if destinationsResolved && r.consumerManager != nil && r.consumerManager.hasDestinationTokenFailure(string(trigger.UID)) {
			if err := r.consumerManager.validateDestinationTokens(ctx, string(trigger.UID), subscriber, deadLetterSink); err != nil {
				return err
			}
			r.consumerManager.markDestinationTokensValidated(string(trigger.UID), trigger.Generation)
			return nil
		}
		logger.Debugw("broker is not ready, stopping trigger subscription")
		if err := r.stopSubscriptionPreservingTokenFailure(string(trigger.UID)); err != nil {
			return err
		}
		if r.consumerManager == nil {
			return nil
		}
		if !destinationsResolved {
			r.consumerManager.clearDestinationTokenFailures(string(trigger.UID))
			return nil
		}
		// A token issuance failure makes this Pod unready, which in turn makes
		// the Broker unready. Keep checking the already current, resolved status
		// while stopped so credential recovery can break that dependency cycle.
		if err := r.consumerManager.validateDestinationTokens(ctx, string(trigger.UID), subscriber, deadLetterSink); err != nil {
			return err
		}
		r.consumerManager.markDestinationTokensValidated(string(trigger.UID), trigger.Generation)
		return nil
	}
	if !destinationsResolved || !trigger.Status.IsReady() {
		logger.Debugw("trigger subscriber is not currently resolved, stopping subscription")
		if r.consumerManager != nil {
			r.consumerManager.clearDestinationTokenValidation(string(trigger.UID))
		}
		return r.stopSubscription(string(trigger.UID))
	}

	// Get broker ingress URL for reply events
	var brokerIngressURL *duckv1.Addressable
	if broker.Status.Address != nil && broker.Status.Address.URL != nil {
		brokerIngressURL = &duckv1.Addressable{URL: broker.Status.Address.URL.DeepCopy()}
	}

	if (nonEmptyAudience(subscriber.Audience) || (deadLetterSink != nil && nonEmptyAudience(deadLetterSink.Audience))) &&
		(trigger.Status.Auth == nil || trigger.Status.Auth.ServiceAccountName == nil || *trigger.Status.Auth.ServiceAccountName != brokeroidc.DeliveryServiceAccountName) {
		logger.Debugw("trigger OIDC identity is not current, stopping subscription")
		return r.stopSubscription(string(trigger.UID))
	}
	if err := r.consumerManager.validateDestinationTokens(ctx, string(trigger.UID), subscriber, deadLetterSink); err != nil {
		_ = r.stopSubscriptionPreservingTokenFailure(string(trigger.UID))
		return err
	}
	r.consumerManager.markDestinationTokensValidated(string(trigger.UID), trigger.Generation)

	// Build retry config from the effective delivery spec (trigger overrides broker).
	var retryConfig *kncloudevents.RetryConfig
	if delivery := brokerutils.EffectiveDelivery(trigger, broker); delivery != nil {
		config, err := kncloudevents.RetryConfigFromDeliverySpec(*delivery)
		if err != nil {
			logger.Warnw("failed to build retry config from delivery spec", zap.Error(err))
		} else {
			retryConfig = &config
		}
	}

	// Build no-retry config (JetStream handles retries via redelivery)
	var requestTimeout time.Duration
	if retryConfig != nil {
		requestTimeout = retryConfig.RequestTimeout
	}
	var noRetryConfig = kncloudevents.RetryConfig{
		RetryMax: 0,
		CheckRetry: func(ctx context.Context, resp *http.Response, err error) (bool, error) {
			return false, nil
		},
		Backoff: func(attemptNum int, resp *http.Response) time.Duration {
			return 0
		},
		RequestTimeout: requestTimeout,
	}

	// Subscribe to the trigger's consumer
	err = r.consumerManager.SubscribeTrigger(
		trigger,
		broker,
		subscriber,
		brokerIngressURL,
		deadLetterSink,
		retryConfig,
		&noRetryConfig,
	)
	if err != nil {
		return fmt.Errorf("failed to subscribe to trigger: %w", err)
	}

	return nil
}

func (r *FilterReconciler) validateActiveOIDCAudiences() error {
	triggers, err := r.triggerLister.Triggers(r.brokerNamespace).List(labels.Everything())
	if err != nil {
		return fmt.Errorf("list Triggers for OIDC validation: %w", err)
	}
	owned := make([]*eventingv1.Trigger, 0, len(triggers))
	for _, trigger := range triggers {
		if trigger.Spec.Broker == r.brokerName {
			owned = append(owned, trigger)
		}
	}
	if _, err := brokeroidc.AudiencesFromTriggers(owned); err != nil {
		return fmt.Errorf("invalid active OIDC audience set: %w", err)
	}
	return nil
}

func (r *FilterReconciler) refreshActiveOIDCAudienceState() error {
	err := r.validateActiveOIDCAudiences()
	if r.consumerManager != nil {
		becameValid := r.consumerManager.setAudienceConfigurationValid(err == nil)
		if becameValid && r.enqueue != nil {
			triggers, listErr := r.triggerLister.Triggers(r.brokerNamespace).List(labels.Everything())
			if listErr != nil {
				return errors.Join(err, fmt.Errorf("list Triggers after OIDC audience recovery: %w", listErr))
			}
			for _, trigger := range triggers {
				if r.ownsTrigger(trigger) {
					r.enqueue(trigger)
				}
			}
		}
	}
	return err
}

func nonEmptyAudience(audience *string) bool {
	return audience != nil && *audience != ""
}

func resolvedTriggerDestinations(trigger *eventingv1.Trigger) (duckv1.Addressable, *duckv1.Addressable, bool) {
	subscriberResolved := trigger.Status.GetCondition(eventingv1.TriggerConditionSubscriberResolved)
	deadLetterResolved := trigger.Status.GetCondition(eventingv1.TriggerConditionDeadLetterSinkResolved)
	if trigger.Status.ObservedGeneration != trigger.Generation ||
		subscriberResolved == nil || !subscriberResolved.IsTrue() ||
		deadLetterResolved == nil || !deadLetterResolved.IsTrue() ||
		trigger.Status.SubscriberURI == nil {
		return duckv1.Addressable{}, nil, false
	}
	subscriber := duckv1.Addressable{
		URL:      trigger.Status.SubscriberURI.DeepCopy(),
		CACerts:  trigger.Status.SubscriberCACerts,
		Audience: trigger.Status.SubscriberAudience,
	}
	var deadLetterSink *duckv1.Addressable
	if trigger.Status.DeadLetterSinkURI != nil {
		deadLetterSink = &duckv1.Addressable{
			URL:      trigger.Status.DeadLetterSinkURI.DeepCopy(),
			CACerts:  trigger.Status.DeadLetterSinkCACerts,
			Audience: trigger.Status.DeadLetterSinkAudience,
		}
	}
	return subscriber, deadLetterSink, true
}

func (r *FilterReconciler) stopSubscription(triggerUID string) error {
	if r.consumerManager == nil {
		return nil
	}
	return r.consumerManager.UnsubscribeTrigger(triggerUID)
}

func (r *FilterReconciler) stopSubscriptionPreservingTokenFailure(triggerUID string) error {
	if r.consumerManager == nil {
		return nil
	}
	return r.consumerManager.unsubscribeTrigger(triggerUID, false)
}

func (r *FilterReconciler) ownsTrigger(trigger *eventingv1.Trigger) bool {
	return trigger != nil &&
		trigger.Namespace == r.brokerNamespace &&
		trigger.Spec.Broker == r.brokerName
}

// DeleteTrigger removes the subscription for a deleted trigger
func (r *FilterReconciler) DeleteTrigger(triggerUID string) error {
	return r.consumerManager.UnsubscribeTrigger(triggerUID)
}
