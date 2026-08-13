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
	"time"

	"github.com/kelseyhightower/envconfig"
	"go.uber.org/zap"
	"k8s.io/client-go/tools/cache"

	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"

	eventingv1 "knative.dev/eventing/pkg/apis/eventing/v1"
	brokerinformer "knative.dev/eventing/pkg/client/injection/informers/eventing/v1/broker"
	triggerinformer "knative.dev/eventing/pkg/client/injection/informers/eventing/v1/trigger"
	eventinglisters "knative.dev/eventing/pkg/client/listers/eventing/v1"

	"knative.dev/eventing-natss/pkg/broker/constants"
	commonnats "knative.dev/eventing-natss/pkg/common/nats"
)

type envConfig struct {
	PodName         string        `envconfig:"POD_NAME" required:"true"`
	ContainerName   string        `envconfig:"CONTAINER_NAME" required:"true"`
	BrokerName      string        `envconfig:"BROKER_NAME" required:"true"`
	BrokerNamespace string        `envconfig:"BROKER_NAMESPACE" required:"true"`
	StreamName      string        `envconfig:"STREAM_NAME" required:"true"`
	NatsURL         string        `envconfig:"NATS_URL" required:"true"`
	FetchBatchSize  int           `envconfig:"CONSUMER_FETCH_BATCH_SIZE" default:"0"`
	FetchTimeout    time.Duration `envconfig:"CONSUMER_FETCH_TIMEOUT" default:"0"`
	MaxConcurrency  int           `envconfig:"CONSUMER_MAX_CONCURRENCY" default:"0"`
}

// NewController creates a new filter controller
func NewController(ctx context.Context, _ configmap.Watcher) *controller.Impl {
	logger := logging.FromContext(ctx)

	env := &envConfig{}
	if err := envconfig.Process("", env); err != nil {
		logger.Fatalw("Failed to process environment variables", zap.Error(err))
	}

	// Create NATS connection using URL from environment variable
	natsConn, err := commonnats.NewNatsConnFromURL(ctx, env.NatsURL)
	if err != nil {
		logger.Fatalw("Failed to create NATS connection", zap.Error(err))
	}

	// Create JetStream context
	js, err := natsConn.JetStream()
	if err != nil {
		logger.Fatalw("Failed to create JetStream context", zap.Error(err))
	}

	// Get informers
	triggerInformer := triggerinformer.Get(ctx)
	brokerInformer := brokerinformer.Get(ctx)

	// Create consumer manager with optional configuration from environment
	consumerConfig := &ConsumerManagerConfig{
		FetchBatchSize: env.FetchBatchSize,
		FetchTimeout:   env.FetchTimeout,
		MaxConcurrency: env.MaxConcurrency,
		StreamName:     env.StreamName,
	}
	consumerManager := NewConsumerManager(ctx, natsConn, js, consumerConfig)

	// Create filter reconciler
	reconciler := NewFilterReconciler(
		ctx,
		triggerInformer.Lister(),
		brokerInformer.Lister(),
		consumerManager,
		env.BrokerNamespace,
		env.BrokerName,
	)

	// Create controller using the filter reconciler which implements
	// reconciler.Interface via its Reconcile(ctx, key) method.
	impl := controller.NewContext(ctx, reconciler, controller.ControllerOptions{
		WorkQueueName: "NatsJetStreamBrokerFilter",
		Logger:        logger,
	})

	// Set up event handlers for Trigger resources.
	// Events are enqueued into the work queue and reconciled via
	// FilterReconciler.Reconcile, giving us rate limiting, dedup,
	// per-key serialization, and backoff on errors.
	triggerInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: filterTriggersForBroker(brokerInformer.Lister(), env.BrokerNamespace, env.BrokerName),
		Handler:    controller.HandleAll(impl.Enqueue),
	})

	logger.Info("Filter controller initialized")
	return impl
}

// filterTriggersForBroker returns a filter function that only passes Triggers
// belonging to this filter process's Broker.
func filterTriggersForBroker(brokerLister eventinglisters.BrokerLister, brokerNamespace, brokerName string) func(obj interface{}) bool {
	return func(obj interface{}) bool {
		if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
			obj = tombstone.Obj
		}
		trigger, ok := obj.(*eventingv1.Trigger)
		if !ok || trigger.Namespace != brokerNamespace || trigger.Spec.Broker != brokerName {
			return false
		}

		// Get the broker referenced by this trigger
		broker, err := brokerLister.Brokers(brokerNamespace).Get(brokerName)
		if err != nil {
			// If we can't get the broker, include the trigger anyway
			// and let the reconciler handle the error
			return true
		}

		// Check if the broker is of class NatsJetStreamBroker
		return broker.GetAnnotations()[eventingv1.BrokerClassAnnotationKey] == constants.BrokerClassName
	}
}
