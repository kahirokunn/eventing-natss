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
	"fmt"
	"time"

	"github.com/kelseyhightower/envconfig"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"

	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/system"

	eventingv1 "knative.dev/eventing/pkg/apis/eventing/v1"
	brokerinformer "knative.dev/eventing/pkg/client/injection/informers/eventing/v1/broker"
	triggerinformer "knative.dev/eventing/pkg/client/injection/informers/eventing/v1/trigger"
	eventinglisters "knative.dev/eventing/pkg/client/listers/eventing/v1"

	"knative.dev/eventing-natss/pkg/broker/constants"
	commonconfig "knative.dev/eventing-natss/pkg/common/config"
	commonnats "knative.dev/eventing-natss/pkg/common/nats"
)

type envConfig struct {
	PodName               string        `envconfig:"POD_NAME" required:"true"`
	ContainerName         string        `envconfig:"CONTAINER_NAME" required:"true"`
	BrokerName            string        `envconfig:"BROKER_NAME" required:"true"`
	BrokerNamespace       string        `envconfig:"BROKER_NAMESPACE" required:"true"`
	StreamName            string        `envconfig:"STREAM_NAME" required:"true"`
	NatsURL               string        `envconfig:"NATS_URL"`
	NatsConfig            string        `envconfig:"NATS_CONFIG"`
	OIDCServiceAccount    string        `envconfig:"OIDC_SERVICE_ACCOUNT"`
	OIDCServiceAccountUID string        `envconfig:"OIDC_SERVICE_ACCOUNT_UID"`
	FetchBatchSize        int           `envconfig:"CONSUMER_FETCH_BATCH_SIZE" default:"0"`
	FetchTimeout          time.Duration `envconfig:"CONSUMER_FETCH_TIMEOUT" default:"0"`
	MaxConcurrency        int           `envconfig:"CONSUMER_MAX_CONCURRENCY" default:"0"`
}

// NewController creates a new filter controller
func NewController(ctx context.Context, watcher configmap.Watcher) *controller.Impl {
	return newController(ctx, watcher, nil)
}

// NewController creates a filter controller whose data-plane resources are
// owned and shut down by this Runtime.
func (r *Runtime) NewController(ctx context.Context, watcher configmap.Watcher) *controller.Impl {
	return newController(ctx, watcher, r)
}

func newController(ctx context.Context, _ configmap.Watcher, runtime *Runtime) *controller.Impl {
	logger := logging.FromContext(ctx)

	env := &envConfig{}
	if err := envconfig.Process("", env); err != nil {
		logger.Fatalw("Failed to process environment variables", zap.Error(err))
	}

	natsConn, err := connectFilterNATS(ctx, env)
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
	var tokenSource audienceTokenSource
	if env.OIDCServiceAccount != "" {
		if env.OIDCServiceAccountUID == "" {
			logger.Fatal("OIDC_SERVICE_ACCOUNT_UID is required when OIDC_SERVICE_ACCOUNT is configured")
		}
		tokenSource = newTokenRequestAudienceSource(
			kubeclient.Get(ctx).CoreV1().ServiceAccounts(env.BrokerNamespace),
			env.OIDCServiceAccount,
			types.UID(env.OIDCServiceAccountUID),
		)
	}
	consumerConfig := &ConsumerManagerConfig{
		FetchBatchSize:      env.FetchBatchSize,
		FetchTimeout:        env.FetchTimeout,
		MaxConcurrency:      env.MaxConcurrency,
		StreamName:          env.StreamName,
		AudienceTokenSource: tokenSource,
	}
	consumerCtx := ctx
	if runtime != nil {
		consumerCtx = context.WithoutCancel(ctx)
	}
	consumerManager := NewConsumerManager(consumerCtx, natsConn, js, consumerConfig)

	// Create filter reconciler
	reconciler := NewFilterReconciler(
		ctx,
		triggerInformer.Lister(),
		brokerInformer.Lister(),
		consumerManager,
		env.BrokerNamespace,
		env.BrokerName,
	)
	consumerManager.setTokenReadinessRequirements(func() ([]tokenReadinessRequirement, bool) {
		if !triggerInformer.Informer().HasSynced() {
			return nil, false
		}
		triggers, err := triggerInformer.Lister().Triggers(env.BrokerNamespace).List(labels.Everything())
		if err != nil {
			return nil, false
		}
		requirements := make([]tokenReadinessRequirement, 0, len(triggers))
		for _, trigger := range triggers {
			if trigger.Spec.Broker != env.BrokerName {
				continue
			}
			subscriber, deadLetterSink, resolved := resolvedTriggerDestinations(trigger)
			authenticated := resolved && (nonEmptyAudience(subscriber.Audience) ||
				(deadLetterSink != nil && nonEmptyAudience(deadLetterSink.Audience)))
			requirements = append(requirements, tokenReadinessRequirement{
				triggerUID:    string(trigger.UID),
				generation:    trigger.Generation,
				resolved:      resolved,
				authenticated: authenticated,
			})
		}
		return requirements, true
	})
	if runtime != nil {
		runtime.Attach(consumerManager, natsConn)
	}

	// Create controller using the filter reconciler which implements
	// reconciler.Interface via its Reconcile(ctx, key) method.
	impl := controller.NewContext(ctx, reconciler, controller.ControllerOptions{
		WorkQueueName: "NatsJetStreamBrokerFilter",
		Logger:        logger,
	})
	reconciler.enqueue = impl.Enqueue

	// Set up event handlers for Trigger resources.
	// Events are enqueued into the work queue and reconciled via
	// FilterReconciler.Reconcile, giving us rate limiting, dedup,
	// per-key serialization, and backoff on errors.
	triggerInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: filterTriggersForBroker(brokerInformer.Lister(), env.BrokerNamespace, env.BrokerName),
		Handler:    controller.HandleAll(impl.Enqueue),
	})
	brokerInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: func(obj interface{}) bool {
			if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
				obj = tombstone.Obj
			}
			broker, ok := obj.(*eventingv1.Broker)
			return ok && broker.Namespace == env.BrokerNamespace && broker.Name == env.BrokerName
		},
		Handler: controller.HandleAll(func(interface{}) {
			triggers, err := triggerInformer.Lister().Triggers(env.BrokerNamespace).List(labels.Everything())
			if err != nil {
				return
			}
			for _, trigger := range triggers {
				if trigger.Spec.Broker == env.BrokerName {
					impl.Enqueue(trigger)
				}
			}
		}),
	})

	logger.Info("Filter controller initialized")
	return impl
}

func filterNATSConfig(env *envConfig) (commonconfig.EventingNatsConfig, error) {
	if env.NatsConfig != "" {
		return commonnats.ParseEventingNatsConfig(env.NatsConfig)
	}
	if env.NatsURL == "" {
		return commonconfig.EventingNatsConfig{}, fmt.Errorf("one of NATS_CONFIG or NATS_URL is required")
	}
	return commonconfig.EventingNatsConfig{URL: env.NatsURL}, nil
}

func connectFilterNATS(ctx context.Context, env *envConfig) (*nats.Conn, error) {
	// Preserve the public URL-only controller contract for manually managed
	// filters. This path intentionally needs no Kubernetes Secret client.
	if env.NatsConfig == "" {
		if env.NatsURL == "" {
			return nil, fmt.Errorf("one of NATS_CONFIG or NATS_URL is required")
		}
		return commonnats.NewNatsConnFromURL(ctx, env.NatsURL)
	}

	config, err := filterNATSConfig(env)
	if err != nil {
		return nil, err
	}
	// Resolve every credential reference in the system namespace. The filter's
	// informer scope is its Broker namespace and must not affect Secret lookup.
	return commonnats.NewNatsConnWithSecrets(ctx, config,
		kubeclient.Get(ctx).CoreV1().Secrets(system.Namespace()), filterNATSOptions(ctx, config)...)
}

func filterNATSOptions(ctx context.Context, config commonconfig.EventingNatsConfig) []nats.Option {
	logger := logging.FromContext(ctx)
	options := []nats.Option{
		nats.Name("natsjs broker filter"),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			logger.Warnw("Disconnected from NATS", zap.Error(err))
		}),
		nats.ReconnectHandler(func(conn *nats.Conn) {
			logger.Infow("Reconnected to NATS", zap.String("url", conn.ConnectedUrl()))
		}),
		nats.ClosedHandler(func(_ *nats.Conn) {
			logger.Error("NATS connection closed; liveness will restart the filter")
		}),
	}
	if config.ConnOpts != nil && config.ConnOpts.RetryOnFailedConnect {
		reconnectWait := time.Duration(config.ConnOpts.ReconnectWaitMilliseconds) * time.Millisecond
		maxReconnects := config.ConnOpts.MaxReconnects
		options = append(options, nats.CustomReconnectDelay(func(attempts int) time.Duration {
			logger.Warnw("Waiting to reconnect to NATS",
				zap.Int("attempt", attempts),
				zap.Int("max_reconnects", maxReconnects),
			)
			return reconnectWait
		}))
	}
	return options
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
