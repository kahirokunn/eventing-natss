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

package autoscaler

import (
	"crypto/sha256"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	eventingv1 "knative.dev/eventing/pkg/apis/eventing/v1"

	brokerutils "knative.dev/eventing-natss/pkg/broker/utils"
)

var ScaledObjectGVR = schema.GroupVersionResource{
	Group: "keda.sh", Version: "v1alpha1", Resource: "scaledobjects",
}

const (
	fallbackFailureThreshold int64 = 3
)

// FallbackReplicaCount is shared by KEDA's native fallback and the Broker
// controller's safety paths so neither can violate the configured minimum.
func FallbackReplicaCount(minScale int64) int64 {
	if minScale > 1 && minScale <= math.MaxInt32 {
		return minScale
	}
	return 1
}

// ScaledObjectName keeps the usual filter name for readable kubectl output,
// but adds a stable suffix when the Kubernetes resource-name limit would be
// exceeded.
func ScaledObjectName(targetName string) string {
	return stableName(targetName, 63)
}

// hpaName explicitly names KEDA's generated HPA. This preserves the existing
// ScaledObject name while keeping the HPA within its own 63-character limit.
func hpaName(targetName string) string {
	return stableName("keda-hpa-"+targetName, 63)
}

func stableName(name string, maxLength int) string {
	const (
		hashLength = 8
	)
	if len(name) <= maxLength {
		return name
	}
	hash := sha256.Sum256([]byte(name))
	prefixLength := maxLength - 1 - hashLength
	prefix := strings.TrimRight(name[:prefixLength], "-.")
	return prefix + "-" + fmt.Sprintf("%x", hash[:hashLength/2])
}

// MakeScaledObject builds the one ScaledObject owned by a Broker. Each
// Eventing Trigger contributes one NATS JetStream scaler trigger, while all of
// them target the same per-Broker filter Deployment.
func MakeScaledObject(
	broker *eventingv1.Broker,
	triggers []*eventingv1.Trigger,
	targetName string,
	settings Settings,
	monitoring MonitoringConfig,
) (*unstructured.Unstructured, error) {
	sortedTriggers := append([]*eventingv1.Trigger(nil), triggers...)
	sort.Slice(sortedTriggers, func(i, j int) bool {
		if sortedTriggers[i].Namespace == sortedTriggers[j].Namespace {
			return sortedTriggers[i].Name < sortedTriggers[j].Name
		}
		return sortedTriggers[i].Namespace < sortedTriggers[j].Namespace
	})

	scaleTriggers := make([]interface{}, 0, len(sortedTriggers))
	streamName := brokerutils.BrokerStreamName(broker)
	for _, trigger := range sortedTriggers {
		if trigger.Namespace != broker.Namespace || trigger.Spec.Broker != broker.Name {
			continue
		}
		lag, activationLag, err := ResolveLagThresholds(trigger.Annotations, broker.Annotations)
		if err != nil {
			return nil, fmt.Errorf("invalid autoscaling annotations on Trigger %s/%s: %w", trigger.Namespace, trigger.Name, err)
		}
		scaleTriggers = append(scaleTriggers, map[string]interface{}{
			"type": "nats-jetstream",
			"metadata": map[string]interface{}{
				"natsServerMonitoringEndpoint": monitoring.Endpoint,
				"account":                      monitoring.Account,
				"stream":                       streamName,
				"consumer":                     brokerutils.TriggerConsumerName(string(trigger.UID)),
				"lagThreshold":                 strconv.FormatInt(lag, 10),
				"activationLagThreshold":       strconv.FormatInt(activationLag, 10),
				"useHttps":                     strconv.FormatBool(monitoring.UseHTTPS),
			},
		})
	}

	object := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "keda.sh/v1alpha1",
		"kind":       "ScaledObject",
		"metadata": map[string]interface{}{
			"name":      ScaledObjectName(targetName),
			"namespace": broker.Namespace,
			"labels": map[string]interface{}{
				"eventing.knative.dev/broker": broker.Name,
			},
		},
		"spec": map[string]interface{}{
			"pollingInterval": settings.PollingInterval,
			"cooldownPeriod":  settings.CooldownPeriod,
			"minReplicaCount": settings.MinScale,
			"maxReplicaCount": settings.MaxScale,
			"fallback": map[string]interface{}{
				"failureThreshold": fallbackFailureThreshold,
				"replicas":         FallbackReplicaCount(settings.MinScale),
				"behavior":         "static",
			},
			"advanced": map[string]interface{}{
				"horizontalPodAutoscalerConfig": map[string]interface{}{
					"name": hpaName(targetName),
				},
			},
			"scaleTargetRef": map[string]interface{}{
				"apiVersion": "apps/v1",
				"kind":       "Deployment",
				"name":       targetName,
			},
			"triggers": scaleTriggers,
		},
	}}
	object.SetOwnerReferences([]metav1.OwnerReference{*metav1.NewControllerRef(
		broker,
		eventingv1.SchemeGroupVersion.WithKind("Broker"),
	)})
	return object, nil
}
