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

package oidc

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"

	corev1 "k8s.io/api/core/v1"
	"knative.dev/pkg/apis"

	eventingv1 "knative.dev/eventing/pkg/apis/eventing/v1"
)

const (
	MaxAudiences                  = 32
	MaxAudienceBytes              = 4096
	TokenExpirationSeconds        = int64(3600)
	DeliveryServiceAccountName    = "natsjs-broker-oidc"
	TokenCreatorClusterRoleName   = "natsjetstream-broker-oidc-token-creator"
	ManagedServiceAccountLabelKey = "app.kubernetes.io/managed-by"
	ManagedServiceAccountLabel    = "natsjetstream-broker-controller"
)

// NormalizeAudiences validates, de-duplicates, and sorts an audience set.
// Empty audiences mean that authentication is not requested and are ignored.
func NormalizeAudiences(values []string) ([]string, error) {
	set := make(map[string]struct{}, len(values))
	for _, audience := range values {
		if audience == "" {
			continue
		}
		if len(audience) > MaxAudienceBytes {
			return nil, fmt.Errorf("OIDC audience exceeds %d bytes", MaxAudienceBytes)
		}
		set[audience] = struct{}{}
		if len(set) > MaxAudiences {
			return nil, fmt.Errorf("OIDC audience count exceeds %d", MaxAudiences)
		}
	}
	result := make([]string, 0, len(set))
	for audience := range set {
		result = append(result, audience)
	}
	sort.Strings(result)
	return result, nil
}

// AudiencesFromTriggers returns only audiences resolved for the current
// Trigger generation. Stale status must never authorize a newly configured
// destination.
func AudiencesFromTriggers(triggers []*eventingv1.Trigger) ([]string, error) {
	values := make([]string, 0, len(triggers)*2)
	for _, trigger := range triggers {
		if trigger == nil || trigger.Status.ObservedGeneration != trigger.Generation {
			continue
		}
		if conditionTrue(trigger.Status.GetCondition(eventingv1.TriggerConditionSubscriberResolved)) && trigger.Status.SubscriberAudience != nil {
			values = append(values, *trigger.Status.SubscriberAudience)
		}
		if conditionTrue(trigger.Status.GetCondition(eventingv1.TriggerConditionDeadLetterSinkResolved)) && trigger.Status.DeadLetterSinkAudience != nil {
			values = append(values, *trigger.Status.DeadLetterSinkAudience)
		}
	}
	return NormalizeAudiences(values)
}

func conditionTrue(condition *apis.Condition) bool {
	return condition != nil && condition.IsTrue()
}

// AudienceKey returns an opaque stable identifier suitable for errors and
// metrics. Raw audience values may contain sensitive tenant information and
// must not be logged.
func AudienceKey(audience string) string {
	digest := sha256.Sum256([]byte(audience))
	return "sha256-" + hex.EncodeToString(digest[:])
}

// IsManagedDeliveryServiceAccount verifies the non-adopting shape shared by
// Broker and Trigger reconciliation. The Broker controller additionally
// compares the owner UID with a live Namespace before updating the object.
func IsManagedDeliveryServiceAccount(serviceAccount *corev1.ServiceAccount) bool {
	if serviceAccount == nil || serviceAccount.Name != DeliveryServiceAccountName ||
		serviceAccount.Labels[ManagedServiceAccountLabelKey] != ManagedServiceAccountLabel ||
		serviceAccount.AutomountServiceAccountToken == nil || *serviceAccount.AutomountServiceAccountToken ||
		len(serviceAccount.OwnerReferences) != 1 {
		return false
	}
	owner := serviceAccount.OwnerReferences[0]
	return owner.APIVersion == "v1" && owner.Kind == "Namespace" && owner.Name == serviceAccount.Namespace &&
		owner.UID != "" && owner.Controller != nil && *owner.Controller &&
		owner.BlockOwnerDeletion != nil && !*owner.BlockOwnerDeletion
}
