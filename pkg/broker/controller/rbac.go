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
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/retry"

	kncontroller "knative.dev/pkg/controller"
	"knative.dev/pkg/kmeta"
	"knative.dev/pkg/logging"
	pkgreconciler "knative.dev/pkg/reconciler"
	"knative.dev/pkg/system"

	eventingv1 "knative.dev/eventing/pkg/apis/eventing/v1"

	commonnats "knative.dev/eventing-natss/pkg/common/nats"
)

const (
	eventingConfigReaderRoleName = "eventing-config-reader"
	natsSecretRoleName           = "natsjetstream-broker-nats-secrets"
	managedByLabelKey            = "app.kubernetes.io/managed-by"
	managedByLabelValue          = "natsjetstream-broker-controller"
	brokerNamespaceAnnotation    = "nats.eventing.knative.dev/broker-namespace"
	brokerNameAnnotation         = "nats.eventing.knative.dev/broker-name"
	brokerUIDAnnotation          = "nats.eventing.knative.dev/broker-uid"
	filterServiceAccountBase     = "natsjs-filter"
)

func (r *Reconciler) dataplaneIdentity(b *eventingv1.Broker) string {
	return kmeta.ChildName(fmt.Sprintf("%s-%s-%s-%s", filterServiceAccountBase, b.Namespace, b.Name, b.UID), "")
}

func systemRBACName(identity, suffix string) string {
	return kmeta.ChildName(identity, suffix)
}

func brokerOwnerReference(b *eventingv1.Broker) []metav1.OwnerReference {
	return []metav1.OwnerReference{*kmeta.NewControllerRef(b)}
}

func managedMetadata(b *eventingv1.Broker) (map[string]string, map[string]string) {
	return map[string]string{managedByLabelKey: managedByLabelValue}, map[string]string{
		brokerNamespaceAnnotation: b.Namespace,
		brokerNameAnnotation:      b.Name,
		brokerUIDAnnotation:       string(b.UID),
	}
}

func managedForBroker(meta metav1.Object, b *eventingv1.Broker) bool {
	annotations := meta.GetAnnotations()
	return meta.GetLabels()[managedByLabelKey] == managedByLabelValue &&
		annotations[brokerNamespaceAnnotation] == b.Namespace &&
		annotations[brokerNameAnnotation] == b.Name &&
		annotations[brokerUIDAnnotation] == string(b.UID)
}

func (r *Reconciler) natsSecretNames() ([]string, error) {
	if r.natsConfigJSON == "" {
		return nil, nil
	}
	config, err := commonnats.ParseEventingNatsConfig(r.natsConfigJSON)
	if err != nil {
		return nil, fmt.Errorf("parse filter NATS configuration: %w", err)
	}
	var names []string
	if config.Auth != nil {
		if config.Auth.CredentialFile != nil && config.Auth.CredentialFile.Secret != nil {
			names = append(names, config.Auth.CredentialFile.Secret.Name)
		}
		if config.Auth.TLS != nil && config.Auth.TLS.Secret != nil {
			names = append(names, config.Auth.TLS.Secret.Name)
		}
	}
	if config.RootCA != nil && config.RootCA.Secret != nil {
		names = append(names, config.RootCA.Secret.Name)
	}
	sort.Strings(names)
	unique := names[:0]
	for _, name := range names {
		if name != "" && (len(unique) == 0 || name != unique[len(unique)-1]) {
			unique = append(unique, name)
		}
	}
	return unique, nil
}

// reconcileDataplaneRBAC grants one filter only the objects required by its
// namespace-scoped informers and its configured system-namespace credentials.
func (r *Reconciler) reconcileDataplaneRBAC(ctx context.Context, b *eventingv1.Broker) pkgreconciler.Event {
	identity := r.dataplaneIdentity(b)
	secretNames, err := r.natsSecretNames()
	if err != nil {
		return err
	}

	sa := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{
		Name: identity, Namespace: b.Namespace,
		Labels:          map[string]string{managedByLabelKey: managedByLabelValue},
		OwnerReferences: brokerOwnerReference(b),
	}}
	if err := r.reconcileServiceAccount(ctx, b, sa); err != nil {
		return err
	}

	subjects := []rbacv1.Subject{{Kind: rbacv1.ServiceAccountKind, Name: identity, Namespace: b.Namespace}}
	tenantBinding := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name: identity, Namespace: b.Namespace,
			Labels:          map[string]string{managedByLabelKey: managedByLabelValue},
			OwnerReferences: brokerOwnerReference(b),
		},
		Subjects: subjects,
		RoleRef:  rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: FilterReaderClusterRoleName},
	}
	if err := r.reconcileRoleBinding(ctx, b, tenantBinding, true); err != nil {
		return err
	}

	labels, annotations := managedMetadata(b)
	configBinding := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name: systemRBACName(identity, "-config"), Namespace: system.Namespace(),
			Labels: labels, Annotations: annotations,
		},
		Subjects: subjects,
		RoleRef:  rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: eventingConfigReaderRoleName},
	}
	if err := r.reconcileRoleBinding(ctx, b, configBinding, false); err != nil {
		return err
	}

	if err := r.reconcileNATSSecretRole(ctx, secretNames); err != nil {
		return err
	}
	secretName := systemRBACName(identity, "-secrets")
	if len(secretNames) == 0 {
		return r.deleteManagedRoleBinding(ctx, b, system.Namespace(), secretName, false)
	}
	secretBinding := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: system.Namespace(), Labels: labels, Annotations: annotations},
		Subjects:   subjects,
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: natsSecretRoleName},
	}
	return r.reconcileRoleBinding(ctx, b, secretBinding, false)
}

func (r *Reconciler) reconcileServiceAccount(ctx context.Context, b *eventingv1.Broker, expected *corev1.ServiceAccount) error {
	client := r.kubeClientSet.CoreV1().ServiceAccounts(expected.Namespace)
	existing, err := client.Get(ctx, expected.Name, metav1.GetOptions{})
	if apierrs.IsNotFound(err) {
		_, err = client.Create(ctx, expected, metav1.CreateOptions{})
		return err
	}
	if err != nil {
		return err
	}
	if !metav1.IsControlledBy(existing, b) {
		return fmt.Errorf("service account %s/%s is not owned by Broker UID %s", expected.Namespace, expected.Name, b.UID)
	}
	return nil
}

func (r *Reconciler) reconcileRoleBinding(ctx context.Context, b *eventingv1.Broker, expected *rbacv1.RoleBinding, namespacedOwner bool) error {
	client := r.kubeClientSet.RbacV1().RoleBindings(expected.Namespace)
	existing, err := client.Get(ctx, expected.Name, metav1.GetOptions{})
	if apierrs.IsNotFound(err) {
		_, err = client.Create(ctx, expected, metav1.CreateOptions{})
		return err
	}
	if err != nil {
		return err
	}
	if namespacedOwner {
		if !metav1.IsControlledBy(existing, b) {
			return fmt.Errorf("role binding %s/%s is not owned by Broker UID %s", expected.Namespace, expected.Name, b.UID)
		}
	} else if !managedForBroker(existing, b) {
		return fmt.Errorf("role binding %s/%s is not managed for Broker UID %s", expected.Namespace, expected.Name, b.UID)
	}
	if existing.RoleRef != expected.RoleRef {
		// RoleRef is immutable. Revoke the obsolete grant first and retry rather
		// than leaving a Pod running with broader or unrelated authority.
		if err := client.Delete(ctx, existing.Name, uidDeleteOptions(existing.UID)); err != nil {
			return err
		}
		return kncontroller.NewRequeueAfter(time.Second)
	}
	if equality.Semantic.DeepEqual(existing.Subjects, expected.Subjects) {
		return nil
	}
	updated := existing.DeepCopy()
	updated.Subjects = expected.Subjects
	_, err = client.Update(ctx, updated, metav1.UpdateOptions{})
	return err
}

func (r *Reconciler) reconcileNATSSecretRole(ctx context.Context, secretNames []string) error {
	rules := []rbacv1.PolicyRule(nil)
	if len(secretNames) > 0 {
		rules = []rbacv1.PolicyRule{{
			APIGroups: []string{""}, Resources: []string{"secrets"},
			ResourceNames: secretNames, Verbs: []string{"get"},
		}}
	}
	expected := &rbacv1.Role{ObjectMeta: metav1.ObjectMeta{
		Name: natsSecretRoleName, Namespace: system.Namespace(),
		Labels: map[string]string{managedByLabelKey: managedByLabelValue},
	}, Rules: rules}
	client := r.kubeClientSet.RbacV1().Roles(expected.Namespace)
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		existing, err := client.Get(ctx, expected.Name, metav1.GetOptions{})
		if apierrs.IsNotFound(err) {
			_, err = client.Create(ctx, expected, metav1.CreateOptions{})
			if apierrs.IsAlreadyExists(err) {
				return apierrs.NewConflict(rbacv1.Resource("roles"), expected.Name, err)
			}
			return err
		}
		if err != nil {
			return err
		}
		if existing.Labels[managedByLabelKey] != managedByLabelValue {
			return fmt.Errorf("role %s/%s is not managed by %s", expected.Namespace, expected.Name, managedByLabelValue)
		}
		if equality.Semantic.DeepEqual(existing.Rules, expected.Rules) {
			return nil
		}
		updated := existing.DeepCopy()
		updated.Rules = expected.Rules
		_, err = client.Update(ctx, updated, metav1.UpdateOptions{})
		return err
	})
}

// deleteDataplaneRBAC revokes cross-namespace access before deleting the
// account. UID checks prevent a stale finalizer from deleting replacement RBAC.
func (r *Reconciler) deleteDataplaneRBAC(ctx context.Context, b *eventingv1.Broker) error {
	identity := r.dataplaneIdentity(b)
	secretName := systemRBACName(identity, "-secrets")
	if err := r.deleteManagedRoleBinding(ctx, b, system.Namespace(), secretName, false); err != nil {
		return err
	}
	if err := r.deleteManagedRoleBinding(ctx, b, system.Namespace(), systemRBACName(identity, "-config"), false); err != nil {
		return err
	}
	if err := r.deleteManagedRoleBinding(ctx, b, b.Namespace, identity, true); err != nil {
		return err
	}
	return r.deleteManagedServiceAccount(ctx, b, identity)
}

func uidDeleteOptions(uid types.UID) metav1.DeleteOptions {
	return metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}}
}

func (r *Reconciler) deleteManagedRoleBinding(ctx context.Context, b *eventingv1.Broker, namespace, name string, namespacedOwner bool) error {
	client := r.kubeClientSet.RbacV1().RoleBindings(namespace)
	existing, err := client.Get(ctx, name, metav1.GetOptions{})
	if apierrs.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	owned := managedForBroker(existing, b)
	if namespacedOwner {
		owned = metav1.IsControlledBy(existing, b)
	}
	if !owned {
		return fmt.Errorf("refusing to delete role binding %s/%s not owned by Broker UID %s", namespace, name, b.UID)
	}
	return client.Delete(ctx, name, uidDeleteOptions(existing.UID))
}

func (r *Reconciler) deleteManagedServiceAccount(ctx context.Context, b *eventingv1.Broker, name string) error {
	client := r.kubeClientSet.CoreV1().ServiceAccounts(b.Namespace)
	existing, err := client.Get(ctx, name, metav1.GetOptions{})
	if apierrs.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if !metav1.IsControlledBy(existing, b) {
		return fmt.Errorf("refusing to delete service account %s/%s not owned by Broker UID %s", b.Namespace, name, b.UID)
	}
	return client.Delete(ctx, name, uidDeleteOptions(existing.UID))
}

func (r *Reconciler) runDataplaneRBACSweeper(ctx context.Context, brokerSynced cache.InformerSynced) {
	if !cache.WaitForCacheSync(ctx.Done(), brokerSynced) {
		return
	}
	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()
	for {
		if err := r.sweepOrphanedDataplaneBindings(ctx); err != nil {
			logging.FromContext(ctx).Errorw("Failed to sweep orphaned filter RBAC", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// sweepOrphanedDataplaneBindings revokes system-namespace grants left behind
// when a namespace or Broker was force-deleted without running its finalizer.
func (r *Reconciler) sweepOrphanedDataplaneBindings(ctx context.Context) error {
	if r.brokerLister == nil {
		return nil
	}
	if r.eventingClient == nil {
		return fmt.Errorf("eventing client is required to confirm orphaned RBAC")
	}
	selector := fmt.Sprintf("%s=%s", managedByLabelKey, managedByLabelValue)
	bindings, err := r.kubeClientSet.RbacV1().RoleBindings(system.Namespace()).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return err
	}
	var errs []error
	for i := range bindings.Items {
		binding := &bindings.Items[i]
		annotations := binding.Annotations
		namespace, name, uid := annotations[brokerNamespaceAnnotation], annotations[brokerNameAnnotation], annotations[brokerUIDAnnotation]
		if namespace == "" || name == "" || uid == "" {
			continue
		}
		_, getErr := r.brokerLister.Brokers(namespace).Get(name)
		if getErr != nil && !apierrs.IsNotFound(getErr) {
			errs = append(errs, fmt.Errorf("check Broker %s/%s for RoleBinding %s: %w", namespace, name, binding.Name, getErr))
			continue
		}
		// Informer caches on different replicas can momentarily disagree during
		// startup and rolling updates. Confirm the Broker's live UID before
		// revoking a cross-namespace credential grant.
		live, liveErr := r.eventingClient.EventingV1().Brokers(namespace).Get(ctx, name, metav1.GetOptions{})
		if liveErr == nil {
			if string(live.UID) == uid {
				// The sweeper handles only force deletion and same-name recreation.
				// Normal reconciliation owns the shape of grants for a live Broker.
				continue
			}
		} else if !apierrs.IsNotFound(liveErr) {
			errs = append(errs, fmt.Errorf("live check Broker %s/%s for RoleBinding %s: %w", namespace, name, binding.Name, liveErr))
			continue
		}
		if err := r.kubeClientSet.RbacV1().RoleBindings(system.Namespace()).Delete(ctx, binding.Name, uidDeleteOptions(binding.UID)); err != nil && !apierrs.IsNotFound(err) {
			errs = append(errs, fmt.Errorf("delete orphaned RoleBinding %s: %w", binding.Name, err))
			continue
		}
	}
	return errors.Join(errs...)
}
