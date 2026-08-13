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
	"reflect"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	kubefake "k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"

	eventingv1 "knative.dev/eventing/pkg/apis/eventing/v1"
	eventingfake "knative.dev/eventing/pkg/client/clientset/versioned/fake"
	eventinglisters "knative.dev/eventing/pkg/client/listers/eventing/v1"
)

func TestDataplaneIdentityIsUIDStableAndDNS1123(t *testing.T) {
	oldController := &Reconciler{filterServiceAccount: "old-filter-service-account-prefix"}
	newController := &Reconciler{filterServiceAccount: "new-filter-service-account-prefix"}
	broker := testBroker(strings.Repeat("namespace-", 8), strings.Repeat("broker-", 10))
	broker.UID = types.UID("11111111-2222-3333-4444-555555555555")

	first := oldController.dataplaneIdentity(broker)
	second := oldController.dataplaneIdentity(broker.DeepCopy())
	if first != second {
		t.Fatalf("identity changed for the same Broker UID: %q != %q", first, second)
	}
	if got := newController.dataplaneIdentity(broker); got != first {
		t.Fatalf("identity changed across controller configurations: old = %q, new = %q", first, got)
	}
	if len(first) > 63 {
		t.Errorf("identity length = %d, want <= 63: %q", len(first), first)
	}
	if errors := validation.IsDNS1123Label(first); len(errors) != 0 {
		t.Errorf("identity %q is not a DNS-1123 label: %v", first, errors)
	}

	recreated := broker.DeepCopy()
	recreated.UID = types.UID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
	if got := oldController.dataplaneIdentity(recreated); got == first {
		t.Errorf("recreated Broker identity = %q, want a new UID-derived identity", got)
	}
	for _, suffix := range []string{"-config", "-secrets"} {
		if got := systemRBACName(first, suffix); len(got) > 63 || len(validation.IsDNS1123Label(got)) != 0 {
			t.Errorf("system RBAC name %q is not a <=63 DNS-1123 label", got)
		}
	}
}

func TestNATSSecretNamesAreExactSortedAndDeduplicated(t *testing.T) {
	tests := []struct {
		name    string
		config  string
		want    []string
		wantErr bool
	}{
		{name: "empty", want: nil},
		{name: "no secret references", config: `{"url":"nats://nats:4222"}`, want: nil},
		{
			name: "all references sorted",
			config: `{
				"auth": {
					"credentialFile": {"secret": {"name": "z-creds"}},
					"tls": {"secret": {"name": "a-client"}}
				},
				"tls": {"secret": {"name": "m-root"}}
			}`,
			want: []string{"a-client", "m-root", "z-creds"},
		},
		{
			name: "duplicate and blank references",
			config: `{
				"auth": {
					"credentialFile": {"secret": {"name": "shared"}},
					"tls": {"secret": {"name": "shared"}}
				},
				"tls": {"secret": {"name": ""}}
			}`,
			want: []string{"shared"},
		},
		{name: "malformed", config: "{", wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			r := &Reconciler{natsConfigJSON: test.config}
			got, err := r.natsSecretNames()
			if (err != nil) != test.wantErr {
				t.Fatalf("natsSecretNames() error = %v, wantErr %v", err, test.wantErr)
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Errorf("natsSecretNames() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestReconcileDataplaneRBACRejectsForeignServiceAccount(t *testing.T) {
	t.Setenv("SYSTEM_NAMESPACE", "knative-eventing")
	broker := testBroker(testNamespace, testBrokerName)
	r := &Reconciler{filterServiceAccount: "dp", kubeClientSet: kubefake.NewSimpleClientset()}
	identity := r.dataplaneIdentity(broker)
	foreign := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{
		Name: identity, Namespace: broker.Namespace,
		OwnerReferences: []metav1.OwnerReference{{UID: "foreign-uid", Controller: boolPtr(true)}},
	}}
	r.kubeClientSet = kubefake.NewSimpleClientset(foreign)

	if err := r.reconcileDataplaneRBAC(testContext(), broker); err == nil {
		t.Fatal("reconcileDataplaneRBAC() accepted a foreign ServiceAccount")
	}
	for _, action := range r.kubeClientSet.(*kubefake.Clientset).Actions() {
		if action.GetVerb() == "create" || action.GetVerb() == "update" || action.GetVerb() == "patch" || action.GetVerb() == "delete" {
			t.Errorf("foreign ServiceAccount reconcile performed %s %s", action.GetVerb(), action.GetResource().Resource)
		}
	}
}

func TestReconcileNATSSecretRoleRejectsForeignSingleton(t *testing.T) {
	const systemNamespace = "knative-eventing"
	t.Setenv("SYSTEM_NAMESPACE", systemNamespace)
	foreign := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{Name: natsSecretRoleName, Namespace: systemNamespace},
		Rules:      []rbacv1.PolicyRule{{APIGroups: []string{"*"}, Resources: []string{"*"}, Verbs: []string{"*"}}},
	}
	kube := kubefake.NewSimpleClientset(foreign)
	r := &Reconciler{kubeClientSet: kube}

	if err := r.reconcileNATSSecretRole(testContext(), []string{"credentials"}); err == nil {
		t.Fatal("reconcileNATSSecretRole() accepted a foreign singleton Role")
	}
	for _, action := range kube.Actions() {
		if action.GetVerb() == "create" || action.GetVerb() == "update" || action.GetVerb() == "patch" || action.GetVerb() == "delete" {
			t.Errorf("foreign singleton Role reconcile performed %s %s", action.GetVerb(), action.GetResource().Resource)
		}
	}
	got, err := kube.RbacV1().Roles(systemNamespace).Get(context.Background(), natsSecretRoleName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.Rules, foreign.Rules) {
		t.Errorf("foreign singleton Role rules were mutated: %#v", got.Rules)
	}
}

func TestDeleteDataplaneRBACRejectsForeignSystemBinding(t *testing.T) {
	const systemNamespace = "knative-eventing"
	t.Setenv("SYSTEM_NAMESPACE", systemNamespace)
	broker := testBroker(testNamespace, testBrokerName)
	r := &Reconciler{filterServiceAccount: "dp", kubeClientSet: kubefake.NewSimpleClientset()}
	identity := r.dataplaneIdentity(broker)
	foreign := &rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{
		Name: systemRBACName(identity, "-secrets"), Namespace: systemNamespace,
		Labels: map[string]string{managedByLabelKey: managedByLabelValue},
		Annotations: map[string]string{
			brokerNamespaceAnnotation: broker.Namespace,
			brokerNameAnnotation:      broker.Name,
			brokerUIDAnnotation:       "foreign-uid",
		},
	}}
	r.kubeClientSet = kubefake.NewSimpleClientset(foreign)

	if err := r.deleteDataplaneRBAC(testContext(), broker); err == nil {
		t.Fatal("deleteDataplaneRBAC() accepted a foreign system RoleBinding")
	}
	for _, action := range r.kubeClientSet.(*kubefake.Clientset).Actions() {
		if action.GetVerb() == "delete" {
			t.Errorf("foreign RoleBinding cleanup deleted %s/%s", action.GetNamespace(), action.GetResource().Resource)
		}
	}
}

func TestDeleteDataplaneRBACUsesUIDPreconditions(t *testing.T) {
	const systemNamespace = "knative-eventing"
	t.Setenv("SYSTEM_NAMESPACE", systemNamespace)
	kube := kubefake.NewSimpleClientset()
	broker := testBroker(testNamespace, testBrokerName)
	r := &Reconciler{
		filterServiceAccount: "dp", kubeClientSet: kube,
		natsConfigJSON: `{"auth":{"credentialFile":{"secret":{"name":"credentials"}}}}`,
	}
	ctx := testContext()
	if err := r.reconcileDataplaneRBAC(ctx, broker); err != nil {
		t.Fatal(err)
	}

	identity := r.dataplaneIdentity(broker)
	setServiceAccountIdentity(t, kube, broker.Namespace, identity, "sa-uid", "sa-rv")
	setRoleBindingIdentity(t, kube, broker.Namespace, identity, "tenant-binding-uid", "tenant-binding-rv")
	setRoleBindingIdentity(t, kube, systemNamespace, systemRBACName(identity, "-config"), "config-binding-uid", "config-binding-rv")
	setRoleBindingIdentity(t, kube, systemNamespace, systemRBACName(identity, "-secrets"), "secret-binding-uid", "secret-binding-rv")
	kube.ClearActions()

	if err := r.deleteDataplaneRBAC(ctx, broker); err != nil {
		t.Fatal(err)
	}
	wantPreconditions := map[types.UID]string{
		"sa-uid": "sa-rv", "tenant-binding-uid": "tenant-binding-rv", "config-binding-uid": "config-binding-rv", "secret-binding-uid": "secret-binding-rv",
	}
	foundUIDs := make(map[types.UID]bool, len(wantPreconditions))
	deletes := 0
	for _, action := range kube.Actions() {
		if action.GetVerb() != "delete" {
			continue
		}
		deletes++
		options := action.(clienttesting.DeleteAction).GetDeleteOptions()
		if options.Preconditions == nil || options.Preconditions.UID == nil || options.Preconditions.ResourceVersion == nil {
			t.Errorf("delete %s/%s has incomplete preconditions: %#v", action.GetNamespace(), action.GetResource().Resource, options.Preconditions)
			continue
		}
		uid := *options.Preconditions.UID
		wantResourceVersion, ok := wantPreconditions[uid]
		if !ok {
			t.Errorf("unexpected delete UID precondition %q", uid)
		} else if got := *options.Preconditions.ResourceVersion; got != wantResourceVersion {
			t.Errorf("delete UID %q resourceVersion precondition = %q, want %q", uid, got, wantResourceVersion)
		}
		foundUIDs[uid] = true
		if options.PropagationPolicy != nil {
			t.Errorf("RBAC delete propagation = %v, want nil", options.PropagationPolicy)
		}
	}
	if deletes != len(wantPreconditions) {
		t.Errorf("delete actions = %d, want %d", deletes, len(wantPreconditions))
	}
	for uid := range wantPreconditions {
		if !foundUIDs[uid] {
			t.Errorf("no delete used UID precondition %q", uid)
		}
	}
	if _, err := kube.RbacV1().Roles(systemNamespace).Get(ctx, natsSecretRoleName, metav1.GetOptions{}); err != nil {
		t.Errorf("singleton NATS Secret Role was removed with one Broker: %v", err)
	}
}

func TestReconcileDataplaneRBACWithoutSecretsRevokesStaleBinding(t *testing.T) {
	const systemNamespace = "knative-eventing"
	t.Setenv("SYSTEM_NAMESPACE", systemNamespace)
	kube := kubefake.NewSimpleClientset()
	broker := testBroker(testNamespace, testBrokerName)
	r := &Reconciler{filterServiceAccount: "dp", kubeClientSet: kube}
	identity := r.dataplaneIdentity(broker)
	labels, annotations := managedMetadata(broker)
	stale := &rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{
		Name: systemRBACName(identity, "-secrets"), Namespace: systemNamespace, UID: "stale-binding-uid",
		Labels: labels, Annotations: annotations,
	}}
	if _, err := kube.RbacV1().RoleBindings(systemNamespace).Create(context.Background(), stale, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	if err := r.reconcileDataplaneRBAC(testContext(), broker); err != nil {
		t.Fatal(err)
	}
	if _, err := kube.RbacV1().RoleBindings(systemNamespace).Get(context.Background(), stale.Name, metav1.GetOptions{}); err == nil {
		t.Error("stale exact-secret RoleBinding remains with no Secret references")
	}
	role, err := kube.RbacV1().Roles(systemNamespace).Get(context.Background(), natsSecretRoleName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(role.Rules) != 0 {
		t.Errorf("no-secret singleton Role rules = %#v, want empty", role.Rules)
	}
}

func TestSweepStaleCacheNotFoundKeepsLiveCurrentBroker(t *testing.T) {
	const systemNamespace = "knative-eventing"
	t.Setenv("SYSTEM_NAMESPACE", systemNamespace)
	broker := testBroker(testNamespace, testBrokerName)
	r := &Reconciler{filterServiceAccount: "dp"}
	identity := r.dataplaneIdentity(broker)
	binding := managedSystemBinding(broker, identity, "-config", "binding-uid")
	kube := kubefake.NewSimpleClientset(binding)
	live := eventingfake.NewSimpleClientset(broker)
	r.kubeClientSet = kube
	r.eventingClient = live
	r.brokerLister = brokerLister()

	if err := r.sweepOrphanedDataplaneBindings(testContext()); err != nil {
		t.Fatal(err)
	}
	if _, err := kube.RbacV1().RoleBindings(systemNamespace).Get(context.Background(), binding.Name, metav1.GetOptions{}); err != nil {
		t.Errorf("live Broker binding was deleted after stale cache NotFound: %v", err)
	}
	if got := countActions(live.Actions(), "get", "brokers"); got != 1 {
		t.Errorf("live Broker GET actions = %d, want 1", got)
	}
	if got := countActions(kube.Actions(), "delete", "rolebindings"); got != 0 {
		t.Errorf("RoleBinding deletes = %d, want 0", got)
	}
}

func TestSweepRevalidatesCacheHitAgainstLiveBroker(t *testing.T) {
	const systemNamespace = "knative-eventing"
	t.Setenv("SYSTEM_NAMESPACE", systemNamespace)
	cached := testBroker(testNamespace, testBrokerName)
	cached.UID = "stale-broker-uid"
	liveBroker := cached.DeepCopy()
	liveBroker.UID = "current-broker-uid"
	r := &Reconciler{filterServiceAccount: "dp"}
	staleIdentity := r.dataplaneIdentity(cached)
	binding := managedSystemBinding(cached, staleIdentity, "-config", "binding-uid")
	kube := kubefake.NewSimpleClientset(binding)
	live := eventingfake.NewSimpleClientset(liveBroker)
	r.kubeClientSet = kube
	r.eventingClient = live
	r.brokerLister = brokerLister(cached)

	if err := r.sweepOrphanedDataplaneBindings(testContext()); err != nil {
		t.Fatal(err)
	}
	if _, err := kube.RbacV1().RoleBindings(systemNamespace).Get(context.Background(), binding.Name, metav1.GetOptions{}); err == nil {
		t.Error("binding for stale cached Broker UID survived live Broker revalidation")
	}
	if got := countActions(live.Actions(), "get", "brokers"); got != 1 {
		t.Errorf("live Broker GET actions = %d, want 1 even after a cache identity match", got)
	}
}

func TestControllerConfigurationSkewSharesOneDataplaneRBACSet(t *testing.T) {
	const systemNamespace = "knative-eventing"
	t.Setenv("SYSTEM_NAMESPACE", systemNamespace)
	broker := testBroker(testNamespace, testBrokerName)
	kube := kubefake.NewSimpleClientset()
	live := eventingfake.NewSimpleClientset(broker)
	oldReconciler := &Reconciler{
		filterServiceAccount: "old-prefix",
		natsConfigJSON:       `{"auth":{"credentialFile":{"secret":{"name":"credentials"}}}}`,
		kubeClientSet:        kube,
		eventingClient:       live,
		brokerLister:         brokerLister(broker),
	}
	newReconciler := &Reconciler{
		filterServiceAccount: "new-prefix",
		natsConfigJSON:       oldReconciler.natsConfigJSON,
		kubeClientSet:        kube,
		eventingClient:       live,
		brokerLister:         brokerLister(broker),
	}
	oldIdentity := oldReconciler.dataplaneIdentity(broker)
	newIdentity := newReconciler.dataplaneIdentity(broker)
	if oldIdentity != newIdentity {
		t.Fatalf("identity differs across controller configuration skew: old = %q, new = %q", oldIdentity, newIdentity)
	}

	if err := oldReconciler.reconcileDataplaneRBAC(testContext(), broker); err != nil {
		t.Fatal(err)
	}
	kube.ClearActions()
	if err := newReconciler.reconcileDataplaneRBAC(testContext(), broker); err != nil {
		t.Fatal(err)
	}
	for _, verb := range []string{"create", "update", "delete"} {
		if got := countActions(kube.Actions(), verb, "serviceaccounts") + countActions(kube.Actions(), verb, "roles") + countActions(kube.Actions(), verb, "rolebindings"); got != 0 {
			t.Errorf("second controller %s actions = %d, want 0", verb, got)
		}
	}

	serviceAccounts, err := kube.CoreV1().ServiceAccounts(broker.Namespace).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(serviceAccounts.Items) != 1 || serviceAccounts.Items[0].Name != oldIdentity {
		t.Fatalf("ServiceAccounts = %#v, want one identity %q", serviceAccounts.Items, oldIdentity)
	}
	tenantBindings, err := kube.RbacV1().RoleBindings(broker.Namespace).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(tenantBindings.Items) != 1 || tenantBindings.Items[0].Name != oldIdentity {
		t.Fatalf("tenant RoleBindings = %#v, want one identity %q", tenantBindings.Items, oldIdentity)
	}
	systemBindings, err := kube.RbacV1().RoleBindings(systemNamespace).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(systemBindings.Items) != 2 {
		t.Fatalf("system RoleBindings = %#v, want one config and one Secret binding", systemBindings.Items)
	}

	for name, reconciler := range map[string]*Reconciler{"old controller": oldReconciler, "new controller": newReconciler} {
		kube.ClearActions()
		live.ClearActions()
		if err := reconciler.sweepOrphanedDataplaneBindings(testContext()); err != nil {
			t.Fatalf("%s sweep: %v", name, err)
		}
		if got := countActions(kube.Actions(), "delete", "rolebindings"); got != 0 {
			t.Errorf("%s RoleBinding deletes = %d, want 0", name, got)
		}
		if got := countActions(live.Actions(), "get", "brokers"); got != 2 {
			t.Errorf("%s live Broker GET actions = %d, want 2", name, got)
		}
	}
	remaining, err := kube.RbacV1().RoleBindings(systemNamespace).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(remaining.Items) != 2 {
		t.Fatalf("system RoleBindings after both sweeps = %#v, want the single reconciled set", remaining.Items)
	}
}

func TestSweepSameUIDKeepsLegacyBindingShapeAcrossControllerConfigurationSkew(t *testing.T) {
	const systemNamespace = "knative-eventing"
	t.Setenv("SYSTEM_NAMESPACE", systemNamespace)
	broker := testBroker(testNamespace, testBrokerName)
	labels, annotations := managedMetadata(broker)
	binding := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "legacy-prefix-binding",
			Namespace:   systemNamespace,
			UID:         "binding-uid",
			Labels:      labels,
			Annotations: annotations,
		},
		Subjects: []rbacv1.Subject{{Kind: rbacv1.ServiceAccountKind, Name: "legacy-subject", Namespace: "legacy-namespace"}},
		RoleRef:  rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: "legacy-role"},
	}
	kube := kubefake.NewSimpleClientset(binding)
	live := eventingfake.NewSimpleClientset(broker)

	for name, prefix := range map[string]string{"old controller": "old-prefix", "new controller": "new-prefix"} {
		r := &Reconciler{
			filterServiceAccount: prefix,
			kubeClientSet:        kube,
			eventingClient:       live,
			brokerLister:         brokerLister(broker),
		}
		kube.ClearActions()
		live.ClearActions()
		if err := r.sweepOrphanedDataplaneBindings(testContext()); err != nil {
			t.Fatalf("%s sweep: %v", name, err)
		}
		if got := countActions(kube.Actions(), "delete", "rolebindings"); got != 0 {
			t.Errorf("%s RoleBinding deletes = %d, want 0", name, got)
		}
		if got := countActions(live.Actions(), "get", "brokers"); got != 1 {
			t.Errorf("%s live Broker GET actions = %d, want 1", name, got)
		}
	}
	if _, err := kube.RbacV1().RoleBindings(systemNamespace).Get(context.Background(), binding.Name, metav1.GetOptions{}); err != nil {
		t.Errorf("same-UID legacy-shaped binding was deleted during controller configuration skew: %v", err)
	}
}

func TestSweepLiveAPIErrorDoesNotDelete(t *testing.T) {
	const systemNamespace = "knative-eventing"
	t.Setenv("SYSTEM_NAMESPACE", systemNamespace)
	broker := testBroker(testNamespace, testBrokerName)
	r := &Reconciler{filterServiceAccount: "dp"}
	identity := r.dataplaneIdentity(broker)
	binding := managedSystemBinding(broker, identity, "-config", "binding-uid")
	kube := kubefake.NewSimpleClientset(binding)
	live := eventingfake.NewSimpleClientset()
	live.PrependReactor("get", "brokers", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("live API unavailable")
	})
	r.kubeClientSet = kube
	r.eventingClient = live
	r.brokerLister = brokerLister()

	if err := r.sweepOrphanedDataplaneBindings(testContext()); err == nil {
		t.Fatal("sweepOrphanedDataplaneBindings() succeeded despite live API error")
	}
	if _, err := kube.RbacV1().RoleBindings(systemNamespace).Get(context.Background(), binding.Name, metav1.GetOptions{}); err != nil {
		t.Errorf("binding was deleted after an inconclusive live API check: %v", err)
	}
	if got := countActions(kube.Actions(), "delete", "rolebindings"); got != 0 {
		t.Errorf("RoleBinding deletes = %d, want 0", got)
	}
}

func managedSystemBinding(broker *eventingv1.Broker, identity, suffix string, uid types.UID) *rbacv1.RoleBinding {
	labels, annotations := managedMetadata(broker)
	return &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name: systemRBACName(identity, suffix), Namespace: "knative-eventing", UID: uid,
			Labels: labels, Annotations: annotations,
		},
		Subjects: []rbacv1.Subject{{Kind: rbacv1.ServiceAccountKind, Name: identity, Namespace: broker.Namespace}},
	}
}

func brokerLister(brokers ...*eventingv1.Broker) eventinglisters.BrokerLister {
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	for _, broker := range brokers {
		_ = indexer.Add(broker)
	}
	return eventinglisters.NewBrokerLister(indexer)
}

func countActions(actions []clienttesting.Action, verb, resource string) int {
	count := 0
	for _, action := range actions {
		if action.Matches(verb, resource) {
			count++
		}
	}
	return count
}

func setServiceAccountIdentity(t *testing.T, kube *kubefake.Clientset, namespace, name string, uid types.UID, resourceVersion string) {
	t.Helper()
	object, err := kube.CoreV1().ServiceAccounts(namespace).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	object.UID = uid
	object.ResourceVersion = resourceVersion
	if _, err := kube.CoreV1().ServiceAccounts(namespace).Update(context.Background(), object, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
}

func setRoleBindingIdentity(t *testing.T, kube *kubefake.Clientset, namespace, name string, uid types.UID, resourceVersion string) {
	t.Helper()
	object, err := kube.RbacV1().RoleBindings(namespace).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	object.UID = uid
	object.ResourceVersion = resourceVersion
	if _, err := kube.RbacV1().RoleBindings(namespace).Update(context.Background(), object, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
}

func boolPtr(value bool) *bool { return &value }
