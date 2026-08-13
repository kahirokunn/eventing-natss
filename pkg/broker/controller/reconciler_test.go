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
	"testing"

	"github.com/nats-io/nats.go"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	kubefake "k8s.io/client-go/kubernetes/fake"
	appsv1listers "k8s.io/client-go/listers/apps/v1"
	corev1listers "k8s.io/client-go/listers/core/v1"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"

	"go.uber.org/zap"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"

	eventingv1 "knative.dev/eventing/pkg/apis/eventing/v1"
	eventinglisters "knative.dev/eventing/pkg/client/listers/eventing/v1"

	"knative.dev/eventing-natss/pkg/broker/autoscaler"
	brokerconfig "knative.dev/eventing-natss/pkg/broker/config"
	"knative.dev/eventing-natss/pkg/broker/contract"
	"knative.dev/eventing-natss/pkg/broker/controller/resources"
	brokerutils "knative.dev/eventing-natss/pkg/broker/utils"
	natsTesting "knative.dev/eventing-natss/pkg/channel/jetstream/dispatcher/testing"
)

const (
	testNamespace  = "test-ns"
	testBrokerName = "test-broker"
)

func testBroker(ns, name string) *eventingv1.Broker {
	return &eventingv1.Broker{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, UID: types.UID(name + "-uid")}}
}

func ownedFilterDeployment(broker *eventingv1.Broker, replicas int32) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: broker.Namespace,
			Name:      resources.FilterName(broker.Name),
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(
				broker,
				eventingv1.SchemeGroupVersion.WithKind("Broker"),
			)},
		},
		Spec: appsv1.DeploymentSpec{Replicas: ptr.To(replicas)},
	}
}

func testTrigger(ns, name, brokerName string) *eventingv1.Trigger {
	return &eventingv1.Trigger{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec:       eventingv1.TriggerSpec{Broker: brokerName},
	}
}

func TestHasTriggers(t *testing.T) {
	tests := []struct {
		name     string
		triggers []*eventingv1.Trigger
		want     bool
	}{
		{name: "no triggers", want: false},
		{
			name:     "matching trigger",
			triggers: []*eventingv1.Trigger{testTrigger(testNamespace, "t1", testBrokerName)},
			want:     true,
		},
		{
			name:     "only trigger for another broker",
			triggers: []*eventingv1.Trigger{testTrigger(testNamespace, "t1", "other-broker")},
			want:     false,
		},
		{
			name:     "matching trigger in another namespace is ignored",
			triggers: []*eventingv1.Trigger{testTrigger("other-ns", "t1", testBrokerName)},
			want:     false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			lister := newFakeTriggerLister()
			for _, tr := range tc.triggers {
				lister.add(tr)
			}
			r := &Reconciler{triggerLister: lister}

			got, err := r.hasTriggers(testBroker(testNamespace, testBrokerName))
			if err != nil {
				t.Fatalf("hasTriggers() error: %v", err)
			}
			if got != tc.want {
				t.Errorf("hasTriggers() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestDeleteFilter(t *testing.T) {
	name := resources.FilterName(testBrokerName)

	t.Run("deletes existing filter deployment and service", func(t *testing.T) {
		kube := kubefake.NewSimpleClientset(
			&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: name}},
			&corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: name}},
		)
		r := &Reconciler{kubeClientSet: kube}
		ctx := logging.WithLogger(context.Background(), zap.NewNop().Sugar())

		if err := r.deleteFilter(ctx, testBroker(testNamespace, testBrokerName)); err != nil {
			t.Fatalf("deleteFilter() error: %v", err)
		}

		if _, err := kube.AppsV1().Deployments(testNamespace).Get(ctx, name, metav1.GetOptions{}); !apierrs.IsNotFound(err) {
			t.Errorf("deployment: expected NotFound, got %v", err)
		}
		if _, err := kube.CoreV1().Services(testNamespace).Get(ctx, name, metav1.GetOptions{}); !apierrs.IsNotFound(err) {
			t.Errorf("service: expected NotFound, got %v", err)
		}
	})

	t.Run("no error when filter is already absent", func(t *testing.T) {
		r := &Reconciler{kubeClientSet: kubefake.NewSimpleClientset()}
		ctx := logging.WithLogger(context.Background(), zap.NewNop().Sugar())

		if err := r.deleteFilter(ctx, testBroker(testNamespace, testBrokerName)); err != nil {
			t.Errorf("deleteFilter() on absent filter returned error: %v", err)
		}
	})

	t.Run("surfaces a non-NotFound delete error and marks filter failed", func(t *testing.T) {
		kube := kubefake.NewSimpleClientset()
		kube.PrependReactor("delete", "deployments", func(clienttesting.Action) (bool, runtime.Object, error) {
			return true, nil, errors.New("boom")
		})
		r := &Reconciler{kubeClientSet: kube}
		ctx := logging.WithLogger(context.Background(), zap.NewNop().Sugar())

		b := testBroker(testNamespace, testBrokerName)
		if err := r.deleteFilter(ctx, b); err == nil {
			t.Fatal("deleteFilter() expected an error, got nil")
		}
		if cond := b.Status.GetCondition(eventingv1.BrokerConditionFilter); cond == nil || cond.IsTrue() {
			t.Errorf("expected BrokerConditionFilter to be marked failed, got %v", cond)
		}
	})
}

func testContext() context.Context {
	ctx := logging.WithLogger(context.Background(), zap.NewNop().Sugar())
	return controller.WithEventRecorder(ctx, record.NewFakeRecorder(100))
}

func newDeploymentLister(objs ...*appsv1.Deployment) appsv1listers.DeploymentLister {
	idx := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	for _, o := range objs {
		_ = idx.Add(o)
	}
	return appsv1listers.NewDeploymentLister(idx)
}

func newServiceLister(objs ...*corev1.Service) corev1listers.ServiceLister {
	idx := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	for _, o := range objs {
		_ = idx.Add(o)
	}
	return corev1listers.NewServiceLister(idx)
}

func deploymentWithReady(ns, name string, ready int32) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Status:     appsv1.DeploymentStatus{ReadyReplicas: ready},
	}
}

func TestReconcileDataplaneRBAC(t *testing.T) {
	t.Setenv("SYSTEM_NAMESPACE", "knative-eventing")
	kube := kubefake.NewSimpleClientset()
	r := &Reconciler{
		kubeClientSet:        kube,
		filterServiceAccount: "dp-sa",
		natsConfigJSON: `{
			"auth": {
				"credentialFile": {"secret": {"name": "nats-credentials"}},
				"tls": {"secret": {"name": "nats-client-tls"}}
			},
			"tls": {"secret": {"name": "nats-root-ca"}}
		}`,
	}
	ctx := testContext()
	oldBroker := testBroker(testNamespace, "recreated-broker")
	oldBroker.UID = "old-broker-uid"
	newBroker := testBroker(testNamespace, "recreated-broker")
	newBroker.UID = "new-broker-uid"
	// Model delayed garbage collection during delete/recreate. The new Broker
	// must never adopt the old Broker UID's credentials or ServiceAccount.
	brokers := []*eventingv1.Broker{oldBroker, newBroker}

	for _, broker := range brokers {
		if err := r.reconcileDataplaneRBAC(ctx, broker); err != nil {
			t.Fatalf("reconcileDataplaneRBAC(%s) error: %v", broker.Name, err)
		}
	}

	for _, action := range kube.Actions() {
		if action.GetResource().Resource == "clusterrolebindings" {
			t.Errorf("reconcileDataplaneRBAC issued forbidden cluster-scoped action: %#v", action)
		}
	}

	serviceAccounts, err := kube.CoreV1().ServiceAccounts(testNamespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(serviceAccounts.Items), len(brokers); got != want {
		t.Errorf("service accounts = %d, want one per Broker (%d)", got, want)
	}
	serviceAccountOwners := make(map[types.UID]string)
	serviceAccountNames := make(map[string]struct{})
	serviceAccountUIDs := make(map[string]types.UID)
	for _, serviceAccount := range serviceAccounts.Items {
		serviceAccountNames[serviceAccount.Name] = struct{}{}
		if len(serviceAccount.OwnerReferences) != 1 || serviceAccount.OwnerReferences[0].UID == "" {
			t.Errorf("ServiceAccount %q owner references = %#v, want its Broker controller", serviceAccount.Name, serviceAccount.OwnerReferences)
			continue
		}
		serviceAccountOwners[serviceAccount.OwnerReferences[0].UID] = serviceAccount.Name
		serviceAccountUIDs[serviceAccount.Name] = serviceAccount.OwnerReferences[0].UID
	}
	for _, broker := range brokers {
		if serviceAccountOwners[broker.UID] == "" {
			t.Errorf("Broker %s/%s UID %q has no dedicated owned ServiceAccount", broker.Namespace, broker.Name, broker.UID)
		}
	}

	tenantBindings, err := kube.RbacV1().RoleBindings(testNamespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(tenantBindings.Items), len(brokers); got != want {
		t.Errorf("tenant RoleBindings = %d, want one per Broker (%d)", got, want)
	}
	for _, binding := range tenantBindings.Items {
		if binding.RoleRef.Kind != "ClusterRole" || binding.RoleRef.Name != FilterReaderClusterRoleName {
			t.Errorf("tenant RoleBinding %q roleRef = %#v, want ClusterRole %q", binding.Name, binding.RoleRef, FilterReaderClusterRoleName)
		}
		if len(binding.Subjects) != 1 || binding.Subjects[0].Kind != "ServiceAccount" || binding.Subjects[0].Namespace != testNamespace {
			t.Errorf("tenant RoleBinding %q subjects = %#v, want one tenant ServiceAccount", binding.Name, binding.Subjects)
			continue
		}
		if _, ok := serviceAccountNames[binding.Subjects[0].Name]; !ok {
			t.Errorf("tenant RoleBinding %q refers to unknown ServiceAccount %q", binding.Name, binding.Subjects[0].Name)
		}
		if len(binding.OwnerReferences) != 1 || binding.OwnerReferences[0].UID != serviceAccountUIDs[binding.Subjects[0].Name] {
			t.Errorf("tenant RoleBinding %q owner references = %#v, want the same Broker as ServiceAccount %q", binding.Name, binding.OwnerReferences, binding.Subjects[0].Name)
		}
	}

	// Running again for the same Broker must not create or update RBAC.
	actionsBefore := len(kube.Actions())
	if err := r.reconcileDataplaneRBAC(ctx, brokers[0]); err != nil {
		t.Errorf("second reconcileDataplaneRBAC() error: %v", err)
	}
	for _, action := range kube.Actions()[actionsBefore:] {
		if action.GetVerb() == "create" || action.GetVerb() == "update" || action.GetVerb() == "patch" {
			t.Errorf("steady-state RBAC reconcile wrote %s %s", action.GetVerb(), action.GetResource().Resource)
		}
	}
}

func TestReconcileDataplaneRBACScopesSystemAccess(t *testing.T) {
	const systemNamespace = "knative-eventing"
	t.Setenv("SYSTEM_NAMESPACE", systemNamespace)
	kube := kubefake.NewSimpleClientset()
	r := &Reconciler{
		kubeClientSet:        kube,
		filterServiceAccount: "dp-sa",
		natsConfigJSON: `{
			"auth": {
				"credentialFile": {"secret": {"name": "nats-credentials"}},
				"tls": {"secret": {"name": "nats-client-tls"}}
			},
			"tls": {"secret": {"name": "nats-root-ca"}}
		}`,
	}
	ctx := testContext()
	broker := testBroker(testNamespace, testBrokerName)

	if err := r.reconcileDataplaneRBAC(ctx, broker); err != nil {
		t.Fatalf("reconcileDataplaneRBAC() error: %v", err)
	}

	bindings, err := kube.RbacV1().RoleBindings(systemNamespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(bindings.Items), 2; got != want {
		t.Errorf("system RoleBindings = %d, want config-reader and exact-secret bindings", got)
	}
	configReaderFound := false
	for _, binding := range bindings.Items {
		if len(binding.Subjects) != 1 || binding.Subjects[0].Kind != "ServiceAccount" || binding.Subjects[0].Namespace != broker.Namespace {
			t.Errorf("system RoleBinding %q subjects = %#v, want the Broker ServiceAccount", binding.Name, binding.Subjects)
		}
		if binding.RoleRef.Kind == "ClusterRole" && binding.RoleRef.Name == "eventing-config-reader" {
			configReaderFound = true
		}
		if !metadataContainsValue(binding.Labels, binding.Annotations, string(broker.UID)) {
			t.Errorf("system RoleBinding %q has no Broker UID marker for safe cleanup: labels=%v annotations=%v", binding.Name, binding.Labels, binding.Annotations)
		}
	}
	if !configReaderFound {
		t.Error("system namespace has no eventing-config-reader RoleBinding for the Broker ServiceAccount")
	}

	roles, err := kube.RbacV1().Roles(systemNamespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(roles.Items), 1; got != want {
		t.Fatalf("generated system Roles = %d, want one exact-secret Role", got)
	}
	if got, want := roles.Items[0].Name, natsSecretRoleName; got != want {
		t.Errorf("singleton system Role = %q, want %q", got, want)
	}
	if metadataContainsValue(roles.Items[0].Labels, roles.Items[0].Annotations, string(broker.UID)) {
		t.Errorf("singleton system Role %q must not be owned by one Broker UID: labels=%v annotations=%v", roles.Items[0].Name, roles.Items[0].Labels, roles.Items[0].Annotations)
	}
	secretBindingFound := false
	for _, binding := range bindings.Items {
		if binding.RoleRef.Kind == "Role" && binding.RoleRef.Name == roles.Items[0].Name {
			secretBindingFound = true
		}
	}
	if !secretBindingFound {
		t.Errorf("no system RoleBinding refers to exact-secret Role %q", roles.Items[0].Name)
	}
	wantSecrets := map[string]bool{
		"nats-credentials": false,
		"nats-client-tls":  false,
		"nats-root-ca":     false,
	}
	for _, rule := range roles.Items[0].Rules {
		if len(rule.APIGroups) != 1 || rule.APIGroups[0] != "" || len(rule.Resources) != 1 || rule.Resources[0] != "secrets" {
			t.Errorf("exact-secret Role contains unrelated rule: %#v", rule)
			continue
		}
		if len(rule.Verbs) != 1 || rule.Verbs[0] != "get" {
			t.Errorf("Secret verbs = %v, want [get]", rule.Verbs)
		}
		for _, name := range rule.ResourceNames {
			if _, ok := wantSecrets[name]; !ok {
				t.Errorf("unexpected Secret resourceName %q", name)
			} else {
				wantSecrets[name] = true
			}
		}
	}
	for name, found := range wantSecrets {
		if !found {
			t.Errorf("exact-secret Role does not grant get on %q", name)
		}
	}
}

func metadataContainsValue(labels, annotations map[string]string, want string) bool {
	for _, value := range labels {
		if value == want {
			return true
		}
	}
	for _, value := range annotations {
		if value == want {
			return true
		}
	}
	return false
}

func TestGetBrokerConfig(t *testing.T) {
	ctx := testContext()

	t.Run("from broker annotation", func(t *testing.T) {
		r := &Reconciler{kubeClientSet: kubefake.NewSimpleClientset()}
		b := testBroker(testNamespace, testBrokerName)
		b.Annotations = map[string]string{brokerconfig.BrokerConfigAnnotation: `{"stream":{"replicas":3}}`}
		cfg, err := r.getBrokerConfig(ctx, b)
		if err != nil {
			t.Fatalf("getBrokerConfig() error: %v", err)
		}
		if cfg.Stream == nil || cfg.Stream.Replicas != 3 {
			t.Errorf("Stream.Replicas = %+v, want 3", cfg.Stream)
		}
	})

	t.Run("hardcoded defaults when no configmap", func(t *testing.T) {
		r := &Reconciler{kubeClientSet: kubefake.NewSimpleClientset()}
		b := testBroker(testNamespace, testBrokerName)
		cfg, err := r.getBrokerConfig(ctx, b)
		if err != nil {
			t.Fatalf("getBrokerConfig() error: %v", err)
		}
		if cfg.Stream.Replicas != 1 {
			t.Errorf("Stream.Replicas = %d, want 1 (default)", cfg.Stream.Replicas)
		}
	})
}

func TestReconcileStream(t *testing.T) {
	s := natsTesting.RunBasicJetstreamServer()
	defer natsTesting.ShutdownJSServerAndRemoveStorage(t, s)
	conn, js := natsTesting.JsClient(t, s)
	defer conn.Close()

	r := &Reconciler{js: js}
	ctx := testContext()
	b := testBroker(testNamespace, testBrokerName)
	streamName := brokerutils.BrokerStreamName(b)
	publish := brokerutils.BrokerPublishSubjectName(b.Namespace, b.Name)

	if err := r.reconcileStream(ctx, b, streamName, publish, brokerconfig.DefaultBrokerConfig()); err != nil {
		t.Fatalf("reconcileStream() error: %v", err)
	}
	if _, err := js.StreamInfo(streamName); err != nil {
		t.Fatalf("stream not created: %v", err)
	}
	// Idempotent when the stream already exists.
	if err := r.reconcileStream(ctx, b, streamName, publish, brokerconfig.DefaultBrokerConfig()); err != nil {
		t.Errorf("second reconcileStream() error: %v", err)
	}
}

func TestPropagateIngressAvailability(t *testing.T) {
	const ingressNS, ingressName = "knative-eventing", "nats-broker-ingress"
	tests := []struct {
		name      string
		dep       *appsv1.Deployment
		wantReady bool
	}{
		{name: "ready", dep: deploymentWithReady(ingressNS, ingressName, 1), wantReady: true},
		{name: "no ready replicas", dep: deploymentWithReady(ingressNS, ingressName, 0), wantReady: false},
		{name: "missing", dep: nil, wantReady: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var deps []*appsv1.Deployment
			if tc.dep != nil {
				deps = append(deps, tc.dep)
			}
			r := &Reconciler{deploymentLister: newDeploymentLister(deps...), ingressServiceName: ingressName, ingressNamespace: ingressNS}
			b := testBroker(testNamespace, testBrokerName)
			if err := r.propagateIngressAvailability(testContext(), b); err != nil {
				t.Fatalf("propagateIngressAvailability() error: %v", err)
			}
			cond := b.Status.GetCondition(eventingv1.BrokerConditionIngress)
			if tc.wantReady != (cond != nil && cond.IsTrue()) {
				t.Errorf("ingress condition = %v, wantReady %v", cond, tc.wantReady)
			}
		})
	}
}

func TestPropagateFilterAvailability(t *testing.T) {
	filterName := resources.FilterName(testBrokerName)
	tests := []struct {
		name       string
		dep        *appsv1.Deployment
		mode       filterAvailabilityMode
		wantReady  bool
		wantReason string
	}{
		{name: "ready", dep: deploymentWithReady(testNamespace, filterName, 1), wantReady: true},
		{name: "no ready replicas", dep: deploymentWithReady(testNamespace, filterName, 0), wantReady: false},
		{name: "missing", dep: nil, wantReady: false},
		{
			name: "autoscaled to zero is ready",
			dep: &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: filterName},
				Spec:       appsv1.DeploymentSpec{Replicas: ptr.To[int32](0)},
			},
			mode:       filterAvailabilityAutoscaled,
			wantReady:  true,
			wantReason: "ScaledToZero",
		},
		{
			name:       "fallback replica is ready with reason",
			dep:        deploymentWithReady(testNamespace, filterName, 1),
			mode:       filterAvailabilityFallback,
			wantReady:  true,
			wantReason: ReasonAutoscalerFallback,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var deps []*appsv1.Deployment
			if tc.dep != nil {
				deps = append(deps, tc.dep)
			}
			r := &Reconciler{deploymentLister: newDeploymentLister(deps...)}
			b := testBroker(testNamespace, testBrokerName)
			if err := r.propagateFilterAvailability(testContext(), b, nil, tc.mode); err != nil {
				t.Fatalf("propagateFilterAvailability() error: %v", err)
			}
			cond := b.Status.GetCondition(eventingv1.BrokerConditionFilter)
			if tc.wantReady != (cond != nil && cond.IsTrue()) {
				t.Errorf("filter condition = %v, wantReady %v", cond, tc.wantReady)
			}
			if tc.wantReason != "" && (cond == nil || cond.Reason != tc.wantReason) {
				t.Errorf("filter condition reason = %v, want %q", cond, tc.wantReason)
			}
		})
	}
}

func TestReconcileFilterServiceCreate(t *testing.T) {
	kube := kubefake.NewSimpleClientset()
	r := &Reconciler{kubeClientSet: kube, serviceLister: newServiceLister()}
	b := testBroker(testNamespace, testBrokerName)

	svc, err := r.reconcileFilterService(testContext(), b)
	if err != nil {
		t.Fatalf("reconcileFilterService() error: %v", err)
	}
	if svc == nil {
		t.Fatal("reconcileFilterService() returned nil service")
	}
	if _, gerr := kube.CoreV1().Services(testNamespace).Get(context.Background(), resources.FilterName(testBrokerName), metav1.GetOptions{}); gerr != nil {
		t.Errorf("filter service not created: %v", gerr)
	}
}

func TestReconcileFilterDeploymentCreate(t *testing.T) {
	kube := kubefake.NewSimpleClientset()
	r := &Reconciler{
		kubeClientSet:        kube,
		deploymentLister:     newDeploymentLister(),
		filterImage:          "filter:latest",
		filterServiceAccount: "dp-sa",
		natsURL:              "nats://localhost:4222",
	}
	b := testBroker(testNamespace, testBrokerName)

	if err := r.reconcileFilterDeployment(testContext(), b, "TEST_STREAM", brokerconfig.DefaultBrokerConfig()); err != nil {
		t.Fatalf("reconcileFilterDeployment() error: %v", err)
	}
	if _, gerr := kube.AppsV1().Deployments(testNamespace).Get(context.Background(), resources.FilterName(testBrokerName), metav1.GetOptions{}); gerr != nil {
		t.Errorf("filter deployment not created: %v", gerr)
	}
}

func TestReconcileFilterServiceUpdate(t *testing.T) {
	name := resources.FilterName(testBrokerName)
	// Existing service with an empty spec differs from the expected spec, so the
	// update branch runs.
	existing := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: name}}
	kube := kubefake.NewSimpleClientset(existing)
	r := &Reconciler{kubeClientSet: kube, serviceLister: newServiceLister(existing)}

	if _, err := r.reconcileFilterService(testContext(), testBroker(testNamespace, testBrokerName)); err != nil {
		t.Fatalf("reconcileFilterService() error: %v", err)
	}
	got, err := kube.CoreV1().Services(testNamespace).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get service: %v", err)
	}
	if len(got.Spec.Ports) == 0 {
		t.Error("service spec was not updated to the expected spec")
	}
}

func TestReconcileFilterDeploymentUpdate(t *testing.T) {
	b := testBroker(testNamespace, testBrokerName)
	name := resources.FilterName(b.Name)
	existing := ownedFilterDeployment(b, 1)
	kube := kubefake.NewSimpleClientset(existing)
	r := &Reconciler{
		kubeClientSet:        kube,
		deploymentLister:     newDeploymentLister(existing),
		filterImage:          "filter:latest",
		filterServiceAccount: "dp-sa",
		natsURL:              "nats://localhost:4222",
		natsConfigJSON:       `{"url":"tls://nats.example:4222"}`,
	}

	if err := r.reconcileFilterDeployment(testContext(), b, "TEST_STREAM", brokerconfig.DefaultBrokerConfig()); err != nil {
		t.Fatalf("reconcileFilterDeployment() error: %v", err)
	}
	got, err := kube.AppsV1().Deployments(testNamespace).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get deployment: %v", err)
	}
	if len(got.Spec.Template.Spec.Containers) == 0 {
		t.Error("deployment spec was not updated to the expected spec")
	}
	envValues := make(map[string]string)
	for _, env := range got.Spec.Template.Spec.Containers[0].Env {
		envValues[env.Name] = env.Value
	}
	if got, want := envValues["NATS_CONFIG"], r.natsConfigJSON; got != want {
		t.Errorf("NATS_CONFIG = %q, want controller snapshot %q", got, want)
	}
}

func TestReconcileFilterDeploymentRepairsRequiredLabelsAndPreservesCustomLabels(t *testing.T) {
	b := testBroker(testNamespace, testBrokerName)
	existing := resources.MakeFilterDeployment(&resources.FilterArgs{
		Broker:             b,
		Image:              "filter:latest",
		ServiceAccountName: "dp-sa",
		StreamName:         "TEST_STREAM",
		NatsURL:            "nats://localhost:4222",
	})
	existing.Labels[resources.BrokerLabelKey] = "wrong-broker"
	existing.Labels[resources.RoleLabelKey] = "wrong-role"
	existing.Labels["custom-existing-label"] = "kept"
	kube := kubefake.NewSimpleClientset(existing)
	r := &Reconciler{
		kubeClientSet:        kube,
		deploymentLister:     newDeploymentLister(existing),
		filterImage:          "filter:latest",
		filterServiceAccount: "dp-sa",
		natsURL:              "nats://localhost:4222",
	}

	if err := r.reconcileFilterDeployment(testContext(), b, "TEST_STREAM", brokerconfig.DefaultBrokerConfig()); err != nil {
		t.Fatalf("reconcileFilterDeployment() error: %v", err)
	}
	got, err := kube.AppsV1().Deployments(b.Namespace).Get(context.Background(), existing.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for key, want := range resources.FilterLabels(b.Name) {
		if got.Labels[key] != want {
			t.Errorf("Deployment label %q = %q, want repaired value %q", key, got.Labels[key], want)
		}
	}
	if got.Labels["custom-existing-label"] != "kept" {
		t.Errorf("custom existing label = %q, want kept", got.Labels["custom-existing-label"])
	}
	updates := 0
	for _, action := range kube.Actions() {
		if action.Matches("update", "deployments") {
			updates++
		}
	}
	if updates != 1 {
		t.Fatalf("Deployment updates = %d, want exactly 1", updates)
	}
}

func TestReconcileFilterDeploymentReplicaPolicy(t *testing.T) {
	name := resources.FilterName(testBrokerName)
	for _, tc := range []struct {
		name         string
		policy       filterReplicaPolicy
		existing     int32
		wantReplicas int32
	}{
		{name: "autoscaled preserves KEDA zero", policy: autoscaledReplicaPolicy(autoscaler.Settings{MinScale: 0}), existing: 0, wantReplicas: 0},
		{name: "fallback forces one", policy: fallbackReplicaPolicy(1), existing: 0, wantReplicas: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := testBroker(testNamespace, testBrokerName)
			existing := ownedFilterDeployment(b, tc.existing)
			kube := kubefake.NewSimpleClientset(existing)
			r := &Reconciler{
				kubeClientSet:        kube,
				deploymentLister:     newDeploymentLister(existing),
				filterImage:          "filter:latest",
				filterServiceAccount: "dp-sa",
				natsURL:              "nats://localhost:4222",
			}

			if err := r.reconcileFilterDeployment(testContext(), b, "TEST_STREAM", brokerconfig.DefaultBrokerConfig(), tc.policy); err != nil {
				t.Fatal(err)
			}
			got, err := kube.AppsV1().Deployments(testNamespace).Get(context.Background(), name, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if got.Spec.Replicas == nil || *got.Spec.Replicas != tc.wantReplicas {
				t.Fatalf("replicas = %v, want %d", got.Spec.Replicas, tc.wantReplicas)
			}
		})
	}
}

func TestEnqueueBrokerOfTrigger(t *testing.T) {
	var got []types.NamespacedName
	h := enqueueBrokerOfTrigger(func(k types.NamespacedName) { got = append(got, k) })

	h(testTrigger(testNamespace, "t1", "broker-a"))                                                // enqueues broker-a
	h(&corev1.Pod{})                                                                               // not a trigger → ignored
	h(cache.DeletedFinalStateUnknown{Key: "k", Obj: testTrigger(testNamespace, "t2", "broker-b")}) // tombstone → broker-b
	h(testTrigger(testNamespace, "t3", ""))                                                        // empty broker ref → ignored

	want := []types.NamespacedName{
		{Namespace: testNamespace, Name: "broker-a"},
		{Namespace: testNamespace, Name: "broker-b"},
	}
	if len(got) != len(want) {
		t.Fatalf("enqueued %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("enqueued[%d] = %v, want %v", i, got[i], want[i])
		}
	}
}

func TestFinalizeKind(t *testing.T) {
	t.Setenv("SYSTEM_NAMESPACE", "knative-eventing")

	s := natsTesting.RunBasicJetstreamServer()
	defer natsTesting.ShutdownJSServerAndRemoveStorage(t, s)
	conn, js := natsTesting.JsClient(t, s)
	defer conn.Close()

	kube := kubefake.NewSimpleClientset()
	cmLister := corev1listers.NewConfigMapLister(cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc}))
	r := &Reconciler{
		kubeClientSet: kube, js: js, contractManager: contract.NewManager(kube, cmLister),
		filterServiceAccount: "dp-sa",
		natsConfigJSON:       `{"auth":{"credentialFile":{"secret":{"name":"credentials"}}}}`,
	}
	ctx := testContext()
	b := testBroker(testNamespace, testBrokerName)
	if err := r.reconcileDataplaneRBAC(ctx, b); err != nil {
		t.Fatalf("reconcileDataplaneRBAC() error: %v", err)
	}

	streamName := brokerutils.BrokerStreamName(b)
	if _, err := js.AddStream(&nats.StreamConfig{
		Name:     streamName,
		Subjects: []string{brokerutils.BrokerPublishSubjectName(b.Namespace, b.Name) + ".>"},
	}); err != nil {
		t.Fatalf("AddStream() error: %v", err)
	}

	if err := r.FinalizeKind(ctx, b); err != nil {
		t.Fatalf("FinalizeKind() error: %v", err)
	}
	if _, err := js.StreamInfo(streamName); !errors.Is(err, nats.ErrStreamNotFound) {
		t.Errorf("stream not deleted: got err %v", err)
	}
	identity := r.dataplaneIdentity(b)
	if _, err := kube.CoreV1().ServiceAccounts(b.Namespace).Get(ctx, identity, metav1.GetOptions{}); !apierrs.IsNotFound(err) {
		t.Errorf("finalized ServiceAccount still exists: %v", err)
	}
	for _, key := range []types.NamespacedName{
		{Namespace: b.Namespace, Name: identity},
		{Namespace: "knative-eventing", Name: systemRBACName(identity, "-config")},
		{Namespace: "knative-eventing", Name: systemRBACName(identity, "-secrets")},
	} {
		if _, err := kube.RbacV1().RoleBindings(key.Namespace).Get(ctx, key.Name, metav1.GetOptions{}); !apierrs.IsNotFound(err) {
			t.Errorf("finalized RoleBinding %s still exists: %v", key, err)
		}
	}
	if _, err := kube.RbacV1().Roles("knative-eventing").Get(ctx, natsSecretRoleName, metav1.GetOptions{}); err != nil {
		t.Errorf("singleton NATS Secret Role was deleted during Broker finalization: %v", err)
	}
}

func TestReconcileKind(t *testing.T) {
	const ingressNS, ingressName = "knative-eventing", "nats-broker-ingress"

	setup := func(t *testing.T, triggers ...*eventingv1.Trigger) (*Reconciler, *kubefake.Clientset) {
		t.Setenv("SYSTEM_NAMESPACE", ingressNS)
		s := natsTesting.RunBasicJetstreamServer()
		t.Cleanup(func() { natsTesting.ShutdownJSServerAndRemoveStorage(t, s) })
		conn, js := natsTesting.JsClient(t, s)
		t.Cleanup(func() { conn.Close() })

		kube := kubefake.NewSimpleClientset()
		cmLister := corev1listers.NewConfigMapLister(cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc}))
		triggerLister := newFakeTriggerLister()
		for _, tr := range triggers {
			triggerLister.add(tr)
		}
		r := &Reconciler{
			kubeClientSet:        kube,
			js:                   js,
			triggerLister:        triggerLister,
			deploymentLister:     newDeploymentLister(deploymentWithReady(ingressNS, ingressName, 1)),
			serviceLister:        newServiceLister(),
			contractManager:      contract.NewManager(kube, cmLister),
			filterImage:          "filter:latest",
			filterServiceAccount: "dp-sa",
			natsURL:              "nats://localhost:4222",
			ingressServiceName:   ingressName,
			ingressNamespace:     ingressNS,
		}
		return r, kube
	}

	t.Run("no triggers: no filter created, filter condition NoTriggers", func(t *testing.T) {
		r, kube := setup(t)
		b := testBroker(testNamespace, testBrokerName)

		if err := r.ReconcileKind(testContext(), b); err != nil {
			t.Fatalf("ReconcileKind() error: %v", err)
		}
		if _, err := kube.AppsV1().Deployments(testNamespace).Get(context.Background(), resources.FilterName(testBrokerName), metav1.GetOptions{}); !apierrs.IsNotFound(err) {
			t.Errorf("filter deployment should not exist, got err %v", err)
		}
		cond := b.Status.GetCondition(eventingv1.BrokerConditionFilter)
		if cond == nil || cond.Reason != "NoTriggers" {
			t.Errorf("filter condition = %v, want reason NoTriggers", cond)
		}
	})

	t.Run("no triggers: RBAC is revoked even when filter deletion fails", func(t *testing.T) {
		r, kube := setup(t)
		r.natsConfigJSON = `{"auth":{"credentialFile":{"secret":{"name":"credentials"}}}}`
		b := testBroker(testNamespace, testBrokerName)
		if err := r.reconcileDataplaneRBAC(testContext(), b); err != nil {
			t.Fatal(err)
		}
		if _, err := kube.AppsV1().Deployments(b.Namespace).Create(context.Background(), &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: resources.FilterName(b.Name), Namespace: b.Namespace},
		}, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
		kube.PrependReactor("delete", "deployments", func(clienttesting.Action) (bool, runtime.Object, error) {
			return true, nil, errors.New("deployment delete failed")
		})

		if err := r.ReconcileKind(testContext(), b); err == nil {
			t.Fatal("ReconcileKind() succeeded despite filter deletion failure")
		}
		identity := r.dataplaneIdentity(b)
		if _, err := kube.CoreV1().ServiceAccounts(b.Namespace).Get(context.Background(), identity, metav1.GetOptions{}); !apierrs.IsNotFound(err) {
			t.Errorf("ServiceAccount was not revoked after filter deletion failure: %v", err)
		}
		for _, key := range []types.NamespacedName{
			{Namespace: b.Namespace, Name: identity},
			{Namespace: ingressNS, Name: systemRBACName(identity, "-config")},
			{Namespace: ingressNS, Name: systemRBACName(identity, "-secrets")},
		} {
			if _, err := kube.RbacV1().RoleBindings(key.Namespace).Get(context.Background(), key.Name, metav1.GetOptions{}); !apierrs.IsNotFound(err) {
				t.Errorf("RoleBinding %s was not revoked after filter deletion failure: %v", key, err)
			}
		}
	})

	t.Run("with a trigger: filter deployment is created", func(t *testing.T) {
		r, kube := setup(t, testTrigger(testNamespace, "t1", testBrokerName))
		b := testBroker(testNamespace, testBrokerName)

		if err := r.ReconcileKind(testContext(), b); err != nil {
			t.Fatalf("ReconcileKind() error: %v", err)
		}
		deployment, err := kube.AppsV1().Deployments(testNamespace).Get(context.Background(), resources.FilterName(testBrokerName), metav1.GetOptions{})
		if err != nil {
			t.Errorf("filter deployment should be created: %v", err)
			return
		}
		if got, want := deployment.Spec.Template.Spec.ServiceAccountName, r.dataplaneIdentity(b); got != want {
			t.Errorf("filter ServiceAccount = %q, want per-Broker identity %q", got, want)
		}
	})
}

// fakeTriggerLister implements eventinglisters.TriggerLister for testing.
type fakeTriggerLister struct {
	triggers map[string]map[string]*eventingv1.Trigger
}

func newFakeTriggerLister() *fakeTriggerLister {
	return &fakeTriggerLister{triggers: make(map[string]map[string]*eventingv1.Trigger)}
}

func (f *fakeTriggerLister) add(tr *eventingv1.Trigger) {
	if f.triggers[tr.Namespace] == nil {
		f.triggers[tr.Namespace] = make(map[string]*eventingv1.Trigger)
	}
	f.triggers[tr.Namespace][tr.Name] = tr
}

func (f *fakeTriggerLister) List(labels.Selector) ([]*eventingv1.Trigger, error) {
	var result []*eventingv1.Trigger
	for _, ns := range f.triggers {
		for _, tr := range ns {
			result = append(result, tr)
		}
	}
	return result, nil
}

func (f *fakeTriggerLister) Triggers(namespace string) eventinglisters.TriggerNamespaceLister {
	return &fakeTriggerNamespaceLister{triggers: f.triggers[namespace]}
}

type fakeTriggerNamespaceLister struct {
	triggers map[string]*eventingv1.Trigger
}

func (f *fakeTriggerNamespaceLister) List(labels.Selector) ([]*eventingv1.Trigger, error) {
	result := make([]*eventingv1.Trigger, 0, len(f.triggers))
	for _, tr := range f.triggers {
		result = append(result, tr)
	}
	return result, nil
}

func (f *fakeTriggerNamespaceLister) Get(name string) (*eventingv1.Trigger, error) {
	if tr, ok := f.triggers[name]; ok {
		return tr, nil
	}
	return nil, apierrs.NewNotFound(schema.GroupResource{Group: "eventing.knative.dev", Resource: "triggers"}, name)
}
