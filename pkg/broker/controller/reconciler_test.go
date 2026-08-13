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
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	kubefake "k8s.io/client-go/kubernetes/fake"
	appsv1listers "k8s.io/client-go/listers/apps/v1"
	corev1listers "k8s.io/client-go/listers/core/v1"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"

	"go.uber.org/zap"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	addressableinjection "knative.dev/pkg/client/injection/ducks/duck/v1/addressable"
	"knative.dev/pkg/controller"
	dynamicclientfake "knative.dev/pkg/injection/clients/dynamicclient/fake"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/resolver"
	"knative.dev/pkg/tracker"

	eventingduckv1 "knative.dev/eventing/pkg/apis/duck/v1"
	eventingv1 "knative.dev/eventing/pkg/apis/eventing/v1"
	eventingfake "knative.dev/eventing/pkg/client/clientset/versioned/fake"
	eventinglisters "knative.dev/eventing/pkg/client/listers/eventing/v1"

	"knative.dev/eventing-natss/pkg/broker/autoscaler"
	brokerconfig "knative.dev/eventing-natss/pkg/broker/config"
	"knative.dev/eventing-natss/pkg/broker/contract"
	"knative.dev/eventing-natss/pkg/broker/controller/resources"
	brokeroidc "knative.dev/eventing-natss/pkg/broker/oidc"
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
	deployment := resources.MakeFilterDeployment(&resources.FilterArgs{
		Broker:             broker,
		Image:              "filter:latest",
		ServiceAccountName: (&Reconciler{}).dataplaneIdentity(broker),
		StreamName:         "TEST_STREAM",
		NatsURL:            "nats://localhost:4222",
	})
	deployment.Spec.Replicas = ptr.To(replicas)
	defaultFilterDeploymentAsAPIServer(deployment)
	return deployment
}

func testTrigger(ns, name, brokerName string) *eventingv1.Trigger {
	return &eventingv1.Trigger{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec:       eventingv1.TriggerSpec{Broker: brokerName},
	}
}

func triggerObjects(triggers ...*eventingv1.Trigger) []runtime.Object {
	objects := make([]runtime.Object, len(triggers))
	for index := range triggers {
		objects[index] = triggers[index]
	}
	return objects
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
		broker := testBroker(testNamespace, testBrokerName)
		deployment := ownedFilterDeployment(broker, 1)
		deployment.UID = "deployment-uid"
		deployment.ResourceVersion = "deployment-rv"
		service := resources.MakeFilterService(broker)
		service.UID = "service-uid"
		service.ResourceVersion = "service-rv"
		kube := kubefake.NewSimpleClientset(
			deployment,
			service,
		)
		r := &Reconciler{kubeClientSet: kube}
		ctx := logging.WithLogger(context.Background(), zap.NewNop().Sugar())

		if err := r.deleteFilter(ctx, broker); err != nil {
			t.Fatalf("deleteFilter() error: %v", err)
		}

		if _, err := kube.AppsV1().Deployments(testNamespace).Get(ctx, name, metav1.GetOptions{}); !apierrs.IsNotFound(err) {
			t.Errorf("deployment: expected NotFound, got %v", err)
		}
		if _, err := kube.CoreV1().Services(testNamespace).Get(ctx, name, metav1.GetOptions{}); !apierrs.IsNotFound(err) {
			t.Errorf("service: expected NotFound, got %v", err)
		}

		kube.ClearActions()
		if err := r.deleteFilter(ctx, broker); err != nil {
			t.Fatalf("second deleteFilter() error: %v", err)
		}
		if got := countResourceActions(kube.Actions(), "delete", "deployments"); got != 0 {
			t.Errorf("second-pass Deployment deletes = %d, want 0", got)
		}
		if got := countResourceActions(kube.Actions(), "delete", "services"); got != 0 {
			t.Errorf("second-pass Service deletes = %d, want 0", got)
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
		b := testBroker(testNamespace, testBrokerName)
		deployment := ownedFilterDeployment(b, 1)
		deployment.UID = "deployment-uid"
		deployment.ResourceVersion = "deployment-rv"
		kube := kubefake.NewSimpleClientset(deployment)
		kube.PrependReactor("delete", "deployments", func(clienttesting.Action) (bool, runtime.Object, error) {
			return true, nil, errors.New("boom")
		})
		r := &Reconciler{kubeClientSet: kube}
		ctx := logging.WithLogger(context.Background(), zap.NewNop().Sugar())

		if err := r.deleteFilter(ctx, b); err == nil {
			t.Fatal("deleteFilter() expected an error, got nil")
		}
		if cond := b.Status.GetCondition(eventingv1.BrokerConditionFilter); cond == nil || cond.IsTrue() {
			t.Errorf("expected BrokerConditionFilter to be marked failed, got %v", cond)
		}
	})
}

func TestDeleteFilterRejectsForeignResourcesBeforeMutation(t *testing.T) {
	for _, tc := range []struct {
		name            string
		foreignResource string
		wantErr         error
	}{
		{name: "foreign Deployment", foreignResource: "deployment", wantErr: errFilterDeploymentNotOwned},
		{name: "foreign Service", foreignResource: "service", wantErr: errFilterServiceNotOwned},
	} {
		t.Run(tc.name, func(t *testing.T) {
			broker := testBroker(testNamespace, testBrokerName)
			deployment := ownedFilterDeployment(broker, 1)
			deployment.UID = "deployment-uid"
			deployment.ResourceVersion = "deployment-rv"
			service := resources.MakeFilterService(broker)
			service.UID = "service-uid"
			service.ResourceVersion = "service-rv"
			foreign := broker.DeepCopy()
			foreign.UID = "foreign-broker-uid"
			switch tc.foreignResource {
			case "deployment":
				deployment.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(foreign, eventingv1.SchemeGroupVersion.WithKind("Broker"))}
			case "service":
				service.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(foreign, eventingv1.SchemeGroupVersion.WithKind("Broker"))}
			}
			originalDeployment := deployment.DeepCopy()
			originalService := service.DeepCopy()
			kube := kubefake.NewSimpleClientset(deployment, service)
			r := &Reconciler{kubeClientSet: kube}

			err := r.deleteFilter(testContext(), broker)
			if !errors.Is(err, tc.wantErr) {
				t.Errorf("deleteFilter() error = %v, want %v", err, tc.wantErr)
			}
			if got := countResourceActions(kube.Actions(), "delete", "deployments"); got != 0 {
				t.Errorf("Deployment delete actions = %d, want 0", got)
			}
			if got := countResourceActions(kube.Actions(), "delete", "services"); got != 0 {
				t.Errorf("Service delete actions = %d, want 0", got)
			}
			storedDeployment, err := kube.AppsV1().Deployments(broker.Namespace).Get(context.Background(), deployment.Name, metav1.GetOptions{})
			if err != nil {
				t.Errorf("Deployment was deleted despite foreign collision: %v", err)
			} else if !apiequality.Semantic.DeepEqual(storedDeployment, originalDeployment) {
				t.Errorf("Deployment changed: got=%#v want=%#v", storedDeployment, originalDeployment)
			}
			storedService, err := kube.CoreV1().Services(broker.Namespace).Get(context.Background(), service.Name, metav1.GetOptions{})
			if err != nil {
				t.Errorf("Service was deleted despite foreign collision: %v", err)
			} else if !apiequality.Semantic.DeepEqual(storedService, originalService) {
				t.Errorf("Service changed: got=%#v want=%#v", storedService, originalService)
			}
		})
	}
}

func TestDeleteFilterUsesUIDPreconditions(t *testing.T) {
	broker := testBroker(testNamespace, testBrokerName)
	deployment := ownedFilterDeployment(broker, 1)
	deployment.UID = "deployment-uid"
	deployment.ResourceVersion = "deployment-rv"
	service := resources.MakeFilterService(broker)
	service.UID = "service-uid"
	service.ResourceVersion = "service-rv"
	kube := kubefake.NewSimpleClientset(deployment, service)
	r := &Reconciler{kubeClientSet: kube}

	if err := r.deleteFilter(testContext(), broker); err != nil {
		t.Fatal(err)
	}
	assertDeletePreconditions(t, kube.Actions(), "deployments", deployment.UID, deployment.ResourceVersion, ptr.To(metav1.DeletePropagationForeground))
	assertDeletePreconditions(t, kube.Actions(), "services", service.UID, service.ResourceVersion, nil)
}

func TestDeleteFilterSkipsResourcesAlreadyDeleting(t *testing.T) {
	broker := testBroker(testNamespace, testBrokerName)
	deletionTime := metav1.Now()
	deployment := ownedFilterDeployment(broker, 1)
	deployment.UID = "deployment-uid"
	deployment.ResourceVersion = "deployment-rv"
	deployment.DeletionTimestamp = &deletionTime
	deployment.Finalizers = []string{"example.test/deployment-hold"}
	service := resources.MakeFilterService(broker)
	service.UID = "service-uid"
	service.ResourceVersion = "service-rv"
	service.DeletionTimestamp = &deletionTime
	service.Finalizers = []string{"example.test/service-hold"}
	kube := kubefake.NewSimpleClientset(deployment, service)
	r := &Reconciler{kubeClientSet: kube}

	err := r.deleteFilter(testContext(), broker)
	if requeue, after := controller.IsRequeueKey(err); !requeue || after != time.Second {
		t.Fatalf("deleteFilter() = %v (requeue=%v after=%s), want 1s requeue barrier", err, requeue, after)
	}
	if got := countResourceActions(kube.Actions(), "delete", "deployments"); got != 0 {
		t.Errorf("Deployment delete actions = %d, want 0", got)
	}
	if got := countResourceActions(kube.Actions(), "delete", "services"); got != 0 {
		t.Errorf("Service delete actions = %d, want 0", got)
	}
}

func TestDeleteFilterWaitsForScaledObjectFinalizerBeforeDeletingChildren(t *testing.T) {
	broker := autoscaledBroker()
	object := expectedScaledObject(t, broker)
	object.SetUID("scaledobject-uid")
	object.SetResourceVersion("scaledobject-rv")
	deletionTime := metav1.Now()
	object.SetDeletionTimestamp(&deletionTime)
	object.SetFinalizers([]string{metav1.FinalizerDeleteDependents, "finalizer.keda.sh"})
	deployment := ownedFilterDeployment(broker, 1)
	deployment.UID = "deployment-uid"
	deployment.ResourceVersion = "deployment-rv"
	service := resources.MakeFilterService(broker)
	service.UID = "service-uid"
	service.ResourceVersion = "service-rv"
	kube := kubefake.NewSimpleClientset(deployment, service)
	dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), object)
	r := &Reconciler{kubeClientSet: kube, dynamicClient: dynamicClient}

	err := r.deleteFilter(testContext(), broker)
	if requeue, after := controller.IsRequeueKey(err); !requeue || after != time.Second {
		t.Fatalf("deleteFilter() = %v (requeue=%v after=%s), want 1s ScaledObject barrier", err, requeue, after)
	}
	if got := countResourceActions(kube.Actions(), "delete", "deployments"); got != 0 {
		t.Errorf("Deployment deletes while ScaledObject finalizer remains = %d, want 0", got)
	}
	if got := countResourceActions(kube.Actions(), "delete", "services"); got != 0 {
		t.Errorf("Service deletes while ScaledObject finalizer remains = %d, want 0", got)
	}
	if got := countResourceActions(dynamicClient.Actions(), "delete", "scaledobjects"); got != 0 {
		t.Errorf("already-deleting ScaledObject delete actions = %d, want 0", got)
	}
}

func TestDeleteFilterUIDPreconditionProtectsReplacement(t *testing.T) {
	broker := testBroker(testNamespace, testBrokerName)
	deployment := ownedFilterDeployment(broker, 1)
	deployment.UID = "deployment-uid"
	deployment.ResourceVersion = "deployment-rv"
	service := resources.MakeFilterService(broker)
	service.UID = "service-uid"
	service.ResourceVersion = "service-rv"
	foreign := broker.DeepCopy()
	foreign.UID = "foreign-broker-uid"
	replacement := deployment.DeepCopy()
	replacement.UID = "replacement-deployment-uid"
	replacement.ResourceVersion = "replacement-deployment-rv"
	replacement.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(foreign, eventingv1.SchemeGroupVersion.WithKind("Broker"))}
	kube := kubefake.NewSimpleClientset(deployment, service)
	kube.PrependReactor("delete", "deployments", func(action clienttesting.Action) (bool, runtime.Object, error) {
		if err := kube.Tracker().Update(appsv1.SchemeGroupVersion.WithResource("deployments"), replacement, replacement.Namespace); err != nil {
			t.Fatalf("replace Deployment in tracker: %v", err)
		}
		options := action.(clienttesting.DeleteAction).GetDeleteOptions()
		if options.Preconditions != nil && options.Preconditions.UID != nil && *options.Preconditions.UID == deployment.UID {
			return true, nil, apierrs.NewConflict(appsv1.Resource("deployments"), deployment.Name, errors.New("UID precondition failed"))
		}
		return false, nil, nil
	})
	r := &Reconciler{kubeClientSet: kube}

	err := r.deleteFilter(testContext(), broker)
	if err == nil || !errorChainHasConflict(err) {
		t.Errorf("deleteFilter() error = %v, want wrapped UID precondition conflict", err)
	}
	stored, getErr := kube.AppsV1().Deployments(broker.Namespace).Get(context.Background(), replacement.Name, metav1.GetOptions{})
	if getErr != nil {
		t.Fatalf("replacement Deployment was deleted: %v", getErr)
	}
	if stored.UID != replacement.UID || metav1.IsControlledBy(stored, broker) {
		t.Errorf("stored Deployment = UID %q owners %v, want preserved foreign replacement", stored.UID, stored.OwnerReferences)
	}
	if _, getErr := kube.CoreV1().Services(broker.Namespace).Get(context.Background(), service.Name, metav1.GetOptions{}); getErr != nil {
		t.Errorf("Service was partially deleted after Deployment precondition failure: %v", getErr)
	}
}

func TestDeleteFilterServiceUIDPreconditionProtectsReplacement(t *testing.T) {
	broker := testBroker(testNamespace, testBrokerName)
	deployment := ownedFilterDeployment(broker, 1)
	deployment.UID = "deployment-uid"
	deployment.ResourceVersion = "deployment-rv"
	service := resources.MakeFilterService(broker)
	service.UID = "service-uid"
	service.ResourceVersion = "service-rv"
	foreign := broker.DeepCopy()
	foreign.UID = "foreign-broker-uid"
	replacement := service.DeepCopy()
	replacement.UID = "replacement-service-uid"
	replacement.ResourceVersion = "replacement-service-rv"
	replacement.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(foreign, eventingv1.SchemeGroupVersion.WithKind("Broker"))}
	kube := kubefake.NewSimpleClientset(deployment, service)
	kube.PrependReactor("delete", "services", func(action clienttesting.Action) (bool, runtime.Object, error) {
		if err := kube.Tracker().Update(corev1.SchemeGroupVersion.WithResource("services"), replacement, replacement.Namespace); err != nil {
			t.Fatalf("replace Service in tracker: %v", err)
		}
		options := action.(clienttesting.DeleteAction).GetDeleteOptions()
		if options.Preconditions != nil && options.Preconditions.UID != nil && *options.Preconditions.UID == service.UID {
			return true, nil, apierrs.NewConflict(corev1.Resource("services"), service.Name, errors.New("UID precondition failed"))
		}
		return false, nil, nil
	})
	r := &Reconciler{kubeClientSet: kube}

	err := r.deleteFilter(testContext(), broker)
	if err == nil || !errorChainHasConflict(err) {
		t.Errorf("deleteFilter() error = %v, want wrapped UID precondition conflict", err)
	}
	stored, getErr := kube.CoreV1().Services(broker.Namespace).Get(context.Background(), replacement.Name, metav1.GetOptions{})
	if getErr != nil {
		t.Fatalf("replacement Service was deleted: %v", getErr)
	}
	if stored.UID != replacement.UID || metav1.IsControlledBy(stored, broker) {
		t.Errorf("stored Service = UID %q owners %v, want preserved foreign replacement", stored.UID, stored.OwnerReferences)
	}
}

func TestDeleteFilterResourceVersionPreconditionProtectsSameUIDOwnerChange(t *testing.T) {
	for _, resourceKind := range []string{"deployment", "service"} {
		t.Run(resourceKind, func(t *testing.T) {
			broker := testBroker(testNamespace, testBrokerName)
			foreign := broker.DeepCopy()
			foreign.UID = "foreign-broker-uid"
			kube := kubefake.NewSimpleClientset()
			var name string
			switch resourceKind {
			case "deployment":
				existing := ownedFilterDeployment(broker, 1)
				existing.UID = "stable-uid"
				existing.ResourceVersion = "old-rv"
				replacement := existing.DeepCopy()
				replacement.ResourceVersion = "new-rv"
				replacement.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(foreign, eventingv1.SchemeGroupVersion.WithKind("Broker"))}
				name = existing.Name
				if err := kube.Tracker().Add(existing); err != nil {
					t.Fatal(err)
				}
				kube.PrependReactor("delete", "deployments", func(action clienttesting.Action) (bool, runtime.Object, error) {
					if err := kube.Tracker().Update(appsv1.SchemeGroupVersion.WithResource("deployments"), replacement, replacement.Namespace); err != nil {
						t.Fatalf("replace Deployment in tracker: %v", err)
					}
					assertDeleteOptionsValues(t, action.(clienttesting.DeleteAction).GetDeleteOptions(), existing.UID, existing.ResourceVersion, ptr.To(metav1.DeletePropagationForeground))
					return true, nil, apierrs.NewConflict(appsv1.Resource("deployments"), existing.Name, errors.New("resourceVersion precondition failed"))
				})
			case "service":
				existing := resources.MakeFilterService(broker)
				existing.UID = "stable-uid"
				existing.ResourceVersion = "old-rv"
				replacement := existing.DeepCopy()
				replacement.ResourceVersion = "new-rv"
				replacement.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(foreign, eventingv1.SchemeGroupVersion.WithKind("Broker"))}
				name = existing.Name
				if err := kube.Tracker().Add(existing); err != nil {
					t.Fatal(err)
				}
				kube.PrependReactor("delete", "services", func(action clienttesting.Action) (bool, runtime.Object, error) {
					if err := kube.Tracker().Update(corev1.SchemeGroupVersion.WithResource("services"), replacement, replacement.Namespace); err != nil {
						t.Fatalf("replace Service in tracker: %v", err)
					}
					assertDeleteOptionsValues(t, action.(clienttesting.DeleteAction).GetDeleteOptions(), existing.UID, existing.ResourceVersion, nil)
					return true, nil, apierrs.NewConflict(corev1.Resource("services"), existing.Name, errors.New("resourceVersion precondition failed"))
				})
			}
			r := &Reconciler{kubeClientSet: kube}

			err := r.deleteFilter(testContext(), broker)
			if err == nil || !errorChainHasConflict(err) {
				t.Fatalf("deleteFilter() error = %v, want wrapped resourceVersion conflict", err)
			}
			switch resourceKind {
			case "deployment":
				stored, getErr := kube.AppsV1().Deployments(broker.Namespace).Get(context.Background(), name, metav1.GetOptions{})
				if getErr != nil || stored.UID != "stable-uid" || stored.ResourceVersion != "new-rv" || metav1.IsControlledBy(stored, broker) {
					t.Fatalf("same-UID replacement Deployment not preserved: object=%#v error=%v", stored, getErr)
				}
			case "service":
				stored, getErr := kube.CoreV1().Services(broker.Namespace).Get(context.Background(), name, metav1.GetOptions{})
				if getErr != nil || stored.UID != "stable-uid" || stored.ResourceVersion != "new-rv" || metav1.IsControlledBy(stored, broker) {
					t.Fatalf("same-UID replacement Service not preserved: object=%#v error=%v", stored, getErr)
				}
			}
		})
	}
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
	b := testBroker(testNamespace, testBrokerName)
	// Keep a valid Broker-owned identity while drifting the controller-owned
	// port list so the update branch runs.
	existing := resources.MakeFilterService(b)
	existing.Spec.Ports = nil
	kube := kubefake.NewSimpleClientset(existing)
	r := &Reconciler{kubeClientSet: kube, serviceLister: newServiceLister(existing)}

	if _, err := r.reconcileFilterService(testContext(), b); err != nil {
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

func TestMergeFilterDeploymentRepairsOIDCIdentityAndPreservesAdmissionExtras(t *testing.T) {
	b := testBroker(testNamespace, testBrokerName)
	args := &resources.FilterArgs{
		Broker: b, Image: "filter:latest", ServiceAccountName: "dp-sa",
		StreamName: "TEST_STREAM", NatsURL: "nats://localhost:4222",
		OIDCServiceAccountUID: "delivery-sa-uid",
	}

	expected := resources.MakeFilterDeployment(args)
	existing := expected.DeepCopy()
	filterIndex, err := namedContainerIndex(existing.Spec.Template.Spec.Containers, resources.FilterContainerName)
	if err != nil || filterIndex < 0 {
		t.Fatalf("find filter container: index=%d err=%v", filterIndex, err)
	}
	for i := range existing.Spec.Template.Spec.Containers[filterIndex].Env {
		variable := &existing.Spec.Template.Spec.Containers[filterIndex].Env[i]
		if variable.Name == "OIDC_SERVICE_ACCOUNT" || variable.Name == "OIDC_SERVICE_ACCOUNT_UID" {
			variable.Value = "drifted"
		}
	}

	admissionVolume := corev1.Volume{
		Name: "admission-extra",
		VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
			LocalObjectReference: corev1.LocalObjectReference{Name: "injected-config"},
		}},
	}
	existing.Spec.Template.Spec.Volumes = append(existing.Spec.Template.Spec.Volumes, admissionVolume)
	admissionMount := corev1.VolumeMount{Name: admissionVolume.Name, MountPath: "/var/run/admission", ReadOnly: true}
	existing.Spec.Template.Spec.Containers[filterIndex].VolumeMounts = append(
		existing.Spec.Template.Spec.Containers[filterIndex].VolumeMounts,
		admissionMount,
	)

	merged, err := mergeFilterDeployment(existing, expected)
	if err != nil {
		t.Fatal(err)
	}
	if !containsVolume(merged.Spec.Template.Spec.Volumes, admissionVolume) {
		t.Errorf("admission-added volume was not preserved: %#v", merged.Spec.Template.Spec.Volumes)
	}
	mergedFilter, err := namedContainer(merged.Spec.Template.Spec.Containers, resources.FilterContainerName)
	if err != nil {
		t.Fatal(err)
	}
	if !containsVolumeMount(mergedFilter.VolumeMounts, admissionMount) {
		t.Errorf("admission-added mount was not preserved: %#v", mergedFilter.VolumeMounts)
	}
	env := make(map[string]string)
	for _, variable := range mergedFilter.Env {
		env[variable.Name] = variable.Value
	}
	if got := env["OIDC_SERVICE_ACCOUNT"]; got != brokeroidc.DeliveryServiceAccountName {
		t.Errorf("OIDC_SERVICE_ACCOUNT = %q, want %q", got, brokeroidc.DeliveryServiceAccountName)
	}
	if got := env["OIDC_SERVICE_ACCOUNT_UID"]; got != args.OIDCServiceAccountUID {
		t.Errorf("OIDC_SERVICE_ACCOUNT_UID = %q, want %q", got, args.OIDCServiceAccountUID)
	}

	steady, err := mergeFilterDeployment(merged, expected)
	if err != nil {
		t.Fatal(err)
	}
	if !apiequality.Semantic.DeepEqual(steady, merged) {
		t.Error("second OIDC identity merge is not a steady-state no-op")
	}
}

func containsVolume(volumes []corev1.Volume, want corev1.Volume) bool {
	for _, volume := range volumes {
		if apiequality.Semantic.DeepEqual(volume, want) {
			return true
		}
	}
	return false
}

func containsVolumeMount(mounts []corev1.VolumeMount, want corev1.VolumeMount) bool {
	for _, mount := range mounts {
		if apiequality.Semantic.DeepEqual(mount, want) {
			return true
		}
	}
	return false
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
	existing.Labels = copyStringMap(existing.Labels)
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

func TestReconcileAutoscaledFilterDeploymentAPIDefaultsAreNoOp(t *testing.T) {
	b := testBroker(testNamespace, testBrokerName)
	r := &Reconciler{
		filterImage:    "filter:v1",
		natsURL:        "nats://localhost:4222",
		natsConfigJSON: `{"url":"nats://localhost:4222"}`,
	}
	existing := resources.MakeFilterDeployment(&resources.FilterArgs{
		Broker:             b,
		Image:              r.filterImage,
		ServiceAccountName: r.dataplaneIdentity(b),
		StreamName:         "TEST_STREAM",
		NatsURL:            r.natsURL,
		NatsConfigJSON:     r.natsConfigJSON,
		Template:           brokerconfig.DefaultBrokerConfig().Filter,
	})
	existing.Spec.Replicas = ptr.To[int32](0)
	defaultFilterDeploymentAsAPIServer(existing)
	kube := kubefake.NewSimpleClientset(existing)
	r.kubeClientSet = kube
	r.deploymentLister = newDeploymentLister(existing)

	if err := r.reconcileFilterDeployment(testContext(), b, "TEST_STREAM", brokerconfig.DefaultBrokerConfig(), autoscaledReplicaPolicy(autoscaler.Settings{MinScale: 0})); err != nil {
		t.Fatalf("reconcileFilterDeployment() error: %v", err)
	}
	if got := countResourceActions(kube.Actions(), "update", "deployments"); got != 0 {
		t.Fatalf("Deployment updates = %d, want 0 for API-defaulted steady state; actions=%#v", got, kube.Actions())
	}
	stored, err := kube.AppsV1().Deployments(b.Namespace).Get(context.Background(), existing.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if stored.Spec.Replicas == nil || *stored.Spec.Replicas != 0 {
		t.Fatalf("autoscaled replicas = %v, want preserved zero", stored.Spec.Replicas)
	}
	assertFilterAPIDefaultFixture(t, stored)
}

func TestReconcileFilterServiceAPIDefaultsAreNoOp(t *testing.T) {
	b := testBroker(testNamespace, testBrokerName)
	existing := resources.MakeFilterService(b)
	defaultFilterServiceAsAPIServer(existing)
	kube := kubefake.NewSimpleClientset(existing)
	r := &Reconciler{kubeClientSet: kube, serviceLister: newServiceLister(existing)}

	if _, err := r.reconcileFilterService(testContext(), b); err != nil {
		t.Fatalf("reconcileFilterService() error: %v", err)
	}
	if got := countResourceActions(kube.Actions(), "update", "services"); got != 0 {
		t.Fatalf("Service updates = %d, want 0 for API-defaulted steady state; actions=%#v", got, kube.Actions())
	}
}

func TestReconcileFilterServiceRejectsForeignOwnerWithoutMutation(t *testing.T) {
	b := testBroker(testNamespace, testBrokerName)
	foreign := b.DeepCopy()
	foreign.UID = "foreign-broker-uid"
	existing := resources.MakeFilterService(b)
	existing.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(foreign, eventingv1.SchemeGroupVersion.WithKind("Broker"))}
	original := existing.DeepCopy()
	kube := kubefake.NewSimpleClientset(existing)
	r := &Reconciler{kubeClientSet: kube, serviceLister: newServiceLister(existing)}

	if _, err := r.reconcileFilterService(testContext(), b); !errors.Is(err, errFilterServiceNotOwned) {
		t.Fatalf("reconcileFilterService() error = %v, want errFilterServiceNotOwned", err)
	}
	if got := countResourceActions(kube.Actions(), "update", "services"); got != 0 {
		t.Fatalf("foreign Service updates = %d, want 0", got)
	}
	stored, err := kube.CoreV1().Services(b.Namespace).Get(context.Background(), existing.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !apiequality.Semantic.DeepEqual(stored, original) {
		t.Fatalf("foreign Service was mutated: got=%#v want=%#v", stored, original)
	}
}

func TestReconcileFilterDeploymentSelectorMismatchFailsClosed(t *testing.T) {
	b := testBroker(testNamespace, testBrokerName)
	existing := ownedFilterDeployment(b, 0)
	existing.Spec.Selector = existing.Spec.Selector.DeepCopy()
	existing.Spec.Selector.MatchLabels = copyStringMap(existing.Spec.Selector.MatchLabels)
	existing.Spec.Selector.MatchLabels[resources.RoleLabelKey] = "foreign-selector"
	original := existing.DeepCopy()
	kube := kubefake.NewSimpleClientset(existing)
	r := &Reconciler{
		kubeClientSet:    kube,
		deploymentLister: newDeploymentLister(existing),
		filterImage:      "filter:latest",
		natsURL:          "nats://localhost:4222",
	}

	err := r.reconcileFilterDeployment(testContext(), b, "TEST_STREAM", brokerconfig.DefaultBrokerConfig(), autoscaledReplicaPolicy(autoscaler.Settings{MinScale: 0}))
	if err == nil || !strings.Contains(err.Error(), "selector is immutable") {
		t.Fatalf("reconcileFilterDeployment() error = %v, want immutable selector rejection", err)
	}
	if got := countResourceActions(kube.Actions(), "update", "deployments"); got != 0 {
		t.Fatalf("selector-mismatched Deployment updates = %d, want 0", got)
	}
	stored, err := kube.AppsV1().Deployments(b.Namespace).Get(context.Background(), existing.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !apiequality.Semantic.DeepEqual(stored, original) {
		t.Fatalf("selector-mismatched Deployment was mutated: got=%#v want=%#v", stored, original)
	}
}

func TestReconcileAutoscaledFilterDeploymentOwnedDriftConvergesOnce(t *testing.T) {
	b := testBroker(testNamespace, testBrokerName)
	brokerConfig := brokerconfig.DefaultBrokerConfig()
	brokerConfig.Filter = &brokerconfig.DeploymentTemplate{
		Annotations: map[string]string{"controller.example/config": "desired"},
	}
	r := &Reconciler{
		filterImage:    "filter:v2",
		natsURL:        "nats://localhost:4222",
		natsConfigJSON: `{"url":"nats://localhost:4222"}`,
	}
	existing := resources.MakeFilterDeployment(&resources.FilterArgs{
		Broker:             b,
		Image:              r.filterImage,
		ServiceAccountName: r.dataplaneIdentity(b),
		StreamName:         "TEST_STREAM",
		NatsURL:            r.natsURL,
		NatsConfigJSON:     r.natsConfigJSON,
		Template:           brokerConfig.Filter,
	})
	existing.Spec.Replicas = ptr.To[int32](0)
	existing.Labels = copyStringMap(existing.Labels)
	existing.Labels["admission.example/extra"] = "preserved"
	existing.Annotations = copyStringMap(existing.Annotations)
	existing.Annotations["controller.example/config"] = "stale"
	existing.Annotations["admission.example/deployment"] = "preserved"
	existing.Spec.Template.Annotations = map[string]string{"sidecar.example/injected": "true"}
	existing.Spec.Template.Spec.Tolerations = []corev1.Toleration{{Key: "dedicated", Operator: corev1.TolerationOpEqual, Value: "events"}}
	filter := &existing.Spec.Template.Spec.Containers[0]
	filter.Env = append(filter.Env, corev1.EnvVar{Name: "ADMISSION_EXTRA", Value: "preserved"})
	filter.Ports = append(filter.Ports, corev1.ContainerPort{Name: "admin-extra", ContainerPort: 9090, Protocol: corev1.ProtocolTCP})
	existing.Spec.Template.Spec.Containers = append(existing.Spec.Template.Spec.Containers, corev1.Container{Name: "injected-sidecar", Image: "sidecar:v1"})
	defaultFilterDeploymentAsAPIServer(existing)

	// Drift only controller-owned fields after API/admission defaults exist.
	filter = &existing.Spec.Template.Spec.Containers[0]
	filter.Image = "filter:stale"
	for index := range filter.Env {
		if filter.Env[index].Name == "NATS_URL" {
			filter.Env[index].Value = "nats://stale:4222"
		}
	}
	kube := kubefake.NewSimpleClientset(existing)
	r.kubeClientSet = kube
	r.deploymentLister = newDeploymentLister(existing)
	policy := autoscaledReplicaPolicy(autoscaler.Settings{MinScale: 0})

	if err := r.reconcileFilterDeployment(testContext(), b, "TEST_STREAM", brokerConfig, policy); err != nil {
		t.Fatal(err)
	}
	if got := countResourceActions(kube.Actions(), "update", "deployments"); got != 1 {
		t.Fatalf("first reconcile Deployment updates = %d, want 1", got)
	}
	updated, err := kube.AppsV1().Deployments(b.Namespace).Get(context.Background(), existing.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	assertFilterDeploymentUnownedFieldsPreserved(t, existing, updated)
	if updated.Spec.Replicas == nil || *updated.Spec.Replicas != 0 {
		t.Fatalf("autoscaled replicas = %v, want preserved zero", updated.Spec.Replicas)
	}
	updatedFilter := containerByName(t, updated.Spec.Template.Spec.Containers, resources.FilterContainerName)
	if updatedFilter.Image != r.filterImage {
		t.Errorf("filter image = %q, want reconciled %q", updatedFilter.Image, r.filterImage)
	}
	if got := envValue(updatedFilter.Env, "NATS_URL"); got != r.natsURL {
		t.Errorf("NATS_URL = %q, want reconciled %q", got, r.natsURL)
	}
	if got := updated.Annotations["controller.example/config"]; got != "desired" {
		t.Errorf("controller-owned Deployment annotation = %q, want desired", got)
	}
	if got := updated.Annotations["admission.example/deployment"]; got != "preserved" {
		t.Errorf("extra Deployment annotation = %q, want preserved", got)
	}

	kube.ClearActions()
	r.deploymentLister = newDeploymentLister(updated)
	if err := r.reconcileFilterDeployment(testContext(), b, "TEST_STREAM", brokerConfig, policy); err != nil {
		t.Fatal(err)
	}
	if got := countResourceActions(kube.Actions(), "update", "deployments"); got != 0 {
		t.Fatalf("second reconcile Deployment updates = %d, want 0; actions=%#v", got, kube.Actions())
	}
}

func TestReconcileFilterDeploymentImagePullPolicyConvergesOnce(t *testing.T) {
	for _, tc := range []struct {
		name         string
		desiredImage string
		storedImage  string
		storedPolicy corev1.PullPolicy
		wantPolicy   corev1.PullPolicy
	}{
		{
			name:         "latest to versioned",
			desiredImage: "registry.example/filter:v2",
			storedImage:  "registry.example/filter:latest",
			storedPolicy: corev1.PullAlways,
			wantPolicy:   corev1.PullIfNotPresent,
		},
		{
			name:         "versioned to latest",
			desiredImage: "registry.example/filter:latest",
			storedImage:  "registry.example/filter:v1",
			storedPolicy: corev1.PullIfNotPresent,
			wantPolicy:   corev1.PullAlways,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := testBroker(testNamespace, testBrokerName)
			r := &Reconciler{filterImage: tc.desiredImage, natsURL: "nats://localhost:4222"}
			existing := filterDeploymentFixture(b, r, brokerconfig.DefaultBrokerConfig(), 0)
			filter := containerByName(t, existing.Spec.Template.Spec.Containers, resources.FilterContainerName)
			filter.Image = tc.storedImage
			filter.ImagePullPolicy = tc.storedPolicy
			kube := kubefake.NewSimpleClientset(existing)
			r.kubeClientSet = kube
			r.deploymentLister = newDeploymentLister(existing)
			policy := autoscaledReplicaPolicy(autoscaler.Settings{MinScale: 0})

			if err := r.reconcileFilterDeployment(testContext(), b, "TEST_STREAM", brokerconfig.DefaultBrokerConfig(), policy); err != nil {
				t.Fatal(err)
			}
			if got := countResourceActions(kube.Actions(), "update", "deployments"); got != 1 {
				t.Fatalf("first reconcile Deployment updates = %d, want 1", got)
			}
			updated, err := kube.AppsV1().Deployments(b.Namespace).Get(context.Background(), existing.Name, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			updatedFilter := containerByName(t, updated.Spec.Template.Spec.Containers, resources.FilterContainerName)
			if updatedFilter.Image != tc.desiredImage || updatedFilter.ImagePullPolicy != tc.wantPolicy {
				t.Errorf("image/policy = %q/%q, want %q/%q", updatedFilter.Image, updatedFilter.ImagePullPolicy, tc.desiredImage, tc.wantPolicy)
			}

			kube.ClearActions()
			r.deploymentLister = newDeploymentLister(updated)
			if err := r.reconcileFilterDeployment(testContext(), b, "TEST_STREAM", brokerconfig.DefaultBrokerConfig(), policy); err != nil {
				t.Fatal(err)
			}
			if got := countResourceActions(kube.Actions(), "update", "deployments"); got != 0 {
				t.Fatalf("second reconcile Deployment updates = %d, want 0; actions=%#v", got, kube.Actions())
			}
		})
	}
}

func TestReconcileFilterDeploymentAPIDefaultedEnvAndResourcesAreNoOp(t *testing.T) {
	b := testBroker(testNamespace, testBrokerName)
	brokerConfig := brokerconfig.DefaultBrokerConfig()
	brokerConfig.Filter = &brokerconfig.DeploymentTemplate{
		Env: []corev1.EnvVar{{
			Name: "FILE_VALUE",
			ValueFrom: &corev1.EnvVarSource{FileKeyRef: &corev1.FileKeySelector{
				VolumeName: "filter-config",
				Path:       "env",
				Key:        "value",
			}},
		}},
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("0.1m")},
			Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("0.1m")},
		},
	}
	r := &Reconciler{filterImage: "filter:v1", natsURL: "nats://localhost:4222"}
	existing := filterDeploymentFixture(b, r, brokerConfig, 0)
	if got := brokerConfig.Filter.Env[0].ValueFrom.FileKeyRef.Optional; got != nil {
		t.Fatalf("fixture defaulting mutated configured FileKeyRef optional to %v", *got)
	}
	if got, want := brokerConfig.Filter.Resources.Requests.Cpu(), resource.MustParse("0.1m"); got.Cmp(want) != 0 {
		t.Fatalf("fixture defaulting mutated configured CPU request to %s, want %s", got.String(), want.String())
	}
	kube := kubefake.NewSimpleClientset(existing)
	r.kubeClientSet = kube
	r.deploymentLister = newDeploymentLister(existing)

	if err := r.reconcileFilterDeployment(testContext(), b, "TEST_STREAM", brokerConfig, autoscaledReplicaPolicy(autoscaler.Settings{MinScale: 0})); err != nil {
		t.Fatal(err)
	}
	if got := countResourceActions(kube.Actions(), "update", "deployments"); got != 0 {
		t.Fatalf("Deployment updates = %d, want 0 for API-defaulted FileKeyRef/resources; actions=%#v", got, kube.Actions())
	}
	filter := containerByName(t, existing.Spec.Template.Spec.Containers, resources.FilterContainerName)
	fileValue := envVarByName(t, filter.Env, "FILE_VALUE")
	if fileValue.ValueFrom == nil || fileValue.ValueFrom.FileKeyRef == nil || fileValue.ValueFrom.FileKeyRef.Optional == nil || *fileValue.ValueFrom.FileKeyRef.Optional {
		t.Fatalf("FILE_VALUE FileKeyRef optional = %#v, want explicit false API default", fileValue.ValueFrom)
	}
	if got := filter.Resources.Requests.Cpu().String(); got != "1m" {
		t.Errorf("defaulted CPU request = %q, want 1m", got)
	}
	if got := filter.Resources.Limits.Memory().String(); got != "1m" {
		t.Errorf("defaulted memory limit = %q, want 1m", got)
	}
}

func TestReconcileFilterDeploymentRepairsDefaultResourcesAndSecurityOnce(t *testing.T) {
	b := testBroker(testNamespace, testBrokerName)
	r := &Reconciler{filterImage: "filter:v1", natsURL: "nats://localhost:4222"}
	existing := filterDeploymentFixture(b, r, brokerconfig.DefaultBrokerConfig(), 0)
	existing.Spec.Template.Spec.Tolerations = []corev1.Toleration{{
		Key: "admission.example/dedicated", Operator: corev1.TolerationOpExists,
	}}
	existing.Spec.Template.Spec.Containers = append(existing.Spec.Template.Spec.Containers, corev1.Container{
		Name: "admission-sidecar", Image: "sidecar:v1", ImagePullPolicy: corev1.PullIfNotPresent,
	})
	filter := containerByName(t, existing.Spec.Template.Spec.Containers, resources.FilterContainerName)
	filter.Resources = corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("10m")},
		Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("32Mi")},
	}
	filter.SecurityContext = &corev1.SecurityContext{
		RunAsUser:                ptr.To[int64](10001),
		RunAsGroup:               ptr.To[int64](10002),
		RunAsNonRoot:             ptr.To(false),
		AllowPrivilegeEscalation: ptr.To(true),
		ReadOnlyRootFilesystem:   ptr.To(false),
		Capabilities: &corev1.Capabilities{
			Add: []corev1.Capability{"NET_ADMIN"},
		},
		SeccompProfile: &corev1.SeccompProfile{
			Type:             corev1.SeccompProfileTypeLocalhost,
			LocalhostProfile: ptr.To("profiles/unsafe.json"),
		},
	}
	originalTolerations := append([]corev1.Toleration(nil), existing.Spec.Template.Spec.Tolerations...)
	kube := kubefake.NewSimpleClientset(existing)
	r.kubeClientSet = kube
	r.deploymentLister = newDeploymentLister(existing)
	policy := autoscaledReplicaPolicy(autoscaler.Settings{MinScale: 0})

	if err := r.reconcileFilterDeployment(testContext(), b, "TEST_STREAM", brokerconfig.DefaultBrokerConfig(), policy); err != nil {
		t.Fatal(err)
	}
	if got := countResourceActions(kube.Actions(), "update", "deployments"); got != 1 {
		t.Fatalf("first reconcile Deployment updates = %d, want 1", got)
	}
	updated, err := kube.AppsV1().Deployments(b.Namespace).Get(context.Background(), existing.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	updatedFilter := containerByName(t, updated.Spec.Template.Spec.Containers, resources.FilterContainerName)
	assertDefaultFilterContainerResources(t, updatedFilter.Resources)
	assertHardenedFilterSecurityContext(t, updatedFilter.SecurityContext)
	if updatedFilter.SecurityContext.RunAsUser == nil || *updatedFilter.SecurityContext.RunAsUser != 10001 ||
		updatedFilter.SecurityContext.RunAsGroup == nil || *updatedFilter.SecurityContext.RunAsGroup != 10002 {
		t.Errorf("admission-owned runAs identity changed: %#v", updatedFilter.SecurityContext)
	}
	if !apiequality.Semantic.DeepEqual(updated.Spec.Template.Spec.Tolerations, originalTolerations) {
		t.Errorf("Pod admission defaults changed: got=%v want=%v", updated.Spec.Template.Spec.Tolerations, originalTolerations)
	}
	if got := containerByName(t, updated.Spec.Template.Spec.Containers, "admission-sidecar"); got.Image != "sidecar:v1" {
		t.Errorf("sidecar changed: %#v", got)
	}
	if updated.Spec.Replicas == nil || *updated.Spec.Replicas != 0 {
		t.Errorf("autoscaled replicas = %v, want preserved zero", updated.Spec.Replicas)
	}

	kube.ClearActions()
	r.deploymentLister = newDeploymentLister(updated)
	if err := r.reconcileFilterDeployment(testContext(), b, "TEST_STREAM", brokerconfig.DefaultBrokerConfig(), policy); err != nil {
		t.Fatal(err)
	}
	if got := countResourceActions(kube.Actions(), "update", "deployments"); got != 0 {
		t.Fatalf("second reconcile Deployment updates = %d, want 0; actions=%#v", got, kube.Actions())
	}
}

func TestReconcileFilterDeploymentPreservesUnknownEnvPositions(t *testing.T) {
	for _, tc := range []struct {
		name        string
		insertIndex func([]corev1.EnvVar) int
	}{
		{name: "beginning", insertIndex: func([]corev1.EnvVar) int { return 0 }},
		{name: "middle", insertIndex: func(env []corev1.EnvVar) int { return len(env) / 2 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := testBroker(testNamespace, testBrokerName)
			r := &Reconciler{filterImage: "filter:v1", natsURL: "nats://old:4222"}
			existing := filterDeploymentFixture(b, r, brokerconfig.DefaultBrokerConfig(), 0)
			filter := containerByName(t, existing.Spec.Template.Spec.Containers, resources.FilterContainerName)
			insertAt := tc.insertIndex(filter.Env)
			filter.Env = insertEnvVar(filter.Env, insertAt, corev1.EnvVar{Name: "WEBHOOK_EXTRA", Value: "preserved"})
			kube := kubefake.NewSimpleClientset(existing)
			r.kubeClientSet = kube
			r.deploymentLister = newDeploymentLister(existing)
			policy := autoscaledReplicaPolicy(autoscaler.Settings{MinScale: 0})

			if err := r.reconcileFilterDeployment(testContext(), b, "TEST_STREAM", brokerconfig.DefaultBrokerConfig(), policy); err != nil {
				t.Fatal(err)
			}
			if got := countResourceActions(kube.Actions(), "update", "deployments"); got != 0 {
				t.Fatalf("steady-state Deployment updates = %d, want 0; actions=%#v", got, kube.Actions())
			}

			r.natsURL = "nats://new:4222"
			kube.ClearActions()
			if err := r.reconcileFilterDeployment(testContext(), b, "TEST_STREAM", brokerconfig.DefaultBrokerConfig(), policy); err != nil {
				t.Fatal(err)
			}
			if got := countResourceActions(kube.Actions(), "update", "deployments"); got != 1 {
				t.Fatalf("owned-config reconcile Deployment updates = %d, want 1", got)
			}
			updated, err := kube.AppsV1().Deployments(b.Namespace).Get(context.Background(), existing.Name, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			updatedEnv := containerByName(t, updated.Spec.Template.Spec.Containers, resources.FilterContainerName).Env
			if got := envVarIndex(updatedEnv, "WEBHOOK_EXTRA"); got != insertAt {
				t.Errorf("unknown env index = %d, want preserved %d; env=%v", got, insertAt, updatedEnv)
			}
			if got := envValue(updatedEnv, "NATS_URL"); got != r.natsURL {
				t.Errorf("NATS_URL = %q, want %q", got, r.natsURL)
			}

			kube.ClearActions()
			r.deploymentLister = newDeploymentLister(updated)
			if err := r.reconcileFilterDeployment(testContext(), b, "TEST_STREAM", brokerconfig.DefaultBrokerConfig(), policy); err != nil {
				t.Fatal(err)
			}
			if got := countResourceActions(kube.Actions(), "update", "deployments"); got != 0 {
				t.Fatalf("post-update Deployment updates = %d, want 0; actions=%#v", got, kube.Actions())
			}
		})
	}
}

func TestReconcileFilterDeploymentRepairsHTTPSProbeOnce(t *testing.T) {
	b := testBroker(testNamespace, testBrokerName)
	r := &Reconciler{filterImage: "filter:v1", natsURL: "nats://localhost:4222"}
	existing := filterDeploymentFixture(b, r, brokerconfig.DefaultBrokerConfig(), 0)
	filter := containerByName(t, existing.Spec.Template.Spec.Containers, resources.FilterContainerName)
	filter.LivenessProbe.HTTPGet.Scheme = corev1.URISchemeHTTPS
	filter.ReadinessProbe.HTTPGet.Scheme = corev1.URISchemeHTTPS
	kube := kubefake.NewSimpleClientset(existing)
	r.kubeClientSet = kube
	r.deploymentLister = newDeploymentLister(existing)
	policy := autoscaledReplicaPolicy(autoscaler.Settings{MinScale: 0})

	if err := r.reconcileFilterDeployment(testContext(), b, "TEST_STREAM", brokerconfig.DefaultBrokerConfig(), policy); err != nil {
		t.Fatal(err)
	}
	if got := countResourceActions(kube.Actions(), "update", "deployments"); got != 1 {
		t.Fatalf("first reconcile Deployment updates = %d, want 1", got)
	}
	updated, err := kube.AppsV1().Deployments(b.Namespace).Get(context.Background(), existing.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	updatedFilter := containerByName(t, updated.Spec.Template.Spec.Containers, resources.FilterContainerName)
	for name, probe := range map[string]*corev1.Probe{"liveness": updatedFilter.LivenessProbe, "readiness": updatedFilter.ReadinessProbe} {
		if probe == nil || probe.HTTPGet == nil || probe.HTTPGet.Scheme != corev1.URISchemeHTTP {
			t.Errorf("%s probe = %#v, want HTTP scheme", name, probe)
		}
	}

	kube.ClearActions()
	r.deploymentLister = newDeploymentLister(updated)
	if err := r.reconcileFilterDeployment(testContext(), b, "TEST_STREAM", brokerconfig.DefaultBrokerConfig(), policy); err != nil {
		t.Fatal(err)
	}
	if got := countResourceActions(kube.Actions(), "update", "deployments"); got != 0 {
		t.Fatalf("second reconcile Deployment updates = %d, want 0; actions=%#v", got, kube.Actions())
	}
}

func TestReconcileFilterServiceRepairsRenamedPortOnce(t *testing.T) {
	b := testBroker(testNamespace, testBrokerName)
	existing := resources.MakeFilterService(b)
	defaultFilterServiceAsAPIServer(existing)
	existing.Spec.Ports[0].Name = "webhook-renamed"
	kube := kubefake.NewSimpleClientset(existing)
	r := &Reconciler{kubeClientSet: kube, serviceLister: newServiceLister(existing)}

	if _, err := r.reconcileFilterService(testContext(), b); err != nil {
		t.Fatal(err)
	}
	if got := countResourceActions(kube.Actions(), "update", "services"); got != 1 {
		t.Fatalf("first reconcile Service updates = %d, want 1", got)
	}
	updated, err := kube.CoreV1().Services(b.Namespace).Get(context.Background(), existing.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	assertFilterServiceAllocationsPreserved(t, existing, updated)
	servicePortByName(t, updated.Spec.Ports, resources.IngressPortName)

	kube.ClearActions()
	r.serviceLister = newServiceLister(updated)
	if _, err := r.reconcileFilterService(testContext(), b); err != nil {
		t.Fatal(err)
	}
	if got := countResourceActions(kube.Actions(), "update", "services"); got != 0 {
		t.Fatalf("second reconcile Service updates = %d, want 0; actions=%#v", got, kube.Actions())
	}
}

func TestReconcileFilterServiceRejectsAmbiguousRenamedPort(t *testing.T) {
	b := testBroker(testNamespace, testBrokerName)
	existing := resources.MakeFilterService(b)
	defaultFilterServiceAsAPIServer(existing)
	existing.Spec.Ports[0].Name = "first-renamed"
	existing.Spec.Ports = append(existing.Spec.Ports, corev1.ServicePort{
		Name: "second-renamed", Protocol: corev1.ProtocolTCP, Port: 80, TargetPort: intstr.FromInt(18080),
	})
	original := existing.DeepCopy()
	kube := kubefake.NewSimpleClientset(existing)
	r := &Reconciler{kubeClientSet: kube, serviceLister: newServiceLister(existing)}

	if _, err := r.reconcileFilterService(testContext(), b); err == nil || !strings.Contains(err.Error(), "unambiguously") {
		t.Fatalf("reconcileFilterService() error = %v, want ambiguous port rejection", err)
	}
	if got := countResourceActions(kube.Actions(), "update", "services"); got != 0 {
		t.Fatalf("ambiguous Service updates = %d, want 0", got)
	}
	stored, err := kube.CoreV1().Services(b.Namespace).Get(context.Background(), existing.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !apiequality.Semantic.DeepEqual(stored, original) {
		t.Fatalf("ambiguous Service was mutated: got=%#v want=%#v", stored, original)
	}
}

func TestReconcileFilterServiceOwnedDriftConvergesOnce(t *testing.T) {
	b := testBroker(testNamespace, testBrokerName)
	existing := resources.MakeFilterService(b)
	appProtocol := "custom.example/http"
	existing.Spec.Ports = append(existing.Spec.Ports, corev1.ServicePort{
		Name: "sidecar-extra", Protocol: corev1.ProtocolTCP, Port: 9090,
		TargetPort: intstr.FromInt(9090), AppProtocol: &appProtocol,
	})
	defaultFilterServiceAsAPIServer(existing)
	existing.Spec.Ports[0].Port = 81
	existing.Spec.Ports[0].TargetPort = intstr.FromInt(8181)
	kube := kubefake.NewSimpleClientset(existing)
	r := &Reconciler{kubeClientSet: kube, serviceLister: newServiceLister(existing)}

	if _, err := r.reconcileFilterService(testContext(), b); err != nil {
		t.Fatal(err)
	}
	if got := countResourceActions(kube.Actions(), "update", "services"); got != 1 {
		t.Fatalf("first reconcile Service updates = %d, want 1", got)
	}
	updated, err := kube.CoreV1().Services(b.Namespace).Get(context.Background(), existing.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	assertFilterServiceAllocationsPreserved(t, existing, updated)
	if got := servicePortByName(t, updated.Spec.Ports, resources.IngressPortName); got.Port != 80 || got.TargetPort != intstr.FromInt(resources.FilterPortNumber) {
		t.Errorf("ingress port = %#v, want controller values", got)
	}
	if got, want := servicePortByName(t, updated.Spec.Ports, "sidecar-extra"), servicePortByName(t, existing.Spec.Ports, "sidecar-extra"); !apiequality.Semantic.DeepEqual(got, want) {
		t.Errorf("extra Service port changed: got=%#v want=%#v", got, want)
	}

	kube.ClearActions()
	r.serviceLister = newServiceLister(updated)
	if _, err := r.reconcileFilterService(testContext(), b); err != nil {
		t.Fatal(err)
	}
	if got := countResourceActions(kube.Actions(), "update", "services"); got != 0 {
		t.Fatalf("second reconcile Service updates = %d, want 0; actions=%#v", got, kube.Actions())
	}
}

func defaultFilterDeploymentAsAPIServer(deployment *appsv1.Deployment) {
	deployment.Spec.Strategy = appsv1.DeploymentStrategy{
		Type: appsv1.RollingUpdateDeploymentStrategyType,
		RollingUpdate: &appsv1.RollingUpdateDeployment{
			MaxUnavailable: ptr.To(intstr.FromString("25%")),
			MaxSurge:       ptr.To(intstr.FromString("25%")),
		},
	}
	deployment.Spec.RevisionHistoryLimit = ptr.To[int32](10)
	deployment.Spec.ProgressDeadlineSeconds = ptr.To[int32](600)
	podSpec := &deployment.Spec.Template.Spec
	podSpec.RestartPolicy = corev1.RestartPolicyAlways
	podSpec.DNSPolicy = corev1.DNSClusterFirst
	podSpec.SchedulerName = corev1.DefaultSchedulerName
	podSpec.SecurityContext = &corev1.PodSecurityContext{}
	for index := range podSpec.Containers {
		container := &podSpec.Containers[index]
		container.ImagePullPolicy = defaultImagePullPolicy(container.Image)
		container.TerminationMessagePath = corev1.TerminationMessagePathDefault
		container.TerminationMessagePolicy = corev1.TerminationMessageReadFile
		for name, quantity := range container.Resources.Limits {
			quantity.RoundUp(-3)
			container.Resources.Limits[name] = quantity
		}
		for name, quantity := range container.Resources.Requests {
			quantity.RoundUp(-3)
			container.Resources.Requests[name] = quantity
		}
		for _, probe := range []*corev1.Probe{container.LivenessProbe, container.ReadinessProbe} {
			if probe == nil {
				continue
			}
			probe.HTTPGet.Scheme = corev1.URISchemeHTTP
			probe.TimeoutSeconds = 1
			probe.SuccessThreshold = 1
			probe.FailureThreshold = 3
		}
		for envIndex := range container.Env {
			variable := &container.Env[envIndex]
			if variable.Name == "POD_NAME" && variable.ValueFrom != nil && variable.ValueFrom.FieldRef != nil {
				variable.ValueFrom.FieldRef.APIVersion = "v1"
			}
			if variable.ValueFrom != nil && variable.ValueFrom.FileKeyRef != nil && variable.ValueFrom.FileKeyRef.Optional == nil {
				variable.ValueFrom.FileKeyRef.Optional = ptr.To(false)
			}
		}
	}
}

func filterDeploymentFixture(broker *eventingv1.Broker, reconciler *Reconciler, brokerConfig *brokerconfig.NatsJetStreamBrokerConfig, replicas int32) *appsv1.Deployment {
	deployment := resources.MakeFilterDeployment(&resources.FilterArgs{
		Broker:             broker,
		Image:              reconciler.filterImage,
		ServiceAccountName: reconciler.dataplaneIdentity(broker),
		StreamName:         "TEST_STREAM",
		NatsURL:            reconciler.natsURL,
		NatsConfigJSON:     reconciler.natsConfigJSON,
		Template:           brokerConfig.Filter,
	})
	// The API server stores an independent object; do not let defaulting the
	// fixture mutate pointer/map values in the controller's config snapshot.
	deployment = deployment.DeepCopy()
	deployment.Spec.Replicas = ptr.To(replicas)
	defaultFilterDeploymentAsAPIServer(deployment)
	return deployment
}

func defaultFilterServiceAsAPIServer(service *corev1.Service) {
	service.Spec.Type = corev1.ServiceTypeClusterIP
	service.Spec.SessionAffinity = corev1.ServiceAffinityNone
	service.Spec.ClusterIP = "10.0.0.10"
	service.Spec.ClusterIPs = []string{service.Spec.ClusterIP}
	service.Spec.IPFamilies = []corev1.IPFamily{corev1.IPv4Protocol}
	service.Spec.IPFamilyPolicy = ptr.To(corev1.IPFamilyPolicySingleStack)
	service.Spec.InternalTrafficPolicy = ptr.To(corev1.ServiceInternalTrafficPolicyCluster)
}

func countResourceActions(actions []clienttesting.Action, verb, resource string) int {
	count := 0
	for _, action := range actions {
		if action.Matches(verb, resource) {
			count++
		}
	}
	return count
}

func assertNoDeleteActions(t *testing.T, actions []clienttesting.Action, clientName string) {
	t.Helper()
	for _, action := range actions {
		if action.GetVerb() == "delete" || strings.HasPrefix(action.GetVerb(), "deletecollection") {
			t.Errorf("%s cleanup action occurred: %#v", clientName, action)
		}
	}
}

func assertDeletePreconditions(t *testing.T, actions []clienttesting.Action, resource string, wantUID types.UID, wantResourceVersion string, wantPropagation *metav1.DeletionPropagation) {
	t.Helper()
	for _, action := range actions {
		if !action.Matches("delete", resource) {
			continue
		}
		assertDeleteOptionsValues(t, action.(clienttesting.DeleteAction).GetDeleteOptions(), wantUID, wantResourceVersion, wantPropagation)
		return
	}
	t.Fatalf("%s delete action was not recorded", resource)
}

func assertDeleteOptionsValues(t *testing.T, options metav1.DeleteOptions, wantUID types.UID, wantResourceVersion string, wantPropagation *metav1.DeletionPropagation) {
	t.Helper()
	if options.Preconditions == nil || options.Preconditions.UID == nil || *options.Preconditions.UID != wantUID ||
		options.Preconditions.ResourceVersion == nil || *options.Preconditions.ResourceVersion != wantResourceVersion {
		t.Fatalf("delete preconditions = %#v, want UID %q resourceVersion %q", options.Preconditions, wantUID, wantResourceVersion)
	}
	if !apiequality.Semantic.DeepEqual(options.PropagationPolicy, wantPropagation) {
		t.Fatalf("delete propagation = %v, want %v", options.PropagationPolicy, wantPropagation)
	}
}

func errorChainHasConflict(err error) bool {
	for err != nil {
		if apierrs.IsConflict(err) {
			return true
		}
		err = errors.Unwrap(err)
	}
	return false
}

func assertFilterDeploymentUnownedFieldsPreserved(t *testing.T, before, after *appsv1.Deployment) {
	t.Helper()
	if !apiequality.Semantic.DeepEqual(after.Spec.Strategy, before.Spec.Strategy) ||
		!apiequality.Semantic.DeepEqual(after.Spec.RevisionHistoryLimit, before.Spec.RevisionHistoryLimit) ||
		!apiequality.Semantic.DeepEqual(after.Spec.ProgressDeadlineSeconds, before.Spec.ProgressDeadlineSeconds) {
		t.Errorf("Deployment API defaults were not preserved: before=%#v after=%#v", before.Spec, after.Spec)
	}
	if after.Labels["admission.example/extra"] != "preserved" || after.Spec.Template.Annotations["sidecar.example/injected"] != "true" {
		t.Errorf("extra metadata was not preserved: labels=%v annotations=%v", after.Labels, after.Spec.Template.Annotations)
	}
	if !apiequality.Semantic.DeepEqual(after.Spec.Template.Spec.Tolerations, before.Spec.Template.Spec.Tolerations) {
		t.Errorf("Pod-level admission fields changed: before=%v after=%v", before.Spec.Template.Spec.Tolerations, after.Spec.Template.Spec.Tolerations)
	}
	if got := containerByName(t, after.Spec.Template.Spec.Containers, "injected-sidecar"); got.Image != "sidecar:v1" {
		t.Errorf("sidecar = %#v, want preserved", got)
	}
	filter := containerByName(t, after.Spec.Template.Spec.Containers, resources.FilterContainerName)
	if got := envValue(filter.Env, "ADMISSION_EXTRA"); got != "preserved" {
		t.Errorf("extra filter env = %q, want preserved", got)
	}
	foundExtraPort := false
	for _, port := range filter.Ports {
		if port.Name == "admin-extra" && port.ContainerPort == 9090 {
			foundExtraPort = true
		}
	}
	if !foundExtraPort {
		t.Errorf("extra filter port was not preserved: %v", filter.Ports)
	}
}

func assertFilterAPIDefaultFixture(t *testing.T, deployment *appsv1.Deployment) {
	t.Helper()
	filter := containerByName(t, deployment.Spec.Template.Spec.Containers, resources.FilterContainerName)
	for _, probe := range []*corev1.Probe{filter.LivenessProbe, filter.ReadinessProbe} {
		if probe == nil || probe.HTTPGet == nil || probe.HTTPGet.Scheme != corev1.URISchemeHTTP {
			t.Errorf("HTTP probe default scheme = %#v, want HTTP", probe)
		}
	}
	for _, variable := range filter.Env {
		if variable.Name == "POD_NAME" {
			if variable.ValueFrom == nil || variable.ValueFrom.FieldRef == nil || variable.ValueFrom.FieldRef.APIVersion != "v1" {
				t.Errorf("POD_NAME fieldRef = %#v, want apiVersion v1", variable.ValueFrom)
			}
			return
		}
	}
	t.Error("POD_NAME env is missing")
}

func assertDefaultFilterContainerResources(t *testing.T, got corev1.ResourceRequirements) {
	t.Helper()
	want := corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("100m"),
			corev1.ResourceMemory: resource.MustParse("64Mi"),
		},
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("500m"),
			corev1.ResourceMemory: resource.MustParse("256Mi"),
		},
	}
	if !apiequality.Semantic.DeepEqual(got, want) {
		t.Errorf("filter resources = %#v, want defaults %#v", got, want)
	}
}

func assertHardenedFilterSecurityContext(t *testing.T, got *corev1.SecurityContext) {
	t.Helper()
	if got == nil {
		t.Fatal("filter securityContext is nil")
	}
	if got.RunAsNonRoot == nil || !*got.RunAsNonRoot {
		t.Errorf("runAsNonRoot = %v, want true", got.RunAsNonRoot)
	}
	if got.AllowPrivilegeEscalation == nil || *got.AllowPrivilegeEscalation {
		t.Errorf("allowPrivilegeEscalation = %v, want false", got.AllowPrivilegeEscalation)
	}
	if got.ReadOnlyRootFilesystem == nil || !*got.ReadOnlyRootFilesystem {
		t.Errorf("readOnlyRootFilesystem = %v, want true", got.ReadOnlyRootFilesystem)
	}
	if got.Capabilities == nil || len(got.Capabilities.Add) != 0 || !apiequality.Semantic.DeepEqual(got.Capabilities.Drop, []corev1.Capability{"ALL"}) {
		t.Errorf("capabilities = %#v, want drop ALL with no additions", got.Capabilities)
	}
	if got.SeccompProfile == nil || got.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault || got.SeccompProfile.LocalhostProfile != nil {
		t.Errorf("seccompProfile = %#v, want RuntimeDefault", got.SeccompProfile)
	}
}

func assertFilterServiceAllocationsPreserved(t *testing.T, before, after *corev1.Service) {
	t.Helper()
	if after.Spec.ClusterIP != before.Spec.ClusterIP ||
		!apiequality.Semantic.DeepEqual(after.Spec.ClusterIPs, before.Spec.ClusterIPs) ||
		!apiequality.Semantic.DeepEqual(after.Spec.IPFamilies, before.Spec.IPFamilies) ||
		!apiequality.Semantic.DeepEqual(after.Spec.IPFamilyPolicy, before.Spec.IPFamilyPolicy) ||
		!apiequality.Semantic.DeepEqual(after.Spec.InternalTrafficPolicy, before.Spec.InternalTrafficPolicy) ||
		after.Spec.Type != before.Spec.Type || after.Spec.SessionAffinity != before.Spec.SessionAffinity {
		t.Errorf("Service API allocations/defaults changed: before=%#v after=%#v", before.Spec, after.Spec)
	}
}

func containerByName(t *testing.T, containers []corev1.Container, name string) *corev1.Container {
	t.Helper()
	for index := range containers {
		if containers[index].Name == name {
			return &containers[index]
		}
	}
	t.Fatalf("container %q not found in %v", name, containers)
	return nil
}

func envValue(env []corev1.EnvVar, name string) string {
	for _, variable := range env {
		if variable.Name == name {
			return variable.Value
		}
	}
	return ""
}

func envVarByName(t *testing.T, env []corev1.EnvVar, name string) *corev1.EnvVar {
	t.Helper()
	for index := range env {
		if env[index].Name == name {
			return &env[index]
		}
	}
	t.Fatalf("env %q not found in %v", name, env)
	return nil
}

func insertEnvVar(env []corev1.EnvVar, index int, variable corev1.EnvVar) []corev1.EnvVar {
	result := make([]corev1.EnvVar, 0, len(env)+1)
	result = append(result, env[:index]...)
	result = append(result, variable)
	result = append(result, env[index:]...)
	return result
}

func envVarIndex(env []corev1.EnvVar, name string) int {
	for index := range env {
		if env[index].Name == name {
			return index
		}
	}
	return -1
}

func servicePortByName(t *testing.T, ports []corev1.ServicePort, name string) *corev1.ServicePort {
	t.Helper()
	for index := range ports {
		if ports[index].Name == name {
			return &ports[index]
		}
	}
	t.Fatalf("Service port %q not found in %v", name, ports)
	return nil
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
			eventingClient:       eventingfake.NewSimpleClientset(triggerObjects(triggers...)...),
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

	t.Run("empty cache with live trigger does not run cleanup", func(t *testing.T) {
		r, kube := setup(t)
		b := testBroker(testNamespace, testBrokerName)
		liveTrigger := testTrigger(testNamespace, "live-trigger", testBrokerName)
		liveTrigger.UID = "live-trigger-uid"
		r.eventingClient = eventingfake.NewSimpleClientset(liveTrigger)
		dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
		r.dynamicClient = dynamicClient
		filterDeployment := ownedFilterDeployment(b, 1)
		filterDeployment.UID = "deployment-uid"
		filterDeployment.ResourceVersion = "deployment-rv"
		filterService := resources.MakeFilterService(b)
		filterService.UID = "service-uid"
		filterService.ResourceVersion = "service-rv"
		if err := kube.Tracker().Add(filterDeployment); err != nil {
			t.Fatal(err)
		}
		if err := kube.Tracker().Add(filterService); err != nil {
			t.Fatal(err)
		}
		r.deploymentLister = newDeploymentLister(deploymentWithReady(ingressNS, ingressName, 1), filterDeployment)
		r.serviceLister = newServiceLister(filterService)

		if err := r.ReconcileKind(testContext(), b); err != nil {
			t.Fatalf("ReconcileKind() with live Trigger: %v", err)
		}
		assertNoDeleteActions(t, kube.Actions(), "Kubernetes")
		assertNoDeleteActions(t, dynamicClient.Actions(), "dynamic")
	})

	t.Run("live trigger list error does not run cleanup", func(t *testing.T) {
		r, kube := setup(t)
		b := testBroker(testNamespace, testBrokerName)
		live := eventingfake.NewSimpleClientset()
		live.PrependReactor("list", "triggers", func(clienttesting.Action) (bool, runtime.Object, error) {
			return true, nil, errors.New("live Trigger list failed")
		})
		r.eventingClient = live
		dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
		r.dynamicClient = dynamicClient
		filterDeployment := ownedFilterDeployment(b, 1)
		filterDeployment.UID = "deployment-uid"
		filterDeployment.ResourceVersion = "deployment-rv"
		filterService := resources.MakeFilterService(b)
		filterService.UID = "service-uid"
		filterService.ResourceVersion = "service-rv"
		if err := kube.Tracker().Add(filterDeployment); err != nil {
			t.Fatal(err)
		}
		if err := kube.Tracker().Add(filterService); err != nil {
			t.Fatal(err)
		}
		r.deploymentLister = newDeploymentLister(deploymentWithReady(ingressNS, ingressName, 1), filterDeployment)
		r.serviceLister = newServiceLister(filterService)

		err := r.ReconcileKind(testContext(), b)
		if err == nil || !strings.Contains(err.Error(), "failed to confirm live triggers") {
			t.Fatalf("ReconcileKind() error = %v, want live Trigger list failure", err)
		}
		assertNoDeleteActions(t, kube.Actions(), "Kubernetes")
		assertNoDeleteActions(t, dynamicClient.Actions(), "dynamic")
	})

	t.Run("no triggers: RBAC is revoked even when filter deletion fails", func(t *testing.T) {
		r, kube := setup(t)
		r.natsConfigJSON = `{"auth":{"credentialFile":{"secret":{"name":"credentials"}}}}`
		b := testBroker(testNamespace, testBrokerName)
		if err := r.reconcileDataplaneRBAC(testContext(), b); err != nil {
			t.Fatal(err)
		}
		if _, err := kube.AppsV1().Deployments(b.Namespace).Create(context.Background(), &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:            resources.FilterName(b.Name),
				Namespace:       b.Namespace,
				UID:             "deployment-uid",
				ResourceVersion: "deployment-rv",
				OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(b, eventingv1.SchemeGroupVersion.WithKind("Broker"))},
			},
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

	t.Run("Broker DLS correction precedes inherited audience cap validation", func(t *testing.T) {
		const triggerCount = brokeroidc.MaxAudiences
		staleDLSAudience := "https://stale-inherited-dls.example"
		correctedDLSAudience := "https://subscriber-00.example"
		addressableGVK := schema.GroupVersionKind{Group: "testing.eventing.knative.dev", Version: "v1", Kind: "OIDCSink"}
		addressableGVR := schema.GroupVersionResource{Group: addressableGVK.Group, Version: addressableGVK.Version, Resource: "oidcsinks"}
		addressableName := "broker-dls"
		triggers := make([]*eventingv1.Trigger, 0, triggerCount)
		for i := 0; i < triggerCount; i++ {
			trigger := testTrigger(testNamespace, fmt.Sprintf("trigger-%02d", i), testBrokerName)
			trigger.Generation = 1
			trigger.Status.ObservedGeneration = 1
			subscriberAudience := fmt.Sprintf("https://subscriber-%02d.example", i)
			trigger.Status.SubscriberAudience = &subscriberAudience
			trigger.Status.DeadLetterSinkAudience = &staleDLSAudience
			trigger.Status.MarkSubscriberResolvedSucceeded()
			trigger.Status.MarkDeadLetterSinkResolvedSucceeded()
			triggers = append(triggers, trigger)
		}

		r, kube := setup(t, triggers...)
		addressable := &unstructured.Unstructured{Object: map[string]interface{}{
			"apiVersion": addressableGVK.GroupVersion().String(),
			"kind":       addressableGVK.Kind,
			"metadata": map[string]interface{}{
				"name":      addressableName,
				"namespace": testNamespace,
			},
			"status": map[string]interface{}{
				"address": map[string]interface{}{
					"url":      "http://stale-dls.example",
					"audience": staleDLSAudience,
				},
			},
		}}
		resolverCtx, cancelResolver := context.WithCancel(testContext())
		t.Cleanup(cancelResolver)
		resolverCtx, dynamicClient := dynamicclientfake.With(resolverCtx, runtime.NewScheme(), addressable)
		resolverCtx = addressableinjection.WithDuck(resolverCtx)
		r.uriResolver = resolver.NewURIResolverFromTracker(resolverCtx, tracker.New(func(types.NamespacedName) {}, time.Hour))
		assignServiceAccountUIDsOnCreate(t, kube)
		if err := kube.Tracker().Add(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: testNamespace, UID: "namespace-uid"}}); err != nil {
			t.Fatal(err)
		}
		b := testBroker(testNamespace, testBrokerName)
		b.Generation = 7
		brokerGeneration := b.Generation
		b.Spec.Delivery = &eventingduckv1.DeliverySpec{DeadLetterSink: &duckv1.Destination{
			Ref: &duckv1.KReference{
				APIVersion: addressableGVK.GroupVersion().String(),
				Kind:       addressableGVK.Kind,
				Name:       addressableName,
			},
		}}

		err := r.ReconcileKind(resolverCtx, b)
		if err == nil || !strings.Contains(err.Error(), "invalid filter OIDC audiences") {
			t.Fatalf("first ReconcileKind() error = %v, want stale inherited audience cap failure", err)
		}
		if b.Status.DeadLetterSinkAudience == nil || *b.Status.DeadLetterSinkAudience != staleDLSAudience {
			t.Fatalf("Broker DLS audience after first resolution = %v, want referenced status %q", b.Status.DeadLetterSinkAudience, staleDLSAudience)
		}

		current, err := dynamicClient.Resource(addressableGVR).Namespace(testNamespace).Get(resolverCtx, addressableName, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if err := unstructured.SetNestedField(current.Object, "http://corrected-dls.example", "status", "address", "url"); err != nil {
			t.Fatal(err)
		}
		if err := unstructured.SetNestedField(current.Object, correctedDLSAudience, "status", "address", "audience"); err != nil {
			t.Fatal(err)
		}
		if _, err := dynamicClient.Resource(addressableGVR).Namespace(testNamespace).Update(resolverCtx, current, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(time.Second)
		for {
			resolved, resolveErr := r.resolveDeadLetterSink(resolverCtx, b)
			if resolveErr == nil && resolved.Audience != nil && *resolved.Audience == correctedDLSAudience {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("referenced DLS audience did not update through the resolver: address=%#v error=%v", resolved, resolveErr)
			}
			time.Sleep(10 * time.Millisecond)
		}

		err = r.ReconcileKind(resolverCtx, b)
		if err == nil || !strings.Contains(err.Error(), "invalid filter OIDC audiences") {
			t.Fatalf("second ReconcileKind() error = %v, want stale inherited Trigger status to remain over the cap", err)
		}
		if b.Generation != brokerGeneration {
			t.Fatalf("Broker generation changed during referenced status correction: got %d, want %d", b.Generation, brokerGeneration)
		}
		if b.Status.DeadLetterSinkAudience == nil || *b.Status.DeadLetterSinkAudience != correctedDLSAudience {
			t.Fatalf("Broker DLS audience after cap failure = %v, want freshly resolved %q", b.Status.DeadLetterSinkAudience, correctedDLSAudience)
		}
		condition := b.Status.GetCondition(eventingv1.BrokerConditionDeadLetterSinkResolved)
		if condition == nil || !condition.IsTrue() {
			t.Fatalf("DeadLetterSinkResolved = %#v, want true before audience validation", condition)
		}

		// Model the Trigger controller inheriting the freshly resolved Broker DLS.
		// The corrected audience de-duplicates with subscriber-00, reducing the
		// aggregate from 33 to the accepted limit of 32 on the next Broker pass.
		for _, trigger := range triggers {
			trigger.Status.DeadLetterSinkAudience = &correctedDLSAudience
		}
		if err := r.ReconcileKind(resolverCtx, b); err != nil {
			t.Fatalf("third ReconcileKind() after inherited DLS correction = %v", err)
		}
		if _, err := kube.AppsV1().Deployments(testNamespace).Get(context.Background(), resources.FilterName(testBrokerName), metav1.GetOptions{}); err != nil {
			t.Errorf("filter Deployment was not created after audience cap recovery: %v", err)
		}
	})

	t.Run("foreign OIDC RBAC clears stale Broker readiness", func(t *testing.T) {
		audience := "https://subscriber.example"
		trigger := testTrigger(testNamespace, "oidc-trigger", testBrokerName)
		trigger.Generation = 1
		trigger.Status.ObservedGeneration = 1
		trigger.Status.SubscriberAudience = &audience
		trigger.Status.MarkSubscriberResolvedSucceeded()
		trigger.Status.MarkDeadLetterSinkNotConfigured()
		r, kube := setup(t, trigger)
		namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: testNamespace, UID: "namespace-uid"}}
		foreign := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{
			Name: brokeroidc.DeliveryServiceAccountName, Namespace: testNamespace,
			Labels: map[string]string{brokeroidc.ManagedServiceAccountLabelKey: "foreign-manager"},
		}}
		if err := kube.Tracker().Add(namespace); err != nil {
			t.Fatal(err)
		}
		if err := kube.Tracker().Add(foreign); err != nil {
			t.Fatal(err)
		}

		b := testBroker(testNamespace, testBrokerName)
		b.Generation = 1
		b.Status = *eventingv1.TestHelper.ReadyBrokerStatusWithoutDLS()
		b.Status.ObservedGeneration = b.Generation
		if !b.IsReady() {
			t.Fatal("Broker fixture is not stale-ready before the RBAC failure")
		}
		recorder := record.NewFakeRecorder(10)
		ctx := controller.WithEventRecorder(logging.WithLogger(context.Background(), zap.NewNop().Sugar()), recorder)
		err := r.ReconcileKind(ctx, b)
		if err == nil || !strings.Contains(err.Error(), "not owned by Namespace") {
			t.Fatalf("ReconcileKind() error = %v, want foreign OIDC identity rejection", err)
		}
		condition := b.Status.GetCondition(eventingv1.BrokerConditionFilter)
		if condition == nil || condition.Status != corev1.ConditionFalse || condition.Reason != "FilterRBACFailed" {
			t.Fatalf("Filter condition = %#v, want False/FilterRBACFailed", condition)
		}
		if b.IsReady() {
			t.Error("Broker retained stale Ready=True after filter RBAC failed")
		}
		deadline := time.After(time.Second)
		for {
			select {
			case event := <-recorder.Events:
				if strings.Contains(event, "Warning FilterRBACFailed") {
					return
				}
			case <-deadline:
				t.Error("FilterRBACFailed Warning event was not emitted")
				return
			}
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
