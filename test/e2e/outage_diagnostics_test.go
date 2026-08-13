//go:build e2e
// +build e2e

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

package e2e

import (
	"context"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/utils/ptr"

	fakekubeclient "knative.dev/pkg/client/injection/kube/client/fake"
)

func TestWorkloadStatusSnapshotIsSanitizedBoundedAndBestEffort(t *testing.T) {
	ctx, kube := fakekubeclient.With(context.Background(),
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: "keda-operator", Namespace: "keda"},
			Spec:       appsv1.DeploymentSpec{Replicas: ptr.To[int32](0)},
			Status:     appsv1.DeploymentStatus{Replicas: 1, UnavailableReplicas: 1},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "filter", Namespace: "test-one"},
			Spec: corev1.PodSpec{
				NodeName: "worker-a",
				InitContainers: []corev1.Container{{
					Name: "init-setup",
				}},
				Containers: []corev1.Container{{
					Name: "filter",
					Env:  []corev1.EnvVar{{Name: "PRIVATE_VALUE", Value: "must-not-be-logged"}},
				}},
				EphemeralContainers: []corev1.EphemeralContainer{{
					EphemeralContainerCommon: corev1.EphemeralContainerCommon{Name: "debugger"},
				}},
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				ContainerStatuses: []corev1.ContainerStatus{{
					Name: "filter", Ready: false, RestartCount: 2,
				}},
			},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "producer", Namespace: "test-one"},
			Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "producer"}}},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "recorder", Namespace: "test-one"},
			Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "recorder"}}},
		},
		&batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{Name: "producer", Namespace: "test-one"},
			Status:     batchv1.JobStatus{Failed: 1},
		},
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "credential", Namespace: "test-one"},
			Data:       map[string][]byte{"password": []byte("super-secret-payload")},
		},
	)

	canceledCtx, cancel := context.WithCancel(ctx)
	cancel()
	snapshot := outageWorkloadStatusSnapshot(canceledCtx, []string{"test-one", "keda", "test-one", "missing"})
	for _, want := range []string{
		"namespace test-one",
		"pod filter phase=Running node=worker-a container=filter/ready=false/restarts=2",
		"job producer active=0 succeeded=0 failed=1",
		"namespace keda",
		"deployment keda-operator desired=0 replicas=1 updated=0 ready=0 available=0 unavailable=1",
		"namespace missing",
		"logs pod=filter container=init-setup instance=current tail=200 limitBytes=262144",
		"logs pod=filter container=debugger instance=previous tail=200 limitBytes=262144",
		"logs pod=producer container=producer instance=current tail=200 limitBytes=262144",
		"logs pod=recorder container=recorder instance=previous tail=200 limitBytes=262144",
		"fake logs",
	} {
		if !strings.Contains(snapshot, want) {
			t.Errorf("snapshot lacks %q:\n%s", want, snapshot)
		}
	}
	if got := strings.Count(snapshot, "namespace test-one\n"); got != 1 {
		t.Errorf("duplicate namespace snapshot count = %d, want 1:\n%s", got, snapshot)
	}
	for _, forbidden := range []string{"must-not-be-logged", "credential", "super-secret-payload"} {
		if strings.Contains(snapshot, forbidden) {
			t.Errorf("status snapshot exposes %q:\n%s", forbidden, snapshot)
		}
	}
	logActions := 0
	for _, action := range kube.Actions() {
		if action.GetResource().Resource == "secrets" {
			t.Errorf("status snapshot queried Secrets: %#v", action)
		}
		if action.GetResource().Resource != "pods" || action.GetSubresource() != "log" {
			continue
		}
		logActions++
		generic, ok := action.(clienttesting.GenericAction)
		if !ok {
			t.Errorf("pod log action type = %T, want GenericAction", action)
			continue
		}
		options, ok := generic.GetValue().(*corev1.PodLogOptions)
		if !ok {
			t.Errorf("pod log options type = %T, want *PodLogOptions", generic.GetValue())
			continue
		}
		if !options.Timestamps || options.TailLines == nil || *options.TailLines != 200 || options.LimitBytes == nil || *options.LimitBytes != 256*1024 {
			t.Errorf("pod log options = %#v, want timestamps and bounded tail/bytes", options)
		}
	}
	if logActions != 10 {
		t.Errorf("pod log actions = %d, want current+previous for five containers", logActions)
	}
}

func TestShouldCaptureOutageSnapshot(t *testing.T) {
	tests := []struct {
		name         string
		failedBefore bool
		failedAfter  bool
		want         bool
	}{
		{name: "success", want: false},
		{name: "new failure", failedAfter: true, want: true},
		{name: "preexisting failure", failedBefore: true, failedAfter: true, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := shouldCaptureOutageSnapshot(test.failedBefore, test.failedAfter); got != test.want {
				t.Errorf("shouldCaptureOutageSnapshot(%t, %t) = %t, want %t", test.failedBefore, test.failedAfter, got, test.want)
			}
		})
	}
}
