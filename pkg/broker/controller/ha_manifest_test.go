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
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	yamlutil "k8s.io/apimachinery/pkg/util/yaml"

	"knative.dev/eventing-natss/pkg/broker/controller/resources"
)

func TestControlPlaneManifestsAreHighlyAvailable(t *testing.T) {
	tests := []struct {
		name           string
		deploymentPath string
	}{
		{name: "broker controller", deploymentPath: "../../../config/broker/500-broker-controller.yaml"},
		{name: "shared ingress", deploymentPath: "../../../config/broker/500-shared-ingress.yaml"},
	}
	pdbs := readPodDisruptionBudgetManifests(t, "../../../config/broker")

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			deployment := readDeploymentManifest(t, test.deploymentPath)
			if deployment.Spec.Replicas == nil {
				t.Errorf("Deployment %q replicas is unset, want 2", deployment.Name)
			} else if *deployment.Spec.Replicas != 2 {
				t.Errorf("Deployment %q replicas = %d, want 2", deployment.Name, *deployment.Spec.Replicas)
			}
			assertPreferredControlPlanePlacement(t, deployment)

			pdb := findPodDisruptionBudgetManifest(t, pdbs, deployment.Name)
			if pdb.Namespace != deployment.Namespace {
				t.Errorf("PodDisruptionBudget %q namespace = %q, want %q", pdb.Name, pdb.Namespace, deployment.Namespace)
			}
			if got := pdb.Labels["nats.eventing.knative.dev/release"]; got != "devel" {
				t.Errorf("PodDisruptionBudget %q release label = %q, want devel", pdb.Name, got)
			}
			if pdb.Spec.MinAvailable == nil || pdb.Spec.MinAvailable.Type != intstr.Int || pdb.Spec.MinAvailable.IntVal != 1 {
				t.Errorf("PodDisruptionBudget %q minAvailable = %v, want integer 1", pdb.Name, pdb.Spec.MinAvailable)
			}
			if !reflect.DeepEqual(pdb.Spec.Selector, deployment.Spec.Selector) {
				t.Errorf("PodDisruptionBudget %q selector = %#v, want Deployment selector %#v", pdb.Name, pdb.Spec.Selector, deployment.Spec.Selector)
			}
		})
	}
}

func TestBrokerManifestsDoNotCreateFilterPodDisruptionBudget(t *testing.T) {
	for _, pdb := range readPodDisruptionBudgetManifests(t, "../../../config/broker") {
		if strings.Contains(pdb.Name, "broker-filter") || strings.HasPrefix(pdb.Name, "natsjs-filter") {
			t.Errorf("PodDisruptionBudget %q targets a scale-to-zero filter by name", pdb.Name)
		}
		if pdb.Spec.Selector != nil && pdb.Spec.Selector.MatchLabels[resources.RoleLabelKey] == resources.FilterRoleLabelValue {
			t.Errorf("PodDisruptionBudget %q targets scale-to-zero filters with selector %#v", pdb.Name, pdb.Spec.Selector)
		}
	}
}

func assertPreferredControlPlanePlacement(t *testing.T, deployment *appsv1.Deployment) {
	t.Helper()
	wantSelector := deployment.Spec.Selector
	requiredTopologyKeys := map[string]bool{
		corev1.LabelTopologyZone: false,
		corev1.LabelHostname:     false,
	}
	for _, constraint := range deployment.Spec.Template.Spec.TopologySpreadConstraints {
		if _, required := requiredTopologyKeys[constraint.TopologyKey]; !required {
			continue
		}
		if constraint.MaxSkew == 1 &&
			constraint.WhenUnsatisfiable == corev1.ScheduleAnyway &&
			reflect.DeepEqual(constraint.LabelSelector, wantSelector) {
			requiredTopologyKeys[constraint.TopologyKey] = true
		}
	}
	if requiredTopologyKeys[corev1.LabelTopologyZone] && requiredTopologyKeys[corev1.LabelHostname] {
		return
	}
	t.Errorf("Deployment %q topology spread = %#v, want ScheduleAnyway maxSkew=1 with matching selectors for both zone and hostname", deployment.Name, deployment.Spec.Template.Spec.TopologySpreadConstraints)
}

func findPodDisruptionBudgetManifest(t *testing.T, pdbs []*policyv1.PodDisruptionBudget, name string) *policyv1.PodDisruptionBudget {
	t.Helper()
	for _, pdb := range pdbs {
		if pdb.Name == name {
			return pdb
		}
	}
	t.Fatalf("PodDisruptionBudget %q not found", name)
	return nil
}

func readPodDisruptionBudgetManifests(t *testing.T, directory string) []*policyv1.PodDisruptionBudget {
	t.Helper()
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	var pdbs []*policyv1.PodDisruptionBudget
	for _, entry := range entries {
		if entry.IsDir() || (!strings.HasSuffix(entry.Name(), ".yaml") && !strings.HasSuffix(entry.Name(), ".yml")) {
			continue
		}
		path := filepath.Join(directory, entry.Name())
		file, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		decoder := yamlutil.NewYAMLOrJSONDecoder(file, 4096)
		for {
			object := &unstructured.Unstructured{}
			err := decoder.Decode(object)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				_ = file.Close()
				t.Fatalf("decode %s: %v", path, err)
			}
			if object.GetKind() != "PodDisruptionBudget" {
				continue
			}
			pdb := &policyv1.PodDisruptionBudget{}
			if err := runtime.DefaultUnstructuredConverter.FromUnstructured(object.Object, pdb); err != nil {
				_ = file.Close()
				t.Fatalf("decode PodDisruptionBudget %q from %s: %v", object.GetName(), path, err)
			}
			pdbs = append(pdbs, pdb)
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
	}
	return pdbs
}
