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
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"
)

func readClusterRoleManifest(t *testing.T, path string) *rbacv1.ClusterRole {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	role := &rbacv1.ClusterRole{}
	if err := yaml.Unmarshal(contents, role); err != nil {
		t.Fatal(err)
	}
	return role
}

func readRoleManifest(t *testing.T, path string) *rbacv1.Role {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	role := &rbacv1.Role{}
	if err := yaml.Unmarshal(contents, role); err != nil {
		t.Fatal(err)
	}
	return role
}

func readRoleBindingManifest(t *testing.T, path string) *rbacv1.RoleBinding {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	binding := &rbacv1.RoleBinding{}
	if err := yaml.Unmarshal(contents, binding); err != nil {
		t.Fatal(err)
	}
	return binding
}

func readServiceAccountManifest(t *testing.T, path string) *corev1.ServiceAccount {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	serviceAccount := &corev1.ServiceAccount{}
	if err := yaml.Unmarshal(contents, serviceAccount); err != nil {
		t.Fatal(err)
	}
	return serviceAccount
}

func readDeploymentManifest(t *testing.T, path string) *appsv1.Deployment {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	deployment := &appsv1.Deployment{}
	if err := yaml.Unmarshal(contents, deployment); err != nil {
		t.Fatal(err)
	}
	return deployment
}

func rulesForResource(rules []rbacv1.PolicyRule, resource string) []rbacv1.PolicyRule {
	var matched []rbacv1.PolicyRule
	for _, rule := range rules {
		for _, candidate := range rule.Resources {
			if candidate == resource || candidate == "*" {
				matched = append(matched, rule)
				break
			}
		}
	}
	return matched
}

func assertLifecycleVerbs(t *testing.T, rules []rbacv1.PolicyRule, resource string) {
	t.Helper()
	got := make(map[string]bool)
	for _, rule := range rulesForResource(rules, resource) {
		for _, verb := range rule.Verbs {
			got[verb] = true
		}
	}
	for _, verb := range []string{"get", "list", "watch", "create", "update", "delete"} {
		if !got[verb] && !got["*"] {
			t.Errorf("%s rules lack %q: %#v", resource, verb, rulesForResource(rules, resource))
		}
	}
}

func TestDataplaneClusterRoleHasNoSystemScopedAuthority(t *testing.T) {
	legacyRole := readClusterRoleManifest(t, "../../../config/broker/200-dataplane-clusterrole.yaml")
	if legacyRole.Name != DataplaneClusterRoleName {
		t.Errorf("legacy dataplane role name = %q, want compatibility name %q", legacyRole.Name, DataplaneClusterRoleName)
	}
	if len(legacyRole.Rules) != 0 {
		t.Errorf("legacy dataplane ClusterRole must be an empty security tombstone: %#v", legacyRole.Rules)
	}

	readerRole := readClusterRoleManifest(t, "../../../config/broker/200-filter-reader-clusterrole.yaml")
	if readerRole.Name != FilterReaderClusterRoleName {
		t.Errorf("filter reader role name = %q, want %q", readerRole.Name, FilterReaderClusterRoleName)
	}
	wantRules := []rbacv1.PolicyRule{{
		APIGroups: []string{"eventing.knative.dev"},
		Resources: []string{"brokers", "triggers"},
		Verbs:     []string{"get", "list", "watch"},
	}}
	if !reflect.DeepEqual(readerRole.Rules, wantRules) {
		t.Errorf("filter reader rules = %#v, want only Broker/Trigger reads %#v", readerRole.Rules, wantRules)
	}

	legacyBindingContents, err := os.ReadFile("../../../config/broker/201-dataplane-clusterrolebinding.yaml")
	if err != nil {
		t.Fatal(err)
	}
	legacyBinding := &rbacv1.ClusterRoleBinding{}
	if err := yaml.Unmarshal(legacyBindingContents, legacyBinding); err != nil {
		t.Fatal(err)
	}
	if len(legacyBinding.Subjects) != 0 || legacyBinding.RoleRef.Name != legacyRole.Name {
		t.Errorf("legacy ClusterRoleBinding must be an empty tombstone for %q: %#v", legacyRole.Name, legacyBinding)
	}
}

func TestBrokerControllerManifestCanManageOnlyNamespacedDataplaneRBAC(t *testing.T) {
	clusterRole := readClusterRoleManifest(t, "../../../config/broker/200-broker-controller-clusterrole.yaml")
	if rules := rulesForResource(clusterRole.Rules, "clusterrolebindings"); len(rules) != 0 {
		t.Errorf("controller must not manage ClusterRoleBindings: %#v", rules)
	}
	assertLifecycleVerbs(t, clusterRole.Rules, "serviceaccounts")
	assertLifecycleVerbs(t, clusterRole.Rules, "rolebindings")
	namespaceRules := rulesForResource(clusterRole.Rules, "namespaces")
	if len(namespaceRules) != 1 || !reflect.DeepEqual(namespaceRules[0].Verbs, []string{"get"}) {
		t.Errorf("controller namespace authority = %#v, want only get", namespaceRules)
	}
	clusterRoleRules := rulesForResource(clusterRole.Rules, "clusterroles")
	if len(clusterRoleRules) != 1 || !reflect.DeepEqual(clusterRoleRules[0].Verbs, []string{"bind"}) ||
		!reflect.DeepEqual(clusterRoleRules[0].ResourceNames, []string{FilterReaderClusterRoleName, OIDCTokenCreatorClusterRoleName}) {
		t.Errorf("controller ClusterRole bind authority = %#v, want exact reader and OIDC token creator roles", clusterRoleRules)
	}

	// Exact Secret Roles live in the system namespace, so their lifecycle can
	// be granted by the controller's namespaced Role rather than cluster-wide.
	systemRole := readRoleManifest(t, "../../../config/broker/200-broker-controller-role.yaml")
	assertLifecycleVerbs(t, systemRole.Rules, "roles")
	assertLifecycleVerbs(t, systemRole.Rules, "rolebindings")
}

func TestOnlyDedicatedOIDCClusterRoleCanCreateExactServiceAccountToken(t *testing.T) {
	roles := make(map[string][]rbacv1.PolicyRule)
	if err := filepath.WalkDir("../../../config", func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || (!strings.HasSuffix(path, ".yaml") && !strings.HasSuffix(path, ".yml")) {
			return nil
		}
		contents, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		meta := metav1.TypeMeta{}
		if err := yaml.Unmarshal(contents, &meta); err != nil {
			return err
		}
		switch meta.Kind {
		case "Role":
			role := rbacv1.Role{}
			if err := yaml.Unmarshal(contents, &role); err != nil {
				return err
			}
			roles[path] = role.Rules
		case "ClusterRole":
			role := rbacv1.ClusterRole{}
			if err := yaml.Unmarshal(contents, &role); err != nil {
				return err
			}
			roles[path] = role.Rules
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	wantPath := filepath.Clean("../../../config/broker/200-oidc-token-creator-clusterrole.yaml")
	foundDedicatedGrant := false
	for path, rules := range roles {
		for _, rule := range rules {
			coreAPI := len(rule.APIGroups) == 0
			for _, group := range rule.APIGroups {
				coreAPI = coreAPI || group == "" || group == "*"
			}
			if !coreAPI {
				continue
			}
			canCreate := false
			for _, verb := range rule.Verbs {
				canCreate = canCreate || verb == "create" || verb == "*"
			}
			if !canCreate {
				continue
			}
			for _, resource := range rule.Resources {
				if resource == "serviceaccounts/token" || resource == "serviceaccounts/*" || resource == "*" {
					if filepath.Clean(path) != wantPath {
						t.Errorf("%s must not create service account tokens: %#v", path, rule)
						continue
					}
					want := rbacv1.PolicyRule{
						APIGroups: []string{""}, Resources: []string{"serviceaccounts/token"},
						ResourceNames: []string{"natsjs-broker-oidc"}, Verbs: []string{"create"},
					}
					if !reflect.DeepEqual(rule, want) {
						t.Errorf("dedicated OIDC TokenRequest rule = %#v, want exact %#v", rule, want)
					}
					foundDedicatedGrant = true
				}
			}
		}
	}
	if !foundDedicatedGrant {
		t.Fatal("dedicated exact-name OIDC TokenRequest grant is missing")
	}
}

func TestIngressUsesDedicatedNamespacedRBAC(t *testing.T) {
	const (
		namespace          = "knative-eventing"
		serviceAccountName = "natsjetstream-broker-ingress"
	)
	serviceAccount := readServiceAccountManifest(t, "../../../config/broker/200-ingress-serviceaccount.yaml")
	if serviceAccount.Namespace != namespace || serviceAccount.Name != serviceAccountName {
		t.Errorf("ingress ServiceAccount = %s/%s, want %s/%s", serviceAccount.Namespace, serviceAccount.Name, namespace, serviceAccountName)
	}

	role := readRoleManifest(t, "../../../config/broker/200-ingress-role.yaml")
	wantRules := []rbacv1.PolicyRule{{
		APIGroups: []string{""}, Resources: []string{"configmaps"}, Verbs: []string{"get", "list", "watch"},
	}}
	if role.Namespace != namespace || role.Name != serviceAccountName || !reflect.DeepEqual(role.Rules, wantRules) {
		t.Errorf("ingress Role = %s/%s rules %#v, want exact ConfigMap reads", role.Namespace, role.Name, role.Rules)
	}

	configBinding := readRoleBindingManifest(t, "../../../config/broker/201-ingress-rolebinding.yaml")
	assertIngressBinding(t, configBinding, serviceAccountName, "Role", serviceAccountName)
	secretBinding := readRoleBindingManifest(t, "../../../config/broker/201-ingress-nats-secret-rolebinding.yaml")
	assertIngressBinding(t, secretBinding, serviceAccountName, "Role", natsSecretRoleName)

	deployment := readDeploymentManifest(t, "../../../config/broker/500-shared-ingress.yaml")
	if got := deployment.Spec.Template.Spec.ServiceAccountName; got != serviceAccountName {
		t.Errorf("ingress Deployment serviceAccountName = %q, want %q", got, serviceAccountName)
	}
	if len(deployment.Spec.Template.Spec.Containers) != 1 {
		t.Fatalf("ingress containers = %d, want one", len(deployment.Spec.Template.Spec.Containers))
	}
	namespaceEnvFound := false
	for _, env := range deployment.Spec.Template.Spec.Containers[0].Env {
		if env.Name == "NAMESPACE" && env.ValueFrom != nil && env.ValueFrom.FieldRef != nil && env.ValueFrom.FieldRef.FieldPath == "metadata.namespace" {
			namespaceEnvFound = true
		}
	}
	if !namespaceEnvFound {
		t.Error("ingress Deployment must inject NAMESPACE from metadata.namespace for namespace-scoped clients")
	}
}

func assertIngressBinding(t *testing.T, binding *rbacv1.RoleBinding, serviceAccountName, roleKind, roleName string) {
	t.Helper()
	if binding.Namespace != "knative-eventing" {
		t.Errorf("RoleBinding %q namespace = %q, want knative-eventing", binding.Name, binding.Namespace)
	}
	if binding.RoleRef.Kind != roleKind || binding.RoleRef.Name != roleName {
		t.Errorf("RoleBinding %q roleRef = %#v, want %s %q", binding.Name, binding.RoleRef, roleKind, roleName)
	}
	wantSubjects := []rbacv1.Subject{{Kind: "ServiceAccount", Name: serviceAccountName, Namespace: "knative-eventing"}}
	if !reflect.DeepEqual(binding.Subjects, wantSubjects) {
		t.Errorf("RoleBinding %q subjects = %#v, want %#v", binding.Name, binding.Subjects, wantSubjects)
	}
}
