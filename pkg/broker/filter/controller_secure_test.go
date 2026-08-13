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
	"testing"
	"time"

	"github.com/nats-io/nkeys"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clienttesting "k8s.io/client-go/testing"
	fakekubeclient "knative.dev/pkg/client/injection/kube/client/fake"
	"knative.dev/pkg/logging"

	eventingfake "knative.dev/eventing/pkg/client/clientset/versioned/fake"
	eventinginformers "knative.dev/eventing/pkg/client/informers/externalversions"
	brokerinformer "knative.dev/eventing/pkg/client/injection/informers/eventing/v1/broker"
	triggerinformer "knative.dev/eventing/pkg/client/injection/informers/eventing/v1/trigger"

	natstesting "knative.dev/eventing-natss/pkg/channel/jetstream/dispatcher/testing"
)

func TestNewControllerUsesNATSConfigSnapshotAndSystemSecret(t *testing.T) {
	t.Setenv("SYSTEM_NAMESPACE", "knative-eventing")
	t.Setenv("POD_NAME", "filter-test")
	t.Setenv("CONTAINER_NAME", "filter")
	t.Setenv("BROKER_NAME", "broker")
	t.Setenv("BROKER_NAMESPACE", "tenant")
	t.Setenv("STREAM_NAME", "broker-stream")

	server := natstesting.RunBasicJetstreamServer()
	defer natstesting.ShutdownJSServerAndRemoveStorage(t, server)
	userKey, err := nkeys.CreateUser()
	if err != nil {
		t.Fatal(err)
	}
	userSeed, err := userKey.Seed()
	if err != nil {
		t.Fatal(err)
	}
	credentialFile := []byte(fmt.Sprintf(`-----BEGIN NATS USER JWT-----
test.jwt.value
------END NATS USER JWT------

-----BEGIN USER NKEY SEED-----
%s
------END USER NKEY SEED------
`, userSeed))
	t.Setenv("NATS_CONFIG", fmt.Sprintf(`{
  "url": %q,
  "auth": {
    "credentialFile": {
      "secret": {"name": "nats-credentials", "key": "custom.creds"}
    }
  }
}`, server.ClientURL()))

	ctx := logging.WithLogger(context.Background(), zap.NewNop().Sugar())
	ctx, kubeClient := fakekubeclient.With(ctx,
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "nats-credentials",
				Namespace: "knative-eventing",
			},
			Data: map[string][]byte{"custom.creds": credentialFile},
		},
	)

	eventingFactory := eventinginformers.NewSharedInformerFactory(eventingfake.NewSimpleClientset(), 0)
	ctx = context.WithValue(ctx, brokerinformer.Key{}, eventingFactory.Eventing().V1().Brokers())
	ctx = context.WithValue(ctx, triggerinformer.Key{}, eventingFactory.Eventing().V1().Triggers())

	runtime := NewRuntime(ctx)
	runtime.NewController(ctx, nil)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := runtime.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("Runtime.Shutdown() = %v", err)
	}

	var configMapActions []clienttesting.Action
	for _, action := range kubeClient.Actions() {
		if action.GetResource().Resource == "configmaps" {
			configMapActions = append(configMapActions, action)
		}
	}
	if len(configMapActions) != 0 {
		t.Fatalf("ConfigMap actions = %#v, want none for an injected NATS_CONFIG snapshot", configMapActions)
	}

	var secretGets []clienttesting.GetAction
	for _, action := range kubeClient.Actions() {
		if action.Matches("get", "secrets") {
			secretGets = append(secretGets, action.(clienttesting.GetAction))
		}
	}
	if len(secretGets) != 1 {
		t.Fatalf("Secret get actions = %#v, want exactly one", secretGets)
	}
	if got, want := secretGets[0].GetNamespace(), "knative-eventing"; got != want {
		t.Errorf("Secret namespace = %q, want %q", got, want)
	}
	if got, want := secretGets[0].GetName(), "nats-credentials"; got != want {
		t.Errorf("Secret name = %q, want %q", got, want)
	}
}

func TestFilterNATSConfig(t *testing.T) {
	tests := []struct {
		name       string
		env        *envConfig
		wantURL    string
		wantSecret string
		wantErr    bool
	}{
		{
			name: "snapshot takes precedence over legacy URL",
			env: &envConfig{
				NatsURL: "nats://stale.example:4222",
				NatsConfig: `{
					"url":"tls://nats.example:4222",
					"auth":{"credentialFile":{"secret":{"name":"credentials"}}}
				}`,
			},
			wantURL:    "tls://nats.example:4222",
			wantSecret: "credentials",
		},
		{
			name:    "legacy URL fallback",
			env:     &envConfig{NatsURL: "nats://legacy.example:4222"},
			wantURL: "nats://legacy.example:4222",
		},
		{
			name:    "missing both",
			env:     &envConfig{},
			wantErr: true,
		},
		{
			name:    "invalid snapshot",
			env:     &envConfig{NatsURL: "nats://legacy.example:4222", NatsConfig: "{"},
			wantErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config, err := filterNATSConfig(test.env)
			if (err != nil) != test.wantErr {
				t.Fatalf("filterNATSConfig() error = %v, wantErr %v", err, test.wantErr)
			}
			if test.wantErr {
				return
			}
			if config.URL != test.wantURL {
				t.Errorf("URL = %q, want %q", config.URL, test.wantURL)
			}
			if test.wantSecret != "" {
				if config.Auth == nil || config.Auth.CredentialFile == nil || config.Auth.CredentialFile.Secret == nil {
					t.Fatal("credential Secret was not decoded")
				}
				if got := config.Auth.CredentialFile.Secret.Name; got != test.wantSecret {
					t.Errorf("credential Secret = %q, want %q", got, test.wantSecret)
				}
			}
		})
	}
}

func TestConnectFilterNATSLegacyURLDoesNotRequireKubeClient(t *testing.T) {
	server := natstesting.RunBasicJetstreamServer()
	defer natstesting.ShutdownJSServerAndRemoveStorage(t, server)

	// Intentionally do not inject a Kubernetes client. A URL-only filter is a
	// supported compatibility path and has no Secret references to resolve.
	connection, err := connectFilterNATS(context.Background(), &envConfig{NatsURL: server.ClientURL()})
	if err != nil {
		t.Fatalf("connectFilterNATS() = %v", err)
	}
	defer connection.Close()
	if !connection.IsConnected() {
		t.Error("URL-only filter did not establish a NATS connection")
	}
}
