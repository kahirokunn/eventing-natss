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

package nats

import (
	"testing"

	"knative.dev/eventing-natss/pkg/common/constants"
)

func TestLoadEventingNatsConfigSecureConnection(t *testing.T) {
	config, err := LoadEventingNatsConfig(map[string]string{constants.EventingNatsSettingsConfigKey: `
url: tls://nats.example.test:4222
auth:
  credentialFile:
    secret:
      name: nats-credentials
      key: account.creds
  tls:
    secret:
      name: nats-client-tls
tls:
  secret:
    name: nats-root-ca
connOpts:
  retryOnFailedConnect: true
  maxReconnects: 17
  reconnectWaitMilliseconds: 250
  reconnectJitterMilliseconds: 75
  reconnectJitterTLSMilliseconds: 125
`})
	if err != nil {
		t.Fatal(err)
	}
	if config.URL != "tls://nats.example.test:4222" {
		t.Errorf("URL = %q", config.URL)
	}
	if config.Auth == nil || config.Auth.CredentialFile == nil || config.Auth.CredentialFile.Secret == nil {
		t.Fatal("credentialFile secret was not decoded")
	}
	if config.Auth.TLS == nil || config.Auth.TLS.Secret == nil {
		t.Fatal("mTLS secret was not decoded")
	}
	if config.RootCA == nil || config.RootCA.Secret == nil {
		t.Fatal("root CA secret was not decoded")
	}
	if config.ConnOpts == nil || !config.ConnOpts.RetryOnFailedConnect || config.ConnOpts.MaxReconnects != 17 {
		t.Fatalf("connOpts = %#v", config.ConnOpts)
	}
}

func TestLoadEventingNatsConfigAutoscaler(t *testing.T) {
	config, err := LoadEventingNatsConfig(map[string]string{constants.EventingNatsSettingsConfigKey: `
url: nats://nats.nats-io.svc:4222
autoscaler:
  monitoringEndpoint: nats.nats-io.svc:8222
  account: "$G"
  useHttps: true
`})
	if err != nil {
		t.Fatal(err)
	}
	if config.Autoscaler == nil {
		t.Fatal("autoscaler config was not decoded")
	}
	if config.Autoscaler.MonitoringEndpoint != "nats.nats-io.svc:8222" || config.Autoscaler.Account != "$G" || !config.Autoscaler.UseHTTPS {
		t.Fatalf("unexpected autoscaler config: %+v", config.Autoscaler)
	}
}
