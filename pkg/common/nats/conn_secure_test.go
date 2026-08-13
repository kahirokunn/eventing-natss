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
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"testing"
	"time"

	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubefake "k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
	"knative.dev/pkg/injection"

	natstesting "knative.dev/eventing-natss/pkg/channel/jetstream/dispatcher/testing"
	commonconfig "knative.dev/eventing-natss/pkg/common/config"
)

func testCertificate(t *testing.T) (certificatePEM, privateKeyPEM []byte) {
	t.Helper()
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "nats-client"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment | x509.KeyUsageCertSign,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		IsCA:         true,
		DNSNames:     []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &privateKey.PublicKey, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	certificatePEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	privateKeyPEM = pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(privateKey)})
	return certificatePEM, privateKeyPEM
}

func TestBuildAuthOptionsFromCredentialAndMTLSSecrets(t *testing.T) {
	certificatePEM, privateKeyPEM := testCertificate(t)
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
	client := kubefake.NewSimpleClientset(
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "credentials", Namespace: "knative-eventing"},
			Data:       map[string][]byte{"custom.creds": credentialFile},
		},
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "client-tls", Namespace: "knative-eventing"},
			Type:       corev1.SecretTypeTLS,
			Data: map[string][]byte{
				corev1.TLSCertKey:       certificatePEM,
				corev1.TLSPrivateKeyKey: privateKeyPEM,
				TLSCaCertKey:            certificatePEM,
			},
		},
	)
	secrets := client.CoreV1().Secrets("knative-eventing")
	options, err := buildAuthOption(context.Background(), commonconfig.ENConfigAuth{
		CredentialFile: &commonconfig.ENConfigAuthCredentialFile{Secret: &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: "credentials"},
			Key:                  "custom.creds",
		}},
		TLS: &commonconfig.ENConfigAuthTLS{Secret: &corev1.LocalObjectReference{Name: "client-tls"}},
	}, secrets)
	if err != nil {
		t.Fatal(err)
	}
	if len(options) != 2 {
		t.Fatalf("auth option count = %d, want 2", len(options))
	}
	natsOptions := natsgo.GetDefaultOptions()
	for index, option := range options {
		if err := option(&natsOptions); err != nil {
			t.Fatalf("auth option %d: %v", index, err)
		}
	}
	if natsOptions.UserJWT == nil || natsOptions.SignatureCB == nil {
		t.Error("credential file did not configure UserJWT and signature callbacks")
	}
	jwt, err := natsOptions.UserJWT()
	if err != nil || jwt != "test.jwt.value" {
		t.Errorf("UserJWT() = %q, %v, want test JWT", jwt, err)
	}
	if signature, err := natsOptions.SignatureCB([]byte("nonce")); err != nil || len(signature) == 0 {
		t.Errorf("SignatureCB() returned %d bytes, %v", len(signature), err)
	}
	if natsOptions.TLSConfig == nil || len(natsOptions.TLSConfig.Certificates) != 1 {
		t.Fatalf("mTLS client certificates = %#v, want one", natsOptions.TLSConfig)
	}
	if natsOptions.TLSConfig.MinVersion != 0 && natsOptions.TLSConfig.MinVersion < tls.VersionTLS12 {
		t.Errorf("TLS minimum version = %#x, want TLS 1.2 or newer", natsOptions.TLSConfig.MinVersion)
	}
	if natsOptions.TLSConfig.RootCAs == nil {
		t.Error("CA bundled with mTLS secret was not installed")
	}
}

func TestBuildRootCAOptionFromSecretAndBundle(t *testing.T) {
	certificatePEM, _ := testCertificate(t)
	client := kubefake.NewSimpleClientset(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "root-ca", Namespace: "knative-eventing"},
		Data:       map[string][]byte{TLSCaCertKey: certificatePEM},
	})

	tests := []struct {
		name   string
		config commonconfig.ENConfigRootCA
	}{
		{
			name: "secret",
			config: commonconfig.ENConfigRootCA{Secret: &corev1.LocalObjectReference{
				Name: "root-ca",
			}},
		},
		{
			name:   "CA bundle",
			config: commonconfig.ENConfigRootCA{CABundle: base64.StdEncoding.EncodeToString(certificatePEM)},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			option, err := buildRootCAOption(context.Background(), test.config, client.CoreV1().Secrets("knative-eventing"))
			if err != nil {
				t.Fatal(err)
			}
			natsOptions := natsgo.GetDefaultOptions()
			if err := option(&natsOptions); err != nil {
				t.Fatal(err)
			}
			if natsOptions.TLSConfig == nil || natsOptions.TLSConfig.RootCAs == nil {
				t.Fatal("root CA option did not configure a certificate pool")
			}
		})
	}
}

func TestSecureOptionErrorsPreserveCategory(t *testing.T) {
	secrets := kubefake.NewSimpleClientset().CoreV1().Secrets("knative-eventing")
	_, err := buildAuthOption(context.Background(), commonconfig.ENConfigAuth{
		CredentialFile: &commonconfig.ENConfigAuthCredentialFile{Secret: &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: "missing"},
		}},
	}, secrets)
	if err == nil || !errors.Is(err, ErrBadCredentialFileOption) {
		t.Errorf("credential error = %v, want category %v", err, ErrBadCredentialFileOption)
	}
	_, err = buildAuthOption(context.Background(), commonconfig.ENConfigAuth{
		TLS: &commonconfig.ENConfigAuthTLS{Secret: &corev1.LocalObjectReference{Name: "missing"}},
	}, secrets)
	if err == nil || !errors.Is(err, ErrBadMTLSOption) {
		t.Errorf("mTLS error = %v, want category %v", err, ErrBadMTLSOption)
	}
}

func TestNewNatsConnWithSecretsAppliesConnectionOptions(t *testing.T) {
	server := natstesting.RunBasicJetstreamServer()
	defer natstesting.ShutdownJSServerAndRemoveStorage(t, server)

	config := commonconfig.EventingNatsConfig{
		URL: server.ClientURL(),
		ConnOpts: &commonconfig.ConnOpts{
			RetryOnFailedConnect:           true,
			MaxReconnects:                  17,
			ReconnectWaitMilliseconds:      250,
			ReconnectJitterMilliseconds:    75,
			ReconnectJitterTLSMilliseconds: 125,
		},
	}
	secrets := kubefake.NewSimpleClientset().CoreV1().Secrets("knative-eventing")
	closed := false
	connection, err := NewNatsConnWithSecrets(context.Background(), config, secrets,
		natsgo.CustomReconnectDelay(func(int) time.Duration { return 999 * time.Millisecond }),
		natsgo.ClosedHandler(func(*natsgo.Conn) { closed = true }),
	)
	if err != nil {
		t.Fatal(err)
	}

	if !connection.Opts.RetryOnFailedConnect {
		t.Error("RetryOnFailedConnect = false, want true")
	}
	if got, want := connection.Opts.MaxReconnect, 17; got != want {
		t.Errorf("MaxReconnect = %d, want %d", got, want)
	}
	if got, want := connection.Opts.ReconnectWait, 250*time.Millisecond; got != want {
		t.Errorf("ReconnectWait = %v, want %v", got, want)
	}
	if got, want := connection.Opts.ReconnectJitter, 75*time.Millisecond; got != want {
		t.Errorf("ReconnectJitter = %v, want %v", got, want)
	}
	if got, want := connection.Opts.ReconnectJitterTLS, 125*time.Millisecond; got != want {
		t.Errorf("ReconnectJitterTLS = %v, want %v", got, want)
	}
	if connection.Opts.CustomReconnectDelayCB == nil {
		t.Error("CustomReconnectDelayCB was not configured")
	} else if got, want := connection.Opts.CustomReconnectDelayCB(1), 999*time.Millisecond; got != want {
		t.Errorf("additional CustomReconnectDelay = %v, want %v", got, want)
	}

	closedHandler := connection.ClosedHandler()
	if closedHandler == nil {
		t.Fatal("additional ClosedHandler was not configured")
	}
	closedHandler(connection)
	if !closed {
		t.Error("additional ClosedHandler did not override the fatal default callback")
	}
	connection.SetClosedHandler(nil)
	connection.Close()
}

func TestNewNatsConnWithSecretsUsesSuppliedSecretNamespace(t *testing.T) {
	client := kubefake.NewSimpleClientset()
	config := commonconfig.EventingNatsConfig{Auth: &commonconfig.ENConfigAuth{
		CredentialFile: &commonconfig.ENConfigAuthCredentialFile{Secret: &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: "credentials"},
		}},
	}}

	_, err := NewNatsConnWithSecrets(context.Background(), config,
		client.CoreV1().Secrets("knative-eventing"))
	if err == nil || !errors.Is(err, ErrBadCredentialFileOption) {
		t.Fatalf("NewNatsConnWithSecrets() error = %v, want category %v", err, ErrBadCredentialFileOption)
	}

	var secretGets []clienttesting.GetAction
	for _, action := range client.Actions() {
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
	if got, want := secretGets[0].GetName(), "credentials"; got != want {
		t.Errorf("Secret name = %q, want %q", got, want)
	}
}

func TestCompatibilityNamespaceSelection(t *testing.T) {
	t.Setenv("SYSTEM_NAMESPACE", "knative-eventing")
	if got, want := getNamespace(context.Background()), "knative-eventing"; got != want {
		t.Errorf("unscoped namespace = %q, want %q", got, want)
	}

	ctx := injection.WithNamespaceScope(context.Background(), "tenant")
	if got, want := getNamespace(ctx), "tenant"; got != want {
		t.Errorf("scoped namespace = %q, want %q", got, want)
	}
}
