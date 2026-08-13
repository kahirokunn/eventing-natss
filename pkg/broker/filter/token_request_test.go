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
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	authenticationv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"

	brokeroidc "knative.dev/eventing-natss/pkg/broker/oidc"
)

type serviceAccountTokenClient struct {
	corev1client.ServiceAccountInterface
	createToken func(context.Context, string, *authenticationv1.TokenRequest) (*authenticationv1.TokenRequest, error)
}

func (c *serviceAccountTokenClient) CreateToken(ctx context.Context, name string, request *authenticationv1.TokenRequest, _ metav1.CreateOptions) (*authenticationv1.TokenRequest, error) {
	return c.createToken(ctx, name, request)
}

func successfulTokenResponse(token string, expires time.Time) *authenticationv1.TokenRequest {
	return &authenticationv1.TokenRequest{Status: authenticationv1.TokenRequestStatus{
		Token: token, ExpirationTimestamp: metav1.NewTime(expires),
	}}
}

func TestTokenRequestAudienceSourceExactRequestContract(t *testing.T) {
	const (
		serviceAccount = "natsjs-broker-oidc"
		audience       = "https://subscriber.example"
	)
	uid := types.UID("delivery-service-account-uid")
	now := time.Unix(1_700_000_000, 0)
	client := &serviceAccountTokenClient{createToken: func(ctx context.Context, name string, request *authenticationv1.TokenRequest) (*authenticationv1.TokenRequest, error) {
		if name != serviceAccount {
			t.Errorf("CreateToken name = %q, want %q", name, serviceAccount)
		}
		if request.UID != uid {
			t.Errorf("TokenRequest metadata UID = %q, want %q", request.UID, uid)
		}
		if len(request.Spec.Audiences) != 1 || request.Spec.Audiences[0] != audience {
			t.Errorf("TokenRequest audiences = %#v, want [%q]", request.Spec.Audiences, audience)
		}
		if request.Spec.ExpirationSeconds == nil || *request.Spec.ExpirationSeconds != brokeroidc.TokenExpirationSeconds {
			t.Errorf("TokenRequest expirationSeconds = %v, want %d", request.Spec.ExpirationSeconds, brokeroidc.TokenExpirationSeconds)
		}
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > tokenRequestTimeout+100*time.Millisecond {
			t.Errorf("TokenRequest context deadline = %v, want at most %v", deadline, tokenRequestTimeout)
		}
		return successfulTokenResponse("token", now.Add(time.Hour)), nil
	}}
	source := &tokenRequestAudienceSource{client: client, name: serviceAccount, uid: uid, entries: make(map[string]*tokenCacheEntry), now: func() time.Time { return now }}

	got, err := source.Token(context.Background(), audience)
	if err != nil {
		t.Fatal(err)
	}
	if got != "token" {
		t.Errorf("token = %q, want token", got)
	}
}

func TestTokenRequestAudienceSourceSingleflight(t *testing.T) {
	const callers = 20
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	client := &serviceAccountTokenClient{createToken: func(ctx context.Context, _ string, _ *authenticationv1.TokenRequest) (*authenticationv1.TokenRequest, error) {
		if calls.Add(1) == 1 {
			close(started)
		}
		select {
		case <-release:
			return successfulTokenResponse("shared-token", time.Now().Add(time.Hour)), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}}
	source := newTokenRequestAudienceSource(client, brokeroidc.DeliveryServiceAccountName, "uid")
	results := make(chan error, callers)
	for i := 0; i < callers; i++ {
		go func() {
			token, err := source(context.Background(), "https://same.example")
			if err == nil && token != "shared-token" {
				err = fmt.Errorf("token = %q", token)
			}
			results <- err
		}()
	}
	<-started
	time.Sleep(20 * time.Millisecond)
	close(release)
	for i := 0; i < callers; i++ {
		if err := <-results; err != nil {
			t.Error(err)
		}
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("CreateToken calls = %d, want 1", got)
	}
}

func TestTokenRequestAudienceSourceRefreshesFiveMinutesBeforeExpiry(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	var calls int
	client := &serviceAccountTokenClient{createToken: func(context.Context, string, *authenticationv1.TokenRequest) (*authenticationv1.TokenRequest, error) {
		calls++
		return successfulTokenResponse(fmt.Sprintf("token-%d", calls), now.Add(time.Hour)), nil
	}}
	source := &tokenRequestAudienceSource{client: client, name: brokeroidc.DeliveryServiceAccountName, uid: "uid", entries: make(map[string]*tokenCacheEntry), now: func() time.Time { return now }}

	if token, err := source.Token(context.Background(), "audience"); err != nil || token != "token-1" {
		t.Fatalf("initial token = %q, err %v", token, err)
	}
	now = now.Add(54*time.Minute + 59*time.Second)
	if token, err := source.Token(context.Background(), "audience"); err != nil || token != "token-1" {
		t.Fatalf("cached token = %q, err %v", token, err)
	}
	now = now.Add(time.Second)
	if token, err := source.Token(context.Background(), "audience"); err != nil || token != "token-2" {
		t.Fatalf("refreshed token = %q, err %v", token, err)
	}
	if calls != 2 {
		t.Errorf("CreateToken calls = %d, want 2", calls)
	}
}

func TestTokenRequestAudienceSourceDoesNotCacheErrors(t *testing.T) {
	var calls int
	client := &serviceAccountTokenClient{createToken: func(context.Context, string, *authenticationv1.TokenRequest) (*authenticationv1.TokenRequest, error) {
		calls++
		if calls == 1 {
			return nil, errors.New("temporary token API failure")
		}
		return successfulTokenResponse("recovered", time.Now().Add(time.Hour)), nil
	}}
	source := newTokenRequestAudienceSource(client, brokeroidc.DeliveryServiceAccountName, "uid")
	if _, err := source(context.Background(), "audience"); !errors.Is(err, ErrOIDCTokenUnavailable) {
		t.Fatalf("first error = %v, want ErrOIDCTokenUnavailable", err)
	}
	if token, err := source(context.Background(), "audience"); err != nil || token != "recovered" {
		t.Fatalf("retry token = %q, err %v", token, err)
	}
	if calls != 2 {
		t.Errorf("CreateToken calls = %d, want 2", calls)
	}
}

func TestTokenRequestAudienceSourceCacheIsBoundedLRU(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	client := &serviceAccountTokenClient{createToken: func(context.Context, string, *authenticationv1.TokenRequest) (*authenticationv1.TokenRequest, error) {
		return successfulTokenResponse("token", now.Add(time.Hour)), nil
	}}
	source := &tokenRequestAudienceSource{client: client, name: brokeroidc.DeliveryServiceAccountName, uid: "uid", entries: make(map[string]*tokenCacheEntry), now: func() time.Time { return now }}
	for i := 0; i < brokeroidc.MaxAudiences+1; i++ {
		audience := fmt.Sprintf("audience-%02d", i)
		if _, err := source.Token(context.Background(), audience); err != nil {
			t.Fatalf("Token(%q) = %v", audience, err)
		}
		now = now.Add(time.Second)
	}
	if len(source.entries) != brokeroidc.MaxAudiences {
		t.Fatalf("cache entries = %d, want %d", len(source.entries), brokeroidc.MaxAudiences)
	}
	if _, found := source.entries["audience-00"]; found {
		t.Error("least recently used audience was not evicted")
	}
	if _, found := source.entries[fmt.Sprintf("audience-%02d", brokeroidc.MaxAudiences)]; !found {
		t.Error("new audience was not cached")
	}
}

func TestTokenRequestAudienceSourceCancellation(t *testing.T) {
	t.Run("already canceled does not call API", func(t *testing.T) {
		var calls atomic.Int32
		client := &serviceAccountTokenClient{createToken: func(context.Context, string, *authenticationv1.TokenRequest) (*authenticationv1.TokenRequest, error) {
			calls.Add(1)
			return nil, errors.New("unexpected")
		}}
		source := newTokenRequestAudienceSource(client, brokeroidc.DeliveryServiceAccountName, "uid")
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := source(ctx, "audience"); !errors.Is(err, context.Canceled) {
			t.Errorf("error = %v, want context.Canceled", err)
		}
		if calls.Load() != 0 {
			t.Errorf("CreateToken calls = %d, want 0", calls.Load())
		}
	})

	t.Run("waiting singleflight caller cancels independently", func(t *testing.T) {
		started := make(chan struct{})
		release := make(chan struct{})
		client := &serviceAccountTokenClient{createToken: func(ctx context.Context, _ string, _ *authenticationv1.TokenRequest) (*authenticationv1.TokenRequest, error) {
			close(started)
			select {
			case <-release:
				return successfulTokenResponse("token", time.Now().Add(time.Hour)), nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}}
		source := newTokenRequestAudienceSource(client, brokeroidc.DeliveryServiceAccountName, "uid")
		first := make(chan error, 1)
		go func() { _, err := source(context.Background(), "audience"); first <- err }()
		<-started
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := source(ctx, "audience"); !errors.Is(err, context.Canceled) {
			t.Errorf("waiting caller error = %v, want context.Canceled", err)
		}
		close(release)
		if err := <-first; err != nil {
			t.Errorf("refresh owner error = %v", err)
		}
	})

	t.Run("API call receives bounded context", func(t *testing.T) {
		client := &serviceAccountTokenClient{createToken: func(ctx context.Context, _ string, _ *authenticationv1.TokenRequest) (*authenticationv1.TokenRequest, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		}}
		source := newTokenRequestAudienceSource(client, brokeroidc.DeliveryServiceAccountName, "uid")
		started := time.Now()
		_, err := source(context.Background(), "audience")
		if !errors.Is(err, ErrOIDCTokenUnavailable) {
			t.Errorf("error = %v, want ErrOIDCTokenUnavailable", err)
		}
		elapsed := time.Since(started)
		if elapsed < tokenRequestTimeout-500*time.Millisecond || elapsed > tokenRequestTimeout+2*time.Second {
			t.Errorf("bounded request duration = %v, want about %v", elapsed, tokenRequestTimeout)
		}
	})
}

func TestTokenRequestAudienceSourceRejectsInvalidAudience(t *testing.T) {
	var calls atomic.Int32
	client := &serviceAccountTokenClient{createToken: func(context.Context, string, *authenticationv1.TokenRequest) (*authenticationv1.TokenRequest, error) {
		calls.Add(1)
		return nil, errors.New("unexpected")
	}}
	source := newTokenRequestAudienceSource(client, brokeroidc.DeliveryServiceAccountName, "uid")
	for _, audience := range []string{"", string(make([]byte, brokeroidc.MaxAudienceBytes+1))} {
		if _, err := source(context.Background(), audience); !errors.Is(err, ErrOIDCTokenUnavailable) {
			t.Errorf("Token(%q) error = %v, want ErrOIDCTokenUnavailable", audience, err)
		}
	}
	if calls.Load() != 0 {
		t.Errorf("CreateToken calls = %d, want 0", calls.Load())
	}
}
