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
	"sync"
	"time"

	authenticationv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/utils/ptr"

	brokeroidc "knative.dev/eventing-natss/pkg/broker/oidc"
)

const (
	tokenRefreshBefore  = 5 * time.Minute
	tokenRequestTimeout = 5 * time.Second
)

type tokenCacheEntry struct {
	token      string
	expires    time.Time
	lastUsed   time.Time
	refreshing chan struct{}
}

// tokenRequestAudienceSource obtains tokens for the dedicated delivery
// ServiceAccount. It never requests a token for the operational filter
// identity, and the RoleBinding restricts the TokenRequest resource name.
type tokenRequestAudienceSource struct {
	client corev1client.ServiceAccountInterface
	name   string
	uid    types.UID

	mu      sync.Mutex
	entries map[string]*tokenCacheEntry
	now     func() time.Time
}

func newTokenRequestAudienceSource(client corev1client.ServiceAccountInterface, name string, uid types.UID) audienceTokenSource {
	source := &tokenRequestAudienceSource{
		client:  client,
		name:    name,
		uid:     uid,
		entries: make(map[string]*tokenCacheEntry),
		now:     time.Now,
	}
	return source.Token
}

func (s *tokenRequestAudienceSource) Token(ctx context.Context, audience string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if normalized, err := brokeroidc.NormalizeAudiences([]string{audience}); err != nil || len(normalized) != 1 {
		return "", fmt.Errorf("%w (%s)", ErrOIDCTokenUnavailable, brokeroidc.AudienceKey(audience))
	}

	for {
		now := s.now()
		s.mu.Lock()
		entry := s.entries[audience]
		if entry != nil && entry.token != "" && now.Before(entry.expires.Add(-tokenRefreshBefore)) {
			entry.lastUsed = now
			token := entry.token
			s.mu.Unlock()
			return token, nil
		}
		if entry != nil && entry.refreshing != nil {
			ready := entry.refreshing
			s.mu.Unlock()
			select {
			case <-ready:
				continue
			case <-ctx.Done():
				return "", ctx.Err()
			}
		}
		if entry == nil {
			if len(s.entries) >= brokeroidc.MaxAudiences {
				s.evictLeastRecentlyUsedLocked()
			}
			if len(s.entries) >= brokeroidc.MaxAudiences {
				s.mu.Unlock()
				return "", fmt.Errorf("%w: token cache is full", ErrOIDCTokenUnavailable)
			}
			entry = &tokenCacheEntry{}
			s.entries[audience] = entry
		}
		entry.refreshing = make(chan struct{})
		entry.lastUsed = now
		ready := entry.refreshing
		s.mu.Unlock()

		requested := brokeroidc.TokenExpirationSeconds
		requestCtx, cancel := context.WithTimeout(ctx, tokenRequestTimeout)
		response, err := s.client.CreateToken(requestCtx, s.name, &authenticationv1.TokenRequest{
			ObjectMeta: metav1.ObjectMeta{UID: s.uid},
			Spec: authenticationv1.TokenRequestSpec{
				Audiences:         []string{audience},
				ExpirationSeconds: ptr.To(requested),
			},
		}, metav1.CreateOptions{})
		cancel()
		if err == nil && (response == nil || response.Status.Token == "" || response.Status.ExpirationTimestamp.IsZero()) {
			err = fmt.Errorf("token API returned an empty token or expiration")
		}
		if err == nil && !response.Status.ExpirationTimestamp.After(s.now()) {
			err = fmt.Errorf("token API returned an expired token")
		}

		s.mu.Lock()
		if err == nil {
			entry.token = response.Status.Token
			entry.expires = response.Status.ExpirationTimestamp.Time
		} else {
			entry.token = ""
			entry.expires = time.Time{}
		}
		entry.refreshing = nil
		close(ready)
		s.mu.Unlock()

		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return "", ctxErr
			}
			return "", fmt.Errorf("%w (%s): %v", ErrOIDCTokenUnavailable, brokeroidc.AudienceKey(audience), err)
		}
		return response.Status.Token, nil
	}
}

func (s *tokenRequestAudienceSource) evictLeastRecentlyUsedLocked() {
	var oldestAudience string
	var oldest time.Time
	for audience, entry := range s.entries {
		if entry.refreshing != nil {
			continue
		}
		if oldestAudience == "" || entry.lastUsed.Before(oldest) {
			oldestAudience = audience
			oldest = entry.lastUsed
		}
	}
	if oldestAudience != "" {
		delete(s.entries, oldestAudience)
	}
}
