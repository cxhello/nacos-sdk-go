/*
 * Copyright 1999-2020 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package security

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/nacos-group/nacos-sdk-go/v3/common/constant"
	"github.com/stretchr/testify/assert"
)

// Compile-time assertion that both auth clients satisfy AuthClient.
var (
	_ AuthClient = (*NacosAuthClient)(nil)
	_ AuthClient = (*RamAuthClient)(nil)
)

// stubRamProvider is a RamCredentialProvider whose Init() fails the first
// failTimes calls and succeeds afterwards, counting both Init() and
// credential lookups so tests can observe the initialization lifecycle.
type stubRamProvider struct {
	mux       sync.Mutex
	matches   bool
	failTimes int
	initCalls int
	credCalls int
}

func (s *stubRamProvider) MatchProvider() bool { return s.matches }

func (s *stubRamProvider) Init() error {
	s.mux.Lock()
	defer s.mux.Unlock()
	s.initCalls++
	if s.initCalls <= s.failTimes {
		return errors.New("stub provider init failed")
	}
	return nil
}

func (s *stubRamProvider) GetCredentialsForNacosClient() RamContext {
	s.mux.Lock()
	defer s.mux.Unlock()
	s.credCalls++
	return RamContext{AccessKey: "ak", SecretKey: "sk"}
}

func (s *stubRamProvider) inits() int {
	s.mux.Lock()
	defer s.mux.Unlock()
	return s.initCalls
}

func (s *stubRamProvider) creds() int {
	s.mux.Lock()
	defer s.mux.Unlock()
	return s.credCalls
}

func newStubbedRamClient(provider RamCredentialProvider, retry time.Duration) *RamAuthClient {
	client := NewRamAuthClient(constant.ClientConfig{})
	client.ramCredentialProviders = []RamCredentialProvider{provider}
	client.initRetryDelay = retry
	return client
}

func TestRamAuthClient_AutoRefresh_NoProviderIsNoOp(t *testing.T) {
	client := NewRamAuthClient(constant.ClientConfig{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Nothing matched, so there is nothing to retry: must not panic or block.
	client.AutoRefresh(ctx)
}

func TestRamAuthClient_UninitializedProviderIsNotExposed(t *testing.T) {
	provider := &stubRamProvider{matches: true, failTimes: 1}
	client := newStubbedRamClient(provider, 20*time.Millisecond)

	success, err := client.Login()
	assert.False(t, success)
	assert.Error(t, err)
	assert.Equal(t, 1, provider.inits())

	client.GetSecurityInfo(BuildNamingResource("ns", "group", "svc"))
	assert.Equal(t, 0, provider.creds(),
		"a provider whose Init failed must not be consulted for credentials")
}

func TestRamAuthClient_AutoRefresh_RetriesFailedInit(t *testing.T) {
	provider := &stubRamProvider{matches: true, failTimes: 1}
	client := newStubbedRamClient(provider, 20*time.Millisecond)

	success, err := client.Login()
	assert.False(t, success)
	assert.Error(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client.AutoRefresh(ctx)

	assert.Eventually(t, func() bool { return provider.inits() >= 2 },
		3*time.Second, 10*time.Millisecond, "a failed Init must be retried")

	assert.Eventually(t, func() bool {
		client.GetSecurityInfo(BuildNamingResource("ns", "group", "svc"))
		return provider.creds() > 0
	}, 3*time.Second, 20*time.Millisecond,
		"after Init finally succeeds the provider must be usable")
}

func TestRamAuthClient_AutoRefresh_StopsAfterSuccessfulInit(t *testing.T) {
	provider := &stubRamProvider{matches: true} // Init succeeds on the first call
	client := newStubbedRamClient(provider, 20*time.Millisecond)

	success, err := client.Login()
	assert.True(t, success)
	assert.NoError(t, err)
	assert.Equal(t, 1, provider.inits())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client.AutoRefresh(ctx)

	time.Sleep(200 * time.Millisecond) // several retry intervals
	assert.Equal(t, 1, provider.inits(),
		"Init must not run again once initialization has succeeded")
}

func TestRamAuthClient_AutoRefresh_StopsOnContextCancel(t *testing.T) {
	provider := &stubRamProvider{matches: true, failTimes: 1 << 30} // never succeeds
	client := newStubbedRamClient(provider, 20*time.Millisecond)

	_, err := client.Login()
	assert.Error(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	client.AutoRefresh(ctx)
	time.Sleep(100 * time.Millisecond) // let it retry a few times
	cancel()

	time.Sleep(60 * time.Millisecond) // let any in-flight retry finish
	settled := provider.inits()
	time.Sleep(200 * time.Millisecond)
	assert.Equal(t, settled, provider.inits(),
		"retries must stop once the context is cancelled")
}
