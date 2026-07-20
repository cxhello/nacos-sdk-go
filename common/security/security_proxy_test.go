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
	"testing"

	"github.com/nacos-group/nacos-sdk-go/v3/common/constant"
	"github.com/stretchr/testify/assert"
)

// stubAuthClient is a minimal AuthClient for exercising SecurityProxy
// aggregation and delegation logic.
type stubAuthClient struct {
	loginErr    error
	refreshedCh chan struct{}
}

func (s *stubAuthClient) Login() (bool, error) {
	return s.loginErr == nil, s.loginErr
}
func (s *stubAuthClient) AutoRefresh(ctx context.Context) {
	if s.refreshedCh != nil {
		close(s.refreshedCh)
	}
}
func (s *stubAuthClient) GetSecurityInfo(resource RequestResource) map[string]string {
	return map[string]string{}
}
func (s *stubAuthClient) UpdateServerList(serverList []constant.ServerConfig) {}

func TestSecurityProxy_Login_ReturnsCredentialError(t *testing.T) {
	credErr := classifyLoginStatus(401, "unknown user!")
	sp := SecurityProxy{Clients: []AuthClient{
		&stubAuthClient{loginErr: errors.New("network down")}, // transient, not returned
		&stubAuthClient{loginErr: credErr},                    // credential, returned
	}}
	err := sp.Login()
	assert.Error(t, err)
	assert.True(t, errors.Is(err, ErrLoginFailed))
}

func TestSecurityProxy_Login_AllSuccess(t *testing.T) {
	sp := SecurityProxy{Clients: []AuthClient{
		&stubAuthClient{loginErr: nil},
		&stubAuthClient{loginErr: nil},
	}}
	assert.NoError(t, sp.Login())
}

func TestSecurityProxy_Login_TransientOnlyIsNil(t *testing.T) {
	sp := SecurityProxy{Clients: []AuthClient{
		&stubAuthClient{loginErr: errors.New("connection refused")},
	}}
	// Transient errors must not surface as a fail-fast credential error.
	assert.NoError(t, sp.Login())
}

func TestSecurityProxy_AutoRefresh_DelegatesToClients(t *testing.T) {
	ch := make(chan struct{})
	sp := SecurityProxy{Clients: []AuthClient{&stubAuthClient{refreshedCh: ch}}}
	sp.AutoRefresh(context.Background())
	select {
	case <-ch:
	default:
		t.Fatal("AutoRefresh should delegate to each client's AutoRefresh")
	}
}
