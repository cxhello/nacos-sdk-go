//go:build integration

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

package integration

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nacos-group/nacos-sdk-go/v3/clients"
	"github.com/nacos-group/nacos-sdk-go/v3/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v3/vo"
)

// TestIntegrationAuth_WrongPasswordFailFast verifies that with
// FailOnAuthError enabled, a wrong password makes NewConfigClient return an
// error instead of silently constructing an unusable client (#670).
func TestIntegrationAuth_WrongPasswordFailFast(t *testing.T) {
	port, err := strconv.ParseUint(envOr("NACOS_SERVER_PORT", "8848"), 10, 64)
	require.NoError(t, err, "invalid NACOS_SERVER_PORT")

	sc := []constant.ServerConfig{
		*constant.NewServerConfig(envOr("NACOS_SERVER_IP", "127.0.0.1"), port,
			constant.WithContextPath("/nacos")),
	}
	cc := *constant.NewClientConfig(
		constant.WithNamespaceId(""),
		constant.WithUsername(envOr("NACOS_USERNAME", "nacos")),
		constant.WithPassword("definitely-wrong-password"),
		constant.WithTimeoutMs(10000),
		constant.WithNotLoadCacheAtStart(true),
		constant.WithFailOnAuthError(true),
		constant.WithLogDir(t.TempDir()),
		constant.WithCacheDir(t.TempDir()),
		constant.WithLogLevel("warn"),
	)

	_, err = clients.NewConfigClient(vo.NacosClientParam{
		ClientConfig:  &cc,
		ServerConfigs: sc,
	})
	assert.Error(t, err, "wrong password with FailOnAuthError must fail NewConfigClient")
}

// TestIntegrationAuth_WrongPasswordDefaultNoFailFast verifies backward-compatible
// behavior: without FailOnAuthError, construction does not abort on bad
// credentials (the error only surfaces later on business requests).
func TestIntegrationAuth_WrongPasswordDefaultNoFailFast(t *testing.T) {
	port, err := strconv.ParseUint(envOr("NACOS_SERVER_PORT", "8848"), 10, 64)
	require.NoError(t, err, "invalid NACOS_SERVER_PORT")

	sc := []constant.ServerConfig{
		*constant.NewServerConfig(envOr("NACOS_SERVER_IP", "127.0.0.1"), port,
			constant.WithContextPath("/nacos")),
	}
	cc := *constant.NewClientConfig(
		constant.WithNamespaceId(""),
		constant.WithUsername(envOr("NACOS_USERNAME", "nacos")),
		constant.WithPassword("definitely-wrong-password"),
		constant.WithTimeoutMs(10000),
		constant.WithNotLoadCacheAtStart(true),
		constant.WithLogDir(t.TempDir()),
		constant.WithCacheDir(t.TempDir()),
		constant.WithLogLevel("warn"),
	)

	client, err := clients.NewConfigClient(vo.NacosClientParam{
		ClientConfig:  &cc,
		ServerConfigs: sc,
	})
	assert.NoError(t, err, "default behavior must not abort construction on bad credentials")
	assert.NotNil(t, client)
}
