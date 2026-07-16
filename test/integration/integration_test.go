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

// Package integration contains smoke tests that run against a real Nacos
// server over gRPC. They are executed by the CI integration jobs against
// both Nacos 2.x and Nacos 3.x:
//
//	go test -tags=integration ./... -run TestIntegration
//
// The target server defaults to 127.0.0.1:8848 with the default
// nacos/nacos credentials and can be overridden via the environment
// variables NACOS_SERVER_IP, NACOS_SERVER_PORT, NACOS_USERNAME and
// NACOS_PASSWORD.
package integration

import (
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nacos-group/nacos-sdk-go/v3/clients"
	"github.com/nacos-group/nacos-sdk-go/v3/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v3/vo"
)

const (
	testGroup   = "it-group"
	testService = "it-service"

	// eventual consistency budget for server-side propagation
	waitTimeout  = 30 * time.Second
	waitInterval = 500 * time.Millisecond
)

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func clientParam(t *testing.T) vo.NacosClientParam {
	t.Helper()
	port, err := strconv.ParseUint(envOr("NACOS_SERVER_PORT", "8848"), 10, 64)
	require.NoError(t, err, "invalid NACOS_SERVER_PORT")

	sc := []constant.ServerConfig{
		*constant.NewServerConfig(envOr("NACOS_SERVER_IP", "127.0.0.1"), port,
			constant.WithContextPath("/nacos")),
	}
	cc := *constant.NewClientConfig(
		constant.WithNamespaceId(""),
		constant.WithUsername(envOr("NACOS_USERNAME", "nacos")),
		constant.WithPassword(envOr("NACOS_PASSWORD", "nacos")),
		constant.WithTimeoutMs(10000),
		constant.WithNotLoadCacheAtStart(true),
		// let the deregister-to-empty push reach the local cache,
		// otherwise the SDK's empty-list protection keeps stale instances
		constant.WithUpdateCacheWhenEmpty(true),
		constant.WithLogDir(t.TempDir()),
		constant.WithCacheDir(t.TempDir()),
		constant.WithLogLevel("warn"),
	)
	return vo.NacosClientParam{
		ClientConfig:  &cc,
		ServerConfigs: sc,
	}
}

// eventually polls fn until it returns true or the timeout elapses.
func eventually(t *testing.T, msg string, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(waitTimeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(waitInterval)
	}
	t.Fatalf("timed out after %s waiting for: %s", waitTimeout, msg)
}

// TestIntegrationConfig verifies the config publish/get/remove round trip
// over the gRPC channel.
func TestIntegrationConfig(t *testing.T) {
	client, err := clients.NewConfigClient(clientParam(t))
	require.NoError(t, err, "create config client")
	defer client.CloseClient()

	dataId := fmt.Sprintf("it-config-%d", time.Now().UnixNano())
	content := "integration-smoke-content"

	ok, err := client.PublishConfig(vo.ConfigParam{
		DataId:  dataId,
		Group:   testGroup,
		Content: content,
	})
	require.NoError(t, err, "publish config")
	require.True(t, ok, "publish config should return true")

	eventually(t, "published config becomes readable", func() bool {
		got, err := client.GetConfig(vo.ConfigParam{DataId: dataId, Group: testGroup})
		return err == nil && got == content
	})

	ok, err = client.DeleteConfig(vo.ConfigParam{DataId: dataId, Group: testGroup})
	require.NoError(t, err, "delete config")
	require.True(t, ok, "delete config should return true")

	eventually(t, "deleted config becomes unreadable", func() bool {
		got, _ := client.GetConfig(vo.ConfigParam{DataId: dataId, Group: testGroup})
		return got == ""
	})
}

// TestIntegrationConfigListen verifies that a config listener receives the
// updated content pushed by the server.
func TestIntegrationConfigListen(t *testing.T) {
	client, err := clients.NewConfigClient(clientParam(t))
	require.NoError(t, err, "create config client")
	defer client.CloseClient()

	dataId := fmt.Sprintf("it-listen-%d", time.Now().UnixNano())
	updated := make(chan string, 1)

	err = client.ListenConfig(vo.ConfigParam{
		DataId: dataId,
		Group:  testGroup,
		OnChange: func(namespace, group, dataId, data string) {
			select {
			case updated <- data:
			default:
			}
		},
	})
	require.NoError(t, err, "listen config")

	ok, err := client.PublishConfig(vo.ConfigParam{
		DataId:  dataId,
		Group:   testGroup,
		Content: "listen-v1",
	})
	require.NoError(t, err, "publish config")
	require.True(t, ok, "publish config should return true")

	select {
	case data := <-updated:
		assert.Equal(t, "listen-v1", data)
	case <-time.After(waitTimeout):
		t.Fatalf("timed out after %s waiting for config change notification", waitTimeout)
	}

	_ = client.CancelListenConfig(vo.ConfigParam{DataId: dataId, Group: testGroup})
	_, _ = client.DeleteConfig(vo.ConfigParam{DataId: dataId, Group: testGroup})
}

// TestIntegrationNaming verifies the instance register/discover/deregister
// round trip over the gRPC channel.
func TestIntegrationNaming(t *testing.T) {
	client, err := clients.NewNamingClient(clientParam(t))
	require.NoError(t, err, "create naming client")
	defer client.CloseClient()

	instanceIp := "10.0.0.10"
	var instancePort uint64 = 18080

	ok, err := client.RegisterInstance(vo.RegisterInstanceParam{
		Ip:          instanceIp,
		Port:        instancePort,
		ServiceName: testService,
		GroupName:   testGroup,
		Weight:      1,
		Enable:      true,
		Healthy:     true,
		Ephemeral:   true,
		Metadata:    map[string]string{"idc": "integration"},
	})
	require.NoError(t, err, "register instance")
	require.True(t, ok, "register instance should return true")

	eventually(t, "registered instance becomes discoverable", func() bool {
		instances, err := client.SelectInstances(vo.SelectInstancesParam{
			ServiceName: testService,
			GroupName:   testGroup,
			HealthyOnly: true,
		})
		if err != nil {
			return false
		}
		for _, ins := range instances {
			if ins.Ip == instanceIp && ins.Port == instancePort {
				return true
			}
		}
		return false
	})

	ok, err = client.DeregisterInstance(vo.DeregisterInstanceParam{
		Ip:          instanceIp,
		Port:        instancePort,
		ServiceName: testService,
		GroupName:   testGroup,
		Ephemeral:   true,
	})
	require.NoError(t, err, "deregister instance")
	require.True(t, ok, "deregister instance should return true")

	eventually(t, "deregistered instance disappears", func() bool {
		instances, err := client.SelectInstances(vo.SelectInstancesParam{
			ServiceName: testService,
			GroupName:   testGroup,
			HealthyOnly: true,
		})
		if err != nil {
			return true
		}
		for _, ins := range instances {
			if ins.Ip == instanceIp && ins.Port == instancePort {
				return false
			}
		}
		return true
	})
}
