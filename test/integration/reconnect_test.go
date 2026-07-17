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
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/nacos-group/nacos-sdk-go/v3/clients"
	"github.com/nacos-group/nacos-sdk-go/v3/vo"
)

// TestIntegrationReconnect restarts the Nacos container and verifies the
// client reconnects and serves config reads again (covering guidance #4
// from the roadmap discussion: connection setup + reconnect coverage).
// It runs last in the package (file names sort after integration_test.go),
// so the restart does not disturb the other smoke tests.
func TestIntegrationReconnect(t *testing.T) {
	container := os.Getenv("NACOS_CONTAINER_NAME")
	if container == "" {
		t.Skip("NACOS_CONTAINER_NAME not set; skipping reconnect test")
	}

	client, err := clients.NewConfigClient(clientParam(t))
	require.NoError(t, err, "create config client")
	defer client.CloseClient()

	dataId := fmt.Sprintf("it-reconnect-%d", time.Now().UnixNano())
	content := "reconnect-payload"
	ok, err := client.PublishConfig(vo.ConfigParam{DataId: dataId, Group: testGroup, Content: content})
	require.NoError(t, err, "publish before restart")
	require.True(t, ok)

	out, err := exec.Command("docker", "restart", container).CombinedOutput()
	require.NoError(t, err, "docker restart %s: %s", container, out)

	// Nacos standalone needs tens of seconds to come back; the gRPC client
	// must detect the broken stream and reconnect on its own.
	deadline := time.Now().Add(180 * time.Second)
	for time.Now().Before(deadline) {
		if got, err := client.GetConfig(vo.ConfigParam{DataId: dataId, Group: testGroup}); err == nil && got == content {
			_, _ = client.DeleteConfig(vo.ConfigParam{DataId: dataId, Group: testGroup})
			return
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatal("client did not recover within 180s after server restart")
}
