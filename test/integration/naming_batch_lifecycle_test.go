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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nacos-group/nacos-sdk-go/v3/clients"
	"github.com/nacos-group/nacos-sdk-go/v3/model"
	"github.com/nacos-group/nacos-sdk-go/v3/vo"
)

// hasPort reports whether hosts contains an instance bound to port.
func hasPort(hosts []model.Instance, port uint64) bool {
	for _, h := range hosts {
		if h.Port == port {
			return true
		}
	}
	return false
}

// TestIntegrationNamingBatchLifecycle walks a BatchRegisterInstance
// publication through its whole life on a real server: shrinking via
// DeregisterInstance republishes the retained set (batch shape, not the
// single-instance path), an unknown member is a harmless no-op republish of
// the same set, and removing every member clears the publication down to an
// empty (but non-nil) batch rather than reverting to single shape. It then
// restarts the server and confirms redo replays the batch shape it holds at
// the time of the restart (#780), not just the most recent instance (#866).
func TestIntegrationNamingBatchLifecycle(t *testing.T) {
	client, err := clients.NewNamingClient(clientParam(t))
	require.NoError(t, err, "create naming client")
	defer client.CloseClient()

	serviceName := fmt.Sprintf("it-batch-lifecycle-%d-%d", time.Now().UnixNano(), os.Getpid())
	const ip = "10.0.0.40"
	portA, portB, portC := uint64(19101), uint64(19102), uint64(19103)

	batchRegister := func(ports ...uint64) {
		t.Helper()
		instances := make([]vo.RegisterInstanceParam, 0, len(ports))
		for _, port := range ports {
			instances = append(instances, vo.RegisterInstanceParam{
				Ip:          ip,
				Port:        port,
				ServiceName: serviceName,
				GroupName:   testGroup,
				Weight:      1,
				Enable:      true,
				Healthy:     true,
				Ephemeral:   true,
			})
		}
		ok, err := client.BatchRegisterInstance(vo.BatchRegisterInstanceParam{
			ServiceName: serviceName,
			GroupName:   testGroup,
			Instances:   instances,
		})
		require.NoError(t, err, "batch register instances %v", ports)
		require.True(t, ok, "batch register instances %v should return true", ports)
	}
	deregister := func(port uint64) {
		t.Helper()
		ok, err := client.DeregisterInstance(vo.DeregisterInstanceParam{
			Ip:          ip,
			Port:        port,
			ServiceName: serviceName,
			GroupName:   testGroup,
			Ephemeral:   true,
		})
		require.NoError(t, err, "deregister instance on port %d", port)
		require.True(t, ok, "deregister instance on port %d should return true", port)
	}
	defer func() {
		_, _ = client.BatchRegisterInstance(vo.BatchRegisterInstanceParam{
			ServiceName: serviceName,
			GroupName:   testGroup,
			Instances:   []vo.RegisterInstanceParam{},
		})
	}()

	// 1. BatchRegisterInstance(A,B,C) -> 3 hosts.
	batchRegister(portA, portB, portC)
	assert.Eventually(t, func() bool {
		svc, err := client.GetService(vo.GetServiceParam{ServiceName: serviceName, GroupName: testGroup})
		return err == nil && len(svc.Hosts) == 3
	}, waitTimeout, waitInterval, "all 3 batch-registered instances should become discoverable")

	// 2. DeregisterInstance(B) shrinks the batch via republish of the
	// retained set {A, C} -> 2 hosts.
	deregister(portB)
	assert.Eventually(t, func() bool {
		svc, err := client.GetService(vo.GetServiceParam{ServiceName: serviceName, GroupName: testGroup})
		if err != nil || len(svc.Hosts) != 2 {
			return false
		}
		return hasPort(svc.Hosts, portA) && hasPort(svc.Hosts, portC) && !hasPort(svc.Hosts, portB)
	}, waitTimeout, waitInterval, "deregistering B should republish the retained {A, C} batch, leaving 2 hosts")

	// 3. DeregisterInstance(unknown) is a no-op against the retained set: it
	// still republishes {A, C} unchanged (ok=true), it must NOT be
	// misinterpreted as clearing or shrinking anything further.
	ok, err := client.DeregisterInstance(vo.DeregisterInstanceParam{
		Ip:          "10.0.0.99",
		Port:        29999,
		ServiceName: serviceName,
		GroupName:   testGroup,
		Ephemeral:   true,
	})
	require.NoError(t, err, "deregister unknown instance")
	require.True(t, ok, "deregister unknown instance should still return true (republish of the unchanged set)")
	assert.Eventually(t, func() bool {
		svc, err := client.GetService(vo.GetServiceParam{ServiceName: serviceName, GroupName: testGroup})
		if err != nil || len(svc.Hosts) != 2 {
			return false
		}
		return hasPort(svc.Hosts, portA) && hasPort(svc.Hosts, portC)
	}, waitTimeout, waitInterval, "deregistering an unknown member should leave {A, C} untouched")

	// 4. Deregistering the remaining members one at a time drains the batch
	// to an empty (non-nil) republish, which clears the publication down to
	// 0 hosts -- the #901-2 semantics: an empty batch is a legitimate,
	// distinct state from "never registered" or "single shape", and must
	// clear the server-side publication rather than leaving stale hosts.
	deregister(portA)
	deregister(portC)
	assert.Eventually(t, func() bool {
		svc, err := client.GetService(vo.GetServiceParam{ServiceName: serviceName, GroupName: testGroup})
		return err == nil && len(svc.Hosts) == 0
	}, waitTimeout, waitInterval, "draining every batch member should clear the publication to 0 hosts")

	// 5. Re-publish a fresh batch {A, B} and, if a container name was
	// provided, restart the server to replay #780: redo must resend the
	// CURRENT batch shape it holds (a BatchInstanceRequest for {A, B}), not
	// collapse to a single-instance shape or replay a stale prior batch.
	batchRegister(portA, portB)
	assert.Eventually(t, func() bool {
		svc, err := client.GetService(vo.GetServiceParam{ServiceName: serviceName, GroupName: testGroup})
		return err == nil && len(svc.Hosts) == 2 && hasPort(svc.Hosts, portA) && hasPort(svc.Hosts, portB)
	}, waitTimeout, waitInterval, "re-published {A, B} batch should become discoverable before the restart")

	container := os.Getenv("NACOS_CONTAINER_NAME")
	if container == "" {
		t.Log("NACOS_CONTAINER_NAME not set; skipping redo-replay-after-restart assertion")
		return
	}

	out, err := exec.Command("docker", "restart", container).CombinedOutput()
	require.NoError(t, err, "docker restart %s: %s", container, out)

	// GetService/SelectInstances serve from the local subscription cache
	// once a service has been seen once, so re-polling with the ORIGINAL
	// client would only prove its in-memory cache still holds the
	// pre-restart value, not that the server-side redo actually replayed
	// anything. Verify with a brand-new client whose first successful read
	// is a genuine subscribe/query against the server's post-redo state.
	//
	// 3.x restarts are noticeably slower than 2.x to become ready for new
	// connections/subscriptions, hence the generous 180s budget below.
	verifyClient, err := clients.NewNamingClient(clientParam(t))
	require.NoError(t, err, "create verification naming client")
	defer verifyClient.CloseClient()

	deadline := time.Now().Add(180 * time.Second)
	for time.Now().Before(deadline) {
		svc, err := verifyClient.GetService(vo.GetServiceParam{ServiceName: serviceName, GroupName: testGroup})
		if err == nil && len(svc.Hosts) == 2 && hasPort(svc.Hosts, portA) && hasPort(svc.Hosts, portB) {
			return
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatal("redo did not replay the {A, B} batch shape within 180s after server restart")
}

// TestIntegrationNamingSingleInstanceReplaces documents #866's underlying
// server behavior: the server keeps exactly one publication per
// (connection, service) for the plain (non-batch) registerInstance call, so
// a second RegisterInstance for the same service REPLACES the first rather
// than adding to it. Callers that need several instances of one service
// published from a single client must use BatchRegisterInstance instead.
func TestIntegrationNamingSingleInstanceReplaces(t *testing.T) {
	client, err := clients.NewNamingClient(clientParam(t))
	require.NoError(t, err, "create naming client")
	defer client.CloseClient()

	serviceName := fmt.Sprintf("it-single-replace-%d-%d", time.Now().UnixNano(), os.Getpid())
	const ip = "10.0.0.41"
	portX, portY := uint64(19201), uint64(19202)

	defer func() {
		_, _ = client.DeregisterInstance(vo.DeregisterInstanceParam{
			Ip: ip, Port: portY, ServiceName: serviceName, GroupName: testGroup, Ephemeral: true,
		})
	}()

	registerOne := func(port uint64) {
		t.Helper()
		ok, err := client.RegisterInstance(vo.RegisterInstanceParam{
			Ip:          ip,
			Port:        port,
			ServiceName: serviceName,
			GroupName:   testGroup,
			Weight:      1,
			Enable:      true,
			Healthy:     true,
			Ephemeral:   true,
		})
		require.NoError(t, err, "register instance on port %d", port)
		require.True(t, ok, "register instance on port %d should return true", port)
	}

	registerOne(portX)
	assert.Eventually(t, func() bool {
		svc, err := client.GetService(vo.GetServiceParam{ServiceName: serviceName, GroupName: testGroup})
		return err == nil && len(svc.Hosts) == 1 && hasPort(svc.Hosts, portX)
	}, waitTimeout, waitInterval, "X should become the sole discoverable instance")

	registerOne(portY)
	assert.Eventually(t, func() bool {
		svc, err := client.GetService(vo.GetServiceParam{ServiceName: serviceName, GroupName: testGroup})
		return err == nil && len(svc.Hosts) == 1 && hasPort(svc.Hosts, portY) && !hasPort(svc.Hosts, portX)
	}, waitTimeout, waitInterval, "registering Y should replace X, leaving Y as the sole instance")
}
