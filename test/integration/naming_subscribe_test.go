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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nacos-group/nacos-sdk-go/v3/clients"
	"github.com/nacos-group/nacos-sdk-go/v3/model"
	"github.com/nacos-group/nacos-sdk-go/v3/vo"
)

// TestIntegrationNamingSubscribeRegression replays #865: a subscription
// (with or without a cluster filter) must keep firing across a
// register -> deregister -> re-register cycle, on both the unfiltered and
// the cluster-filtered subscription path.
func TestIntegrationNamingSubscribeRegression(t *testing.T) {
	client, err := clients.NewNamingClient(clientParam(t))
	require.NoError(t, err, "create naming client")
	defer client.CloseClient()

	suffix := fmt.Sprintf("%d-%d", time.Now().UnixNano(), os.Getpid())
	serviceNoFilter := "it-sub-nofilter-" + suffix
	serviceWithFilter := "it-sub-filter-" + suffix
	const clusterName = "IT-CLUSTER"

	var countNoFilter int64
	var countWithFilter int64

	callbackNoFilter := func(services []model.Instance, err error) {
		atomic.AddInt64(&countNoFilter, 1)
	}
	callbackWithFilter := func(services []model.Instance, err error) {
		atomic.AddInt64(&countWithFilter, 1)
	}

	paramNoFilter := &vo.SubscribeParam{
		ServiceName:       serviceNoFilter,
		GroupName:         testGroup,
		SubscribeCallback: callbackNoFilter,
	}
	paramWithFilter := &vo.SubscribeParam{
		ServiceName:       serviceWithFilter,
		GroupName:         testGroup,
		Clusters:          []string{clusterName},
		SubscribeCallback: callbackWithFilter,
	}

	require.NoError(t, client.Subscribe(paramNoFilter), "subscribe without cluster filter")
	require.NoError(t, client.Subscribe(paramWithFilter), "subscribe with cluster filter")
	defer func() {
		_ = client.Unsubscribe(paramNoFilter)
		_ = client.Unsubscribe(paramWithFilter)
	}()

	const ipNoFilter = "10.0.0.30"
	const ipWithFilter = "10.0.0.31"
	const portNoFilter uint64 = 19001
	const portWithFilter uint64 = 19002

	register := func(svc, ip string, port uint64, cluster string) {
		t.Helper()
		ok, err := client.RegisterInstance(vo.RegisterInstanceParam{
			Ip:          ip,
			Port:        port,
			ServiceName: svc,
			GroupName:   testGroup,
			ClusterName: cluster,
			Weight:      1,
			Enable:      true,
			Healthy:     true,
			Ephemeral:   true,
		})
		require.NoError(t, err, "register instance for %s", svc)
		require.True(t, ok, "register instance for %s should return true", svc)
	}
	deregister := func(svc, ip string, port uint64, cluster string) {
		t.Helper()
		ok, err := client.DeregisterInstance(vo.DeregisterInstanceParam{
			Ip:          ip,
			Port:        port,
			ServiceName: svc,
			GroupName:   testGroup,
			Cluster:     cluster,
			Ephemeral:   true,
		})
		require.NoError(t, err, "deregister instance for %s", svc)
		require.True(t, ok, "deregister instance for %s should return true", svc)
	}

	defer func() {
		_, _ = client.DeregisterInstance(vo.DeregisterInstanceParam{
			Ip: ipNoFilter, Port: portNoFilter, ServiceName: serviceNoFilter, GroupName: testGroup, Ephemeral: true,
		})
		_, _ = client.DeregisterInstance(vo.DeregisterInstanceParam{
			Ip: ipWithFilter, Port: portWithFilter, ServiceName: serviceWithFilter, GroupName: testGroup,
			Cluster: clusterName, Ephemeral: true,
		})
	}()

	// Subscribe synchronously delivers an initial snapshot before it
	// returns, so an absolute threshold like ">= 1 after register" is
	// vacuous -- it's already satisfied by the snapshot before register()
	// even runs, and ">= 2 after deregister" could equally be satisfied by
	// two register fires alone. Capture a
	// per-step baseline for each counter and require it to advance past
	// that baseline, so each assertion is attributable to its own step and
	// a regression that breaks only e.g. the deregister-notification path
	// cannot hide behind the other steps' fires.

	// register
	baseNoFilter := atomic.LoadInt64(&countNoFilter)
	baseWithFilter := atomic.LoadInt64(&countWithFilter)
	register(serviceNoFilter, ipNoFilter, portNoFilter, "")
	register(serviceWithFilter, ipWithFilter, portWithFilter, clusterName)

	assert.Eventually(t, func() bool { return atomic.LoadInt64(&countNoFilter) > baseNoFilter },
		waitTimeout, waitInterval, "unfiltered subscription should fire on register")
	assert.Eventually(t, func() bool { return atomic.LoadInt64(&countWithFilter) > baseWithFilter },
		waitTimeout, waitInterval, "cluster-filtered subscription should fire on register")

	// deregister
	baseNoFilter = atomic.LoadInt64(&countNoFilter)
	baseWithFilter = atomic.LoadInt64(&countWithFilter)
	deregister(serviceNoFilter, ipNoFilter, portNoFilter, "")
	deregister(serviceWithFilter, ipWithFilter, portWithFilter, clusterName)

	assert.Eventually(t, func() bool { return atomic.LoadInt64(&countNoFilter) > baseNoFilter },
		waitTimeout, waitInterval, "unfiltered subscription should fire on deregister")
	assert.Eventually(t, func() bool { return atomic.LoadInt64(&countWithFilter) > baseWithFilter },
		waitTimeout, waitInterval, "cluster-filtered subscription should fire on deregister")

	// re-register
	baseNoFilter = atomic.LoadInt64(&countNoFilter)
	baseWithFilter = atomic.LoadInt64(&countWithFilter)
	register(serviceNoFilter, ipNoFilter, portNoFilter, "")
	register(serviceWithFilter, ipWithFilter, portWithFilter, clusterName)

	assert.Eventually(t, func() bool { return atomic.LoadInt64(&countNoFilter) > baseNoFilter },
		waitTimeout, waitInterval, "unfiltered subscription should fire on re-register")
	assert.Eventually(t, func() bool { return atomic.LoadInt64(&countWithFilter) > baseWithFilter },
		waitTimeout, waitInterval, "cluster-filtered subscription should fire on re-register")
}
