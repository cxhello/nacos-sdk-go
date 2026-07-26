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
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nacos-group/nacos-sdk-go/v3/clients"
	"github.com/nacos-group/nacos-sdk-go/v3/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v3/model"
	"github.com/nacos-group/nacos-sdk-go/v3/vo"
)

// isNacosV3Admin probes the server's v3 admin API to decide whether fuzzy
// watch (a 3.x-only naming feature, #859) can be exercised. Nacos 2.x has no
// /nacos/v3/admin/... routes at all and answers with 404; Nacos 3.x has the
// route but guards it with its own admin auth, so it answers with 401/403
// (access denied) rather than 404/501 (route not found / not implemented).
// Either non-404/501 response is therefore proof the route exists, i.e. the
// server is 3.x, without needing admin credentials.
func isNacosV3Admin(t *testing.T) bool {
	t.Helper()
	ip := envOr("NACOS_SERVER_IP", "127.0.0.1")
	port := envOr("NACOS_SERVER_PORT", "8848")
	url := fmt.Sprintf("http://%s:%s/nacos/v3/admin/ns/service/list?pageNo=1&pageSize=1&namespaceId=&groupName=%s",
		ip, port, constant.DEFAULT_GROUP)

	httpClient := &http.Client{Timeout: 5 * time.Second}
	resp, err := httpClient.Get(url)
	if err != nil {
		t.Logf("v3 admin API probe failed: %v", err)
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode != http.StatusNotFound && resp.StatusCode != http.StatusNotImplemented
}

// TestIntegrationNamingFuzzyWatch exercises #859: FuzzyWatch registers a
// pattern watch, and a service registered by an independent client that
// matches the pattern must push an ADD_SERVICE event to the watcher. This
// is a 3.x-only naming feature, so the test skips on 2.x servers.
func TestIntegrationNamingFuzzyWatch(t *testing.T) {
	if !isNacosV3Admin(t) {
		t.Skip("fuzzy watch requires nacos 3.x")
	}

	watchClient, err := clients.NewNamingClient(clientParam(t))
	require.NoError(t, err, "create naming client for fuzzy watch")
	defer watchClient.CloseClient()

	suffix := fmt.Sprintf("%d-%d", time.Now().UnixNano(), os.Getpid())
	pattern := fmt.Sprintf("fuzzy-svc-%s-*", suffix)
	serviceName := fmt.Sprintf("fuzzy-svc-%s-1", suffix)

	var mu sync.Mutex
	var events []model.FuzzyWatchChangeEvent

	fwParam := &vo.FuzzyWatchParam{
		ServiceNamePattern: pattern,
		GroupNamePattern:   constant.DEFAULT_GROUP,
		WatchCallback: func(event model.FuzzyWatchChangeEvent) {
			mu.Lock()
			events = append(events, event)
			mu.Unlock()
		},
	}
	require.NoError(t, watchClient.FuzzyWatch(fwParam), "fuzzy watch %s", pattern)
	defer func() { _ = watchClient.CancelFuzzyWatch(fwParam) }()

	// Register from an independent client, mirroring a separate service
	// instance producing the change rather than the watcher itself.
	regClient, err := clients.NewNamingClient(clientParam(t))
	require.NoError(t, err, "create naming client for registration")
	defer regClient.CloseClient()

	const ip = "10.0.0.40"
	const port uint64 = 19100

	ok, err := regClient.RegisterInstance(vo.RegisterInstanceParam{
		Ip:          ip,
		Port:        port,
		ServiceName: serviceName,
		GroupName:   constant.DEFAULT_GROUP,
		Weight:      1,
		Enable:      true,
		Healthy:     true,
		Ephemeral:   true,
	})
	require.NoError(t, err, "register matching service")
	require.True(t, ok, "register matching service should return true")

	hasEvent := func(changedType string) bool {
		mu.Lock()
		defer mu.Unlock()
		for _, e := range events {
			if e.ChangedType == changedType && e.ServiceName == serviceName {
				return true
			}
		}
		return false
	}

	assert.Eventually(t, func() bool {
		return hasEvent(constant.FUZZY_WATCH_CHANGED_TYPE_ADD_SERVICE)
	}, waitTimeout, waitInterval, "fuzzy watch should observe ADD_SERVICE for the matching service")

	ok, err = regClient.DeregisterInstance(vo.DeregisterInstanceParam{
		Ip: ip, Port: port, ServiceName: serviceName, GroupName: constant.DEFAULT_GROUP, Ephemeral: true,
	})
	require.NoError(t, err, "deregister matching service")
	require.True(t, ok, "deregister matching service should return true")

	// DELETE_SERVICE depends on the server actually GC'ing the now-empty
	// service, which happens on its own housekeeping cadence and is not
	// deterministic within a short test window. Poll for it but only log
	// (never fail) if it doesn't show up in time.
	deleteDeadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deleteDeadline) && !hasEvent(constant.FUZZY_WATCH_CHANGED_TYPE_DELETE_SERVICE) {
		time.Sleep(waitInterval)
	}
	if hasEvent(constant.FUZZY_WATCH_CHANGED_TYPE_DELETE_SERVICE) {
		t.Log("observed DELETE_SERVICE fuzzy watch event")
	} else {
		t.Log("DELETE_SERVICE fuzzy watch event not observed within 10s; server-side service GC timing " +
			"is not deterministic, so this run does not assert it")
	}
}
