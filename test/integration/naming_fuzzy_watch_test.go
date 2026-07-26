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

// eventRecorder is a mutex-guarded collector for fuzzy watch events, shared
// by every scenario in this file. Callbacks fire asynchronously on the
// holder's per-pattern drain goroutine (never on the test goroutine), so
// every read or write of the collected events must go through the mutex -
// a bare slice/bool touched from both sides would trip `go test -race`.
type eventRecorder struct {
	mu     sync.Mutex
	events []model.FuzzyWatchChangeEvent
}

func (r *eventRecorder) record(event model.FuzzyWatchChangeEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, event)
}

// find returns the first recorded event matching changedType and
// serviceName, mirroring the pre-existing hasEvent helper but also handing
// back the event itself so callers can inspect SyncType.
func (r *eventRecorder) find(changedType, serviceName string) (model.FuzzyWatchChangeEvent, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, e := range r.events {
		if e.ChangedType == changedType && e.ServiceName == serviceName {
			return e, true
		}
	}
	return model.FuzzyWatchChangeEvent{}, false
}

func (r *eventRecorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.events)
}

// syncTypes snapshots every SyncType seen so far, purely for t.Logf
// observability - the relative arrival order/timing of
// FUZZY_WATCH_DIFF_SYNC_NOTIFY vs the locally generated
// FUZZY_WATCH_INIT_NOTIFY replay is server-push-timing-dependent and is not
// asserted on, only logged.
func (r *eventRecorder) syncTypes() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.events))
	for i, e := range r.events {
		out[i] = e.SyncType
	}
	return out
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

	recorder := &eventRecorder{}

	fwParam := &vo.FuzzyWatchParam{
		ServiceNamePattern: pattern,
		GroupNamePattern:   constant.DEFAULT_GROUP,
		WatchCallback:      recorder.record,
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

	var addEvent model.FuzzyWatchChangeEvent
	require.Eventually(t, func() bool {
		var found bool
		addEvent, found = recorder.find(constant.FUZZY_WATCH_CHANGED_TYPE_ADD_SERVICE, serviceName)
		return found
	}, waitTimeout, waitInterval, "fuzzy watch should observe ADD_SERVICE for the matching service")

	// The service was registered after the pattern's initial sync had
	// already settled (nothing matched it yet), so this ADD_SERVICE can only
	// have arrived via a post-init change notify - SyncType must be
	// FUZZY_WATCH_RESOURCE_CHANGED, not one of the batch-sync types.
	assert.NotEmpty(t, addEvent.SyncType, "ADD_SERVICE event should carry a non-empty SyncType")
	assert.Equal(t, constant.FUZZY_WATCH_RESOURCE_CHANGED, addEvent.SyncType,
		"ADD_SERVICE for a service registered after initial sync should be tagged FUZZY_WATCH_RESOURCE_CHANGED")

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
	for time.Now().Before(deleteDeadline) {
		if _, found := recorder.find(constant.FUZZY_WATCH_CHANGED_TYPE_DELETE_SERVICE, serviceName); found {
			break
		}
		time.Sleep(waitInterval)
	}
	if _, found := recorder.find(constant.FUZZY_WATCH_CHANGED_TYPE_DELETE_SERVICE, serviceName); found {
		t.Log("observed DELETE_SERVICE fuzzy watch event")
	} else {
		t.Log("DELETE_SERVICE fuzzy watch event not observed within 10s; server-side service GC timing " +
			"is not deterministic, so this run does not assert it")
	}
}

// TestIntegrationNamingFuzzyWatchSecondWatcherReplay exercises the
// second-watcher replay path added on top of #859: once a pattern's initial
// sync has delivered a match to its first watcher, registering a second
// FuzzyWatch callback on the very same pattern must replay that already-known
// match to the new callback alone, as an ADD_SERVICE event tagged
// FUZZY_WATCH_INIT_NOTIFY (the locally generated replay, not a fresh server
// batch sync) - see naming_cache.FuzzyWatchServiceListHolder.RegisterPattern.
func TestIntegrationNamingFuzzyWatchSecondWatcherReplay(t *testing.T) {
	if !isNacosV3Admin(t) {
		t.Skip("fuzzy watch requires nacos 3.x")
	}

	watchClient, err := clients.NewNamingClient(clientParam(t))
	require.NoError(t, err, "create naming client for fuzzy watch")
	defer watchClient.CloseClient()

	suffix := fmt.Sprintf("%d-%d", time.Now().UnixNano(), os.Getpid())
	pattern := fmt.Sprintf("fuzzy-svc-replay-%s-*", suffix)
	serviceName := fmt.Sprintf("fuzzy-svc-replay-%s-1", suffix)

	watcher1 := &eventRecorder{}
	fwParam1 := &vo.FuzzyWatchParam{
		ServiceNamePattern: pattern,
		GroupNamePattern:   constant.DEFAULT_GROUP,
		WatchCallback:      watcher1.record,
	}
	require.NoError(t, watchClient.FuzzyWatch(fwParam1), "fuzzy watch %s (watcher1)", pattern)
	// CancelFuzzyWatch is pattern-scoped (tears down every callback
	// registered for the pattern, not just the one passed in), so a single
	// deferred cancel at the end of the test retires both watchers.
	defer func() { _ = watchClient.CancelFuzzyWatch(fwParam1) }()

	regClient, err := clients.NewNamingClient(clientParam(t))
	require.NoError(t, err, "create naming client for registration")
	defer regClient.CloseClient()

	const ip = "10.0.0.41"
	const port uint64 = 19200

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
	defer func() {
		_, _ = regClient.DeregisterInstance(vo.DeregisterInstanceParam{
			Ip: ip, Port: port, ServiceName: serviceName, GroupName: constant.DEFAULT_GROUP, Ephemeral: true,
		})
	}()

	// Wait until watcher1 has actually observed the service. The pattern's
	// shared receivedGroupKeys (what RegisterPattern replays to a late
	// joiner) is updated synchronously as soon as the change is applied, but
	// watcher1 receiving the callback is the only externally observable
	// proof that has happened - so this is what makes watcher2's replay
	// deterministic rather than a race against the server's own push.
	require.Eventually(t, func() bool {
		_, found := watcher1.find(constant.FUZZY_WATCH_CHANGED_TYPE_ADD_SERVICE, serviceName)
		return found
	}, waitTimeout, waitInterval, "watcher1 should observe ADD_SERVICE before watcher2 registers")

	watcher2 := &eventRecorder{}
	fwParam2 := &vo.FuzzyWatchParam{
		ServiceNamePattern: pattern,
		GroupNamePattern:   constant.DEFAULT_GROUP,
		WatchCallback:      watcher2.record,
	}
	require.NoError(t, watchClient.FuzzyWatch(fwParam2), "fuzzy watch %s (watcher2)", pattern)

	// watcher2 is a late joiner on an already-synced pattern: it must be
	// replayed the existing match as ADD_SERVICE/FUZZY_WATCH_INIT_NOTIFY
	// without waiting for another real change on the server.
	require.Eventually(t, func() bool {
		ev, found := watcher2.find(constant.FUZZY_WATCH_CHANGED_TYPE_ADD_SERVICE, serviceName)
		return found && ev.SyncType == constant.FUZZY_WATCH_INIT_NOTIFY
	}, waitTimeout, waitInterval,
		"watcher2 should be replayed the existing match tagged FUZZY_WATCH_INIT_NOTIFY")

	// watcher1 must not have been replayed a second time: the replay in
	// RegisterPattern only targets the newly registered callback.
	watcher1Count := 0
	for _, st := range watcher1.syncTypes() {
		if st == constant.FUZZY_WATCH_INIT_NOTIFY {
			watcher1Count++
		}
	}
	assert.Zero(t, watcher1Count, "watcher1 should not receive a second-watcher replay of its own event")

	t.Logf("watcher1 syncTypes observed: %v", watcher1.syncTypes())
	t.Logf("watcher2 syncTypes observed: %v", watcher2.syncTypes())
}

// TestIntegrationNamingFuzzyWatchCancelNoResurrection exercises the
// pattern-scoped CancelFuzzyWatch semantics: once the cancel RPC has been
// confirmed by the server, a brand-new service that matches the
// now-canceled pattern must never resurrect it - the canceled callback must
// receive nothing for it, ever, not just "not yet".
func TestIntegrationNamingFuzzyWatchCancelNoResurrection(t *testing.T) {
	if !isNacosV3Admin(t) {
		t.Skip("fuzzy watch requires nacos 3.x")
	}

	watchClient, err := clients.NewNamingClient(clientParam(t))
	require.NoError(t, err, "create naming client for fuzzy watch")
	defer watchClient.CloseClient()

	suffix := fmt.Sprintf("%d-%d", time.Now().UnixNano(), os.Getpid())
	pattern := fmt.Sprintf("fuzzy-svc-cancel-%s-*", suffix)
	serviceName := fmt.Sprintf("fuzzy-svc-cancel-%s-1", suffix)
	laterServiceName := fmt.Sprintf("fuzzy-svc-cancel-%s-2", suffix)

	recorder := &eventRecorder{}
	fwParam := &vo.FuzzyWatchParam{
		ServiceNamePattern: pattern,
		GroupNamePattern:   constant.DEFAULT_GROUP,
		WatchCallback:      recorder.record,
	}
	require.NoError(t, watchClient.FuzzyWatch(fwParam), "fuzzy watch %s", pattern)

	regClient, err := clients.NewNamingClient(clientParam(t))
	require.NoError(t, err, "create naming client for registration")
	defer regClient.CloseClient()

	const ip = "10.0.0.42"
	const port1 uint64 = 19300
	const port2 uint64 = 19301

	ok, err := regClient.RegisterInstance(vo.RegisterInstanceParam{
		Ip:          ip,
		Port:        port1,
		ServiceName: serviceName,
		GroupName:   constant.DEFAULT_GROUP,
		Weight:      1,
		Enable:      true,
		Healthy:     true,
		Ephemeral:   true,
	})
	require.NoError(t, err, "register matching service before cancel")
	require.True(t, ok, "register matching service before cancel should return true")
	defer func() {
		_, _ = regClient.DeregisterInstance(vo.DeregisterInstanceParam{
			Ip: ip, Port: port1, ServiceName: serviceName, GroupName: constant.DEFAULT_GROUP, Ephemeral: true,
		})
	}()

	require.Eventually(t, func() bool {
		_, found := recorder.find(constant.FUZZY_WATCH_CHANGED_TYPE_ADD_SERVICE, serviceName)
		return found
	}, waitTimeout, waitInterval, "watch should observe ADD_SERVICE before cancel")

	require.NoError(t, watchClient.CancelFuzzyWatch(fwParam), "cancel fuzzy watch %s", pattern)
	preCancelCount := recorder.count()

	// Register a brand-new matching service AFTER the cancel is confirmed.
	// If cancellation had left any residue - the callback still attached
	// locally, or the server-side watch still active - this registration
	// would eventually produce a new ADD_SERVICE for the canceled callback.
	ok, err = regClient.RegisterInstance(vo.RegisterInstanceParam{
		Ip:          ip,
		Port:        port2,
		ServiceName: laterServiceName,
		GroupName:   constant.DEFAULT_GROUP,
		Weight:      1,
		Enable:      true,
		Healthy:     true,
		Ephemeral:   true,
	})
	require.NoError(t, err, "register matching service after cancel")
	require.True(t, ok, "register matching service after cancel should return true")
	defer func() {
		_, _ = regClient.DeregisterInstance(vo.DeregisterInstanceParam{
			Ip: ip, Port: port2, ServiceName: laterServiceName, GroupName: constant.DEFAULT_GROUP, Ephemeral: true,
		})
	}()

	// Bounded poll rather than require.Eventually: there is nothing to wait
	// FOR here, only the absence of an event, so every sample in the window
	// must fail to find one - a single passing check proves nothing.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		_, found := recorder.find(constant.FUZZY_WATCH_CHANGED_TYPE_ADD_SERVICE, laterServiceName)
		require.False(t, found,
			"canceled callback must not observe ADD_SERVICE for a service created after cancel (pattern must not resurrect)")
		time.Sleep(waitInterval)
	}

	// Same fact from another angle: the total event count for the canceled
	// callback must not have grown at all during the poll window - not this
	// specific service, not any other kind of event either. This is the
	// closest black-box proxy available for "the pattern is gone" from an
	// external test package, since the holder's internal pattern map is not
	// exported for direct introspection.
	assert.Equal(t, preCancelCount, recorder.count(),
		"no events of any kind should reach the canceled callback after cancellation")

	t.Logf("syncTypes observed before cancel: %v", recorder.syncTypes())
}
