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

package naming_cache

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nacos-group/nacos-sdk-go/v3/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v3/common/remote/rpc/rpc_request"
	"github.com/nacos-group/nacos-sdk-go/v3/model"
)

// eventSink is a concurrency-safe collector for the callback events.
type eventSink struct {
	mu     sync.Mutex
	events []model.FuzzyWatchChangeEvent
}

func (s *eventSink) cb(e model.FuzzyWatchChangeEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, e)
}

func (s *eventSink) snapshot() []model.FuzzyWatchChangeEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]model.FuzzyWatchChangeEvent, len(s.events))
	copy(out, s.events)
	return out
}

func (s *eventSink) len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.events)
}

func syncCtx(serviceKey, changedType string) rpc_request.NamingFuzzyWatchSyncContext {
	return rpc_request.NamingFuzzyWatchSyncContext{ServiceKey: serviceKey, ChangedType: changedType}
}

const testPattern = "public>>DEFAULT_GROUP>>order*"

// waitForEvents blocks until sink has collected exactly n events, polling
// because dispatch is asynchronous now (drain runs on its own goroutine
// rather than delivering synchronously to the caller). Fails the test on
// timeout.
func waitForEvents(t *testing.T, sink *eventSink, n int, msgAndArgs ...interface{}) {
	t.Helper()
	require.Eventually(t, func() bool {
		return sink.len() == n
	}, 2*time.Second, 5*time.Millisecond, msgAndArgs...)
}

func TestHolderInitBatchingAndFinish(t *testing.T) {
	holder := NewFuzzyWatchServiceListHolder("public")
	holder.SetRequester(newFakeRequester())
	sink := &eventSink{}
	holder.RegisterWatcher(testPattern, sink.cb, nil)

	// two INIT batches, then a FINISH marks the initial sync complete.
	holder.HandleSync(testPattern, constant.FUZZY_WATCH_INIT_NOTIFY, []rpc_request.NamingFuzzyWatchSyncContext{
		syncCtx("public@@DEFAULT_GROUP@@order-a", constant.FUZZY_WATCH_CHANGED_TYPE_ADD_SERVICE),
		syncCtx("public@@DEFAULT_GROUP@@order-b", constant.FUZZY_WATCH_CHANGED_TYPE_ADD_SERVICE),
	}, 2, 1)
	assert.False(t, holder.isInitialized(testPattern), "not initialized until FINISH")

	holder.HandleSync(testPattern, constant.FUZZY_WATCH_INIT_NOTIFY, []rpc_request.NamingFuzzyWatchSyncContext{
		syncCtx("public@@DEFAULT_GROUP@@order-c", constant.FUZZY_WATCH_CHANGED_TYPE_ADD_SERVICE),
	}, 2, 2)

	holder.HandleSync(testPattern, constant.FINISH_FUZZY_WATCH_INIT_NOTIFY, nil, 2, 2)
	assert.True(t, holder.isInitialized(testPattern), "FINISH marks initialized")

	waitForEvents(t, sink, 3, "one ADD callback per matched service across both batches")
	events := sink.snapshot()
	for _, e := range events {
		assert.Equal(t, constant.FUZZY_WATCH_CHANGED_TYPE_ADD_SERVICE, e.ChangedType)
		assert.Equal(t, "public", e.NamespaceId)
		assert.Equal(t, "DEFAULT_GROUP", e.GroupName)
	}

	keys := holder.ReceivedGroupKeys(testPattern)
	sort.Strings(keys)
	assert.Equal(t, []string{
		"public@@DEFAULT_GROUP@@order-a",
		"public@@DEFAULT_GROUP@@order-b",
		"public@@DEFAULT_GROUP@@order-c",
	}, keys)
}

func TestHolderDuplicateAddDoesNotRefire(t *testing.T) {
	holder := NewFuzzyWatchServiceListHolder("public")
	holder.SetRequester(newFakeRequester())
	sink := &eventSink{}
	holder.RegisterWatcher(testPattern, sink.cb, nil)

	add := []rpc_request.NamingFuzzyWatchSyncContext{
		syncCtx("public@@DEFAULT_GROUP@@order-a", constant.FUZZY_WATCH_CHANGED_TYPE_ADD_SERVICE),
	}
	holder.HandleSync(testPattern, constant.FUZZY_WATCH_INIT_NOTIFY, add, 1, 1)
	// server re-sends the same key (e.g. redo DIFF): must not re-fire.
	holder.HandleSync(testPattern, constant.FUZZY_WATCH_DIFF_SYNC_NOTIFY, add, 1, 1)

	waitForEvents(t, sink, 1, "duplicate ADD for a known key must not re-fire")
	// waitForEvents only proves the count reached 1 at some poll instant; it
	// cannot see a spurious 2nd event fired a moment later. Settle briefly
	// and re-check that the count is still exactly 1.
	time.Sleep(75 * time.Millisecond)
	assert.Equal(t, 1, sink.len(), "count must still be 1 after settling; a late duplicate fire would only show up here")
	assert.Len(t, holder.ReceivedGroupKeys(testPattern), 1)
}

func TestHolderDiffAddAndDelete(t *testing.T) {
	holder := NewFuzzyWatchServiceListHolder("public")
	holder.SetRequester(newFakeRequester())
	sink := &eventSink{}
	holder.RegisterWatcher(testPattern, sink.cb, nil)

	holder.HandleSync(testPattern, constant.FUZZY_WATCH_INIT_NOTIFY, []rpc_request.NamingFuzzyWatchSyncContext{
		syncCtx("public@@DEFAULT_GROUP@@order-a", constant.FUZZY_WATCH_CHANGED_TYPE_ADD_SERVICE),
		syncCtx("public@@DEFAULT_GROUP@@order-b", constant.FUZZY_WATCH_CHANGED_TYPE_ADD_SERVICE),
	}, 1, 1)

	// DIFF: add order-c, delete order-a.
	holder.HandleSync(testPattern, constant.FUZZY_WATCH_DIFF_SYNC_NOTIFY, []rpc_request.NamingFuzzyWatchSyncContext{
		syncCtx("public@@DEFAULT_GROUP@@order-c", constant.FUZZY_WATCH_CHANGED_TYPE_ADD_SERVICE),
		syncCtx("public@@DEFAULT_GROUP@@order-a", constant.FUZZY_WATCH_CHANGED_TYPE_DELETE_SERVICE),
	}, 1, 1)

	keys := holder.ReceivedGroupKeys(testPattern)
	sort.Strings(keys)
	assert.Equal(t, []string{
		"public@@DEFAULT_GROUP@@order-b",
		"public@@DEFAULT_GROUP@@order-c",
	}, keys)

	waitForEvents(t, sink, 4, "2 initial ADD + 1 DIFF ADD + 1 DIFF DELETE")
	events := sink.snapshot()
	last := events[3]
	assert.Equal(t, constant.FUZZY_WATCH_CHANGED_TYPE_DELETE_SERVICE, last.ChangedType)
	assert.Equal(t, "order-a", last.ServiceName)
}

func TestHolderUnknownDeleteDoesNotFire(t *testing.T) {
	holder := NewFuzzyWatchServiceListHolder("public")
	holder.SetRequester(newFakeRequester())
	sink := &eventSink{}
	holder.RegisterWatcher(testPattern, sink.cb, nil)

	holder.HandleSync(testPattern, constant.FUZZY_WATCH_DIFF_SYNC_NOTIFY, []rpc_request.NamingFuzzyWatchSyncContext{
		syncCtx("public@@DEFAULT_GROUP@@order-unknown", constant.FUZZY_WATCH_CHANGED_TYPE_DELETE_SERVICE),
	}, 1, 1)

	// Nothing was ever enqueued, so there's no drain race to wait out.
	assert.Empty(t, sink.snapshot(), "DELETE for an unknown key must not fire")
	assert.Empty(t, holder.ReceivedGroupKeys(testPattern))
}

func TestHolderMalformedServiceKeySkipped(t *testing.T) {
	holder := NewFuzzyWatchServiceListHolder("public")
	holder.SetRequester(newFakeRequester())
	sink := &eventSink{}
	holder.RegisterWatcher(testPattern, sink.cb, nil)

	holder.HandleSync(testPattern, constant.FUZZY_WATCH_INIT_NOTIFY, []rpc_request.NamingFuzzyWatchSyncContext{
		syncCtx("this-is-not-a-valid-service-key", constant.FUZZY_WATCH_CHANGED_TYPE_ADD_SERVICE),
		syncCtx("public@@DEFAULT_GROUP@@order-a", constant.FUZZY_WATCH_CHANGED_TYPE_ADD_SERVICE),
	}, 1, 1)

	waitForEvents(t, sink, 1, "malformed serviceKey is skipped, the valid one still fires")
	events := sink.snapshot()
	assert.Equal(t, "order-a", events[0].ServiceName)
	assert.Equal(t, []string{"public@@DEFAULT_GROUP@@order-a"}, holder.ReceivedGroupKeys(testPattern))
}

func TestHolderChangeNotifyMatchingPatternOnly(t *testing.T) {
	holder := NewFuzzyWatchServiceListHolder("public")
	holder.SetRequester(newFakeRequester())
	orderSink := &eventSink{}
	userSink := &eventSink{}
	holder.RegisterWatcher("public>>DEFAULT_GROUP>>order*", orderSink.cb, nil)
	holder.RegisterWatcher("public>>DEFAULT_GROUP>>user*", userSink.cb, nil)

	// A change notify carries only a serviceKey; the holder matches it
	// against every registered pattern.
	holder.HandleChangeNotify("public@@DEFAULT_GROUP@@order-x", constant.FUZZY_WATCH_CHANGED_TYPE_ADD_SERVICE)

	waitForEvents(t, orderSink, 1, "matching pattern receives the change")
	assert.Empty(t, userSink.snapshot(), "non-matching pattern is untouched")
	assert.Equal(t, []string{"public@@DEFAULT_GROUP@@order-x"}, holder.ReceivedGroupKeys("public>>DEFAULT_GROUP>>order*"))

	// duplicate ADD via change notify does not re-fire
	holder.HandleChangeNotify("public@@DEFAULT_GROUP@@order-x", constant.FUZZY_WATCH_CHANGED_TYPE_ADD_SERVICE)

	// delete removes and fires
	holder.HandleChangeNotify("public@@DEFAULT_GROUP@@order-x", constant.FUZZY_WATCH_CHANGED_TYPE_DELETE_SERVICE)
	waitForEvents(t, orderSink, 2, "duplicate ADD does not re-fire, DELETE does")
	// waitForEvents only proves the count reached 2 at some poll instant; it
	// cannot see a spurious 3rd event (the duplicate ADD firing late).
	// Settle briefly and re-check that the count is still exactly 2.
	time.Sleep(75 * time.Millisecond)
	assert.Equal(t, 2, orderSink.len(), "count must still be 2 after settling; a late duplicate fire would only show up here")
	assert.Empty(t, holder.ReceivedGroupKeys("public>>DEFAULT_GROUP>>order*"))
}

func TestItemMatchFiveModes(t *testing.T) {
	cases := []struct {
		name    string
		pattern string
		value   string
		want    bool
	}{
		{"exact hit", "order", "order", true},
		{"exact miss", "order", "orders", false},
		{"all", "*", "anything", true},
		{"prefix hit", "order*", "order-service", true},
		{"prefix miss", "order*", "user-service", false},
		{"suffix hit", "*order", "cancel-order", true},
		{"suffix miss", "*order", "order-service", false},
		{"contains hit", "*order*", "my-order-service", true},
		{"contains miss", "*order*", "user-service", false},
		// explicit regressions for the two modes the naive matcher dropped:
		{"suffix regression not treated as exact", "*order", "the-order", true},
		{"contains regression not treated as prefix", "*order*", "an-order-x", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.want, itemMatch(c.pattern, c.value))
		})
	}
}

func TestMatchPatternAllModesForBothSegments(t *testing.T) {
	cases := []struct {
		name    string
		pattern string
		want    bool
	}{
		{"exact group + prefix service", "public>>DEFAULT_GROUP>>order*", true},
		{"suffix group hits", "public>>*GROUP>>order-service", true},
		{"contains group hits", "public>>*FAULT*>>order-service", true},
		{"suffix service hits", "public>>DEFAULT_GROUP>>*service", true},
		{"contains service hits", "public>>DEFAULT_GROUP>>*der-ser*", true},
		{"all group and service", "public>>*>>*", true},
		{"namespace must be exact", "other>>*>>*", false},
		{"suffix service misses", "public>>DEFAULT_GROUP>>*order", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.want, matchPattern(c.pattern, "public", "DEFAULT_GROUP", "order-service"))
		})
	}
}

func TestHandleChangeNotifyContainsPatternReceivesEvent(t *testing.T) {
	holder := NewFuzzyWatchServiceListHolder("public")
	holder.SetRequester(newFakeRequester())
	sink := &eventSink{}
	// a contains-mode pattern: previously its post-init change-notify was dropped.
	holder.RegisterWatcher("public>>DEFAULT_GROUP>>*order*", sink.cb, nil)

	holder.HandleChangeNotify("public@@DEFAULT_GROUP@@my-order-service", constant.FUZZY_WATCH_CHANGED_TYPE_ADD_SERVICE)

	waitForEvents(t, sink, 1, "contains-mode pattern must receive its change-notify")
	events := sink.snapshot()
	assert.Equal(t, "my-order-service", events[0].ServiceName)
	assert.Equal(t, constant.FUZZY_WATCH_CHANGED_TYPE_ADD_SERVICE, events[0].ChangedType)
}

func TestHandleChangeNotifySuffixPatternReceivesEvent(t *testing.T) {
	holder := NewFuzzyWatchServiceListHolder("public")
	holder.SetRequester(newFakeRequester())
	sink := &eventSink{}
	holder.RegisterWatcher("public>>DEFAULT_GROUP>>*service", sink.cb, nil)

	holder.HandleChangeNotify("public@@DEFAULT_GROUP@@order-service", constant.FUZZY_WATCH_CHANGED_TYPE_ADD_SERVICE)

	waitForEvents(t, sink, 1, "suffix-mode pattern must receive its change-notify")
}

// TestHandleSyncIgnoresUnknownPattern is a regression test asserting that a
// late server sync must never recreate a pattern context the user already
// canceled (or one that was never registered). HandleSync uses get(), which
// only looks up an existing context rather than creating one on demand, so
// an unknown pattern is dropped rather than resurrected.
func TestHandleSyncIgnoresUnknownPattern(t *testing.T) {
	holder := NewFuzzyWatchServiceListHolder("public")
	holder.SetRequester(newFakeRequester())

	holder.HandleSync(testPattern, constant.FUZZY_WATCH_INIT_NOTIFY, []rpc_request.NamingFuzzyWatchSyncContext{
		syncCtx("public@@DEFAULT_GROUP@@order-a", constant.FUZZY_WATCH_CHANGED_TYPE_ADD_SERVICE),
	}, 1, 1)

	assert.NotContains(t, holder.Patterns(), testPattern, "HandleSync must not resurrect a canceled/unknown pattern")
}

// TestCallbacksRunOffCallerGoroutine is a regression test asserting that a
// callback that blocks must not block the caller (in production, the gRPC
// push-handler goroutine that has to ACK the server request). HandleSync
// must return promptly even though the callback
// it queued has not run yet.
func TestCallbacksRunOffCallerGoroutine(t *testing.T) {
	holder := NewFuzzyWatchServiceListHolder("public")
	holder.SetRequester(newFakeRequester())
	started := make(chan struct{})
	release := make(chan struct{})
	holder.RegisterWatcher(testPattern, func(model.FuzzyWatchChangeEvent) {
		close(started)
		<-release
	}, nil)

	handleSyncDone := make(chan struct{})
	go func() {
		holder.HandleSync(testPattern, constant.FUZZY_WATCH_INIT_NOTIFY, []rpc_request.NamingFuzzyWatchSyncContext{
			syncCtx("public@@DEFAULT_GROUP@@order-a", constant.FUZZY_WATCH_CHANGED_TYPE_ADD_SERVICE),
		}, 1, 1)
		close(handleSyncDone)
	}()

	select {
	case <-handleSyncDone:
	case <-time.After(2 * time.Second):
		t.Fatal("HandleSync must return promptly even though its callback is blocked")
	}

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("callback was never dispatched off the caller goroutine")
	}
	close(release)
}

// TestEventsDeliveredInOrder asserts the async dispatcher preserves
// per-pattern delivery order: a single drain goroutine per context means
// events are never reordered even though they run off the caller goroutine.
func TestEventsDeliveredInOrder(t *testing.T) {
	holder := NewFuzzyWatchServiceListHolder("public")
	holder.SetRequester(newFakeRequester())
	sink := &eventSink{}
	holder.RegisterWatcher(testPattern, sink.cb, nil)

	holder.HandleSync(testPattern, constant.FUZZY_WATCH_INIT_NOTIFY, []rpc_request.NamingFuzzyWatchSyncContext{
		syncCtx("public@@DEFAULT_GROUP@@order-a", constant.FUZZY_WATCH_CHANGED_TYPE_ADD_SERVICE),
	}, 1, 1)
	holder.HandleSync(testPattern, constant.FUZZY_WATCH_DIFF_SYNC_NOTIFY, []rpc_request.NamingFuzzyWatchSyncContext{
		syncCtx("public@@DEFAULT_GROUP@@order-a", constant.FUZZY_WATCH_CHANGED_TYPE_DELETE_SERVICE),
	}, 1, 1)
	holder.HandleSync(testPattern, constant.FUZZY_WATCH_DIFF_SYNC_NOTIFY, []rpc_request.NamingFuzzyWatchSyncContext{
		syncCtx("public@@DEFAULT_GROUP@@order-a", constant.FUZZY_WATCH_CHANGED_TYPE_ADD_SERVICE),
	}, 1, 1)

	waitForEvents(t, sink, 3, "ADD/DELETE/ADD must all be delivered")
	events := sink.snapshot()
	require.Len(t, events, 3)
	assert.Equal(t, constant.FUZZY_WATCH_CHANGED_TYPE_ADD_SERVICE, events[0].ChangedType)
	assert.Equal(t, constant.FUZZY_WATCH_CHANGED_TYPE_DELETE_SERVICE, events[1].ChangedType)
	assert.Equal(t, constant.FUZZY_WATCH_CHANGED_TYPE_ADD_SERVICE, events[2].ChangedType)
}

// TestRegisterWatcherAppendsDuplicates documents that RegisterWatcher does
// not dedupe by code pointer: registering the same func value twice yields
// two distinct ids and both fire independently, matching the Java client
// where two distinct watcher objects both receive the event.
func TestRegisterWatcherAppendsDuplicates(t *testing.T) {
	holder := NewFuzzyWatchServiceListHolder("public")
	holder.SetRequester(newFakeRequester())
	sink := &eventSink{}
	id1, err := holder.RegisterWatcher(testPattern, sink.cb, nil)
	require.NoError(t, err)
	id2, err := holder.RegisterWatcher(testPattern, sink.cb, nil)
	require.NoError(t, err)

	assert.NotEqual(t, id1, id2, "each registration is a distinct id, even for the same callback value")

	holder.HandleChangeNotify("public@@DEFAULT_GROUP@@order-a", constant.FUZZY_WATCH_CHANGED_TYPE_ADD_SERVICE)

	waitForEvents(t, sink, 2, "duplicate registration is not deduped; the event is delivered once per registration")
}

// TestSecondWatcherReplaysExistingKeys is a regression test for Java parity
// (NamingFuzzyWatchServiceListHolder#registerFuzzyWatcher): a watcher that
// registers on a pattern *after* it has already synced some matches must be
// replayed those matches immediately, as if it had been there from the
// start, instead of only learning about them on the next server push. The
// replay must target only the newly registered callback - the first watcher
// already has these events and must not see them again.
func TestSecondWatcherReplaysExistingKeys(t *testing.T) {
	holder := NewFuzzyWatchServiceListHolder("public")
	holder.SetRequester(newFakeRequester())
	firstSink := &eventSink{}
	_, err := holder.RegisterWatcher(testPattern, firstSink.cb, nil)
	require.NoError(t, err)

	holder.HandleSync(testPattern, constant.FUZZY_WATCH_INIT_NOTIFY, []rpc_request.NamingFuzzyWatchSyncContext{
		syncCtx("public@@DEFAULT_GROUP@@order-a", constant.FUZZY_WATCH_CHANGED_TYPE_ADD_SERVICE),
		syncCtx("public@@DEFAULT_GROUP@@order-b", constant.FUZZY_WATCH_CHANGED_TYPE_ADD_SERVICE),
	}, 1, 1)
	waitForEvents(t, firstSink, 2, "first watcher gets the initial batch")

	secondSink := &eventSink{}
	_, err = holder.RegisterWatcher(testPattern, secondSink.cb, nil)
	require.NoError(t, err)

	waitForEvents(t, secondSink, 2, "late-joining watcher is replayed the existing matched services")
	events := secondSink.snapshot()
	names := []string{events[0].ServiceName, events[1].ServiceName}
	sort.Strings(names)
	assert.Equal(t, []string{"order-a", "order-b"}, names)
	for _, e := range events {
		assert.Equal(t, constant.FUZZY_WATCH_CHANGED_TYPE_ADD_SERVICE, e.ChangedType, "replay is always modeled as an ADD")
		assert.Equal(t, constant.FUZZY_WATCH_INIT_NOTIFY, e.SyncType, "replay carries INIT_NOTIFY sync type, matching Java")
	}

	// The pre-existing watcher must not be replayed again: settle briefly and
	// re-check its count is still exactly 2 (waitForEvents above only proves
	// the *second* sink reached 2 at some instant; it says nothing about a
	// spurious 3rd event landing on firstSink a moment later).
	time.Sleep(75 * time.Millisecond)
	assert.Equal(t, 2, firstSink.len(), "existing watcher must not receive a duplicate replay")
}

// TestSyncTypePropagatedOnHandleSync is a regression test for exposing the
// server's own syncType on the public event: callers otherwise cannot tell an
// initial batch sync apart from a later diff sync.
func TestSyncTypePropagatedOnHandleSync(t *testing.T) {
	holder := NewFuzzyWatchServiceListHolder("public")
	holder.SetRequester(newFakeRequester())
	sink := &eventSink{}
	holder.RegisterWatcher(testPattern, sink.cb, nil)

	holder.HandleSync(testPattern, constant.FUZZY_WATCH_INIT_NOTIFY, []rpc_request.NamingFuzzyWatchSyncContext{
		syncCtx("public@@DEFAULT_GROUP@@order-a", constant.FUZZY_WATCH_CHANGED_TYPE_ADD_SERVICE),
	}, 1, 1)
	waitForEvents(t, sink, 1)
	assert.Equal(t, constant.FUZZY_WATCH_INIT_NOTIFY, sink.snapshot()[0].SyncType)

	holder.HandleSync(testPattern, constant.FUZZY_WATCH_DIFF_SYNC_NOTIFY, []rpc_request.NamingFuzzyWatchSyncContext{
		syncCtx("public@@DEFAULT_GROUP@@order-b", constant.FUZZY_WATCH_CHANGED_TYPE_ADD_SERVICE),
	}, 1, 1)
	waitForEvents(t, sink, 2)
	assert.Equal(t, constant.FUZZY_WATCH_DIFF_SYNC_NOTIFY, sink.snapshot()[1].SyncType)
}

// TestSyncTypeIsResourceChangedOnChangeNotify is a regression test asserting
// that a post-init change-notify event always carries
// constant.FUZZY_WATCH_RESOURCE_CHANGED, distinguishing it from a batch sync
// event even though both fire through the same callback.
func TestSyncTypeIsResourceChangedOnChangeNotify(t *testing.T) {
	holder := NewFuzzyWatchServiceListHolder("public")
	holder.SetRequester(newFakeRequester())
	sink := &eventSink{}
	holder.RegisterWatcher(testPattern, sink.cb, nil)

	holder.HandleChangeNotify("public@@DEFAULT_GROUP@@order-a", constant.FUZZY_WATCH_CHANGED_TYPE_ADD_SERVICE)

	waitForEvents(t, sink, 1)
	assert.Equal(t, constant.FUZZY_WATCH_RESOURCE_CHANGED, sink.snapshot()[0].SyncType)
}

// waitFor polls cond until it returns true, failing the test if it never
// does within the deadline. Unlike waitForEvents (which is tied to
// eventSink), this is a general-purpose poll used by the dispatcher
// hardening tests below.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met within 2s")
}

// TestCallbackPanicIsIsolated asserts that a panicking watcher callback
// cannot starve or block delivery to the other watchers on the same
// pattern: safeInvoke must recover the panic and let drain continue to the
// next target.
func TestCallbackPanicIsIsolated(t *testing.T) {
	h := NewFuzzyWatchServiceListHolder("public")
	h.SetRequester(newFakeRequester())
	h.RegisterWatcher("public>>g>>svc*", func(model.FuzzyWatchChangeEvent) { panic("boom") }, nil)
	got := make(chan model.FuzzyWatchChangeEvent, 1)
	h.RegisterWatcher("public>>g>>svc*", func(ev model.FuzzyWatchChangeEvent) { got <- ev }, nil)
	h.HandleSync("public>>g>>svc*", constant.FUZZY_WATCH_INIT_NOTIFY,
		[]rpc_request.NamingFuzzyWatchSyncContext{{ServiceKey: "public@@g@@svc1", ChangedType: constant.FUZZY_WATCH_CHANGED_TYPE_ADD_SERVICE}}, 1, 1)
	select {
	case ev := <-got:
		assert.Equal(t, "svc1", ev.ServiceName)
	case <-time.After(2 * time.Second):
		t.Fatal("second callback starved by panicking first callback")
	}
}

// TestCancelInvalidatesQueuedCallbacks asserts that a task already sitting in
// the dispatch queue when its watcher is removed must not invoke that watcher
// afterward: a callback mid-flight cannot be recalled, but SDK-owned pending
// work must observe the cancellation. Sequence: the first delivery blocks the
// drain goroutine inside the callback, a second event is enqueued behind it,
// the watcher is removed, and the first delivery is released - the queued
// second task must then be skipped.
func TestCancelInvalidatesQueuedCallbacks(t *testing.T) {
	h := NewFuzzyWatchServiceListHolder("public")
	h.SetRequester(newFakeRequester())
	var count atomic.Int32
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	id, err := h.RegisterWatcher("public>>g>>svc*", func(model.FuzzyWatchChangeEvent) {
		count.Add(1)
		entered <- struct{}{}
		<-release
	}, nil)
	require.NoError(t, err)

	h.HandleChangeNotify("public@@g@@svc1", constant.FUZZY_WATCH_CHANGED_TYPE_ADD_SERVICE)
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("first delivery never started")
	}
	h.HandleChangeNotify("public@@g@@svc2", constant.FUZZY_WATCH_CHANGED_TYPE_ADD_SERVICE)
	h.RemoveWatcherByID("public>>g>>svc*", id)
	close(release)

	ctx, ok := h.get("public>>g>>svc*")
	require.True(t, ok, "pattern state stays until the worker confirms the CANCEL")
	waitFor(t, func() bool {
		ctx.mu.Lock()
		defer ctx.mu.Unlock()
		return !ctx.draining && len(ctx.pending) == 0
	})
	assert.Equal(t, int32(1), count.Load(), "a task queued before Cancel must not invoke the removed watcher")
}

// TestPendingQueueIsBounded asserts that the per-pattern dispatch queue does
// not grow without bound when a callback stalls the drain goroutine: once
// pendingLimit is reached, further notifications are dropped (logged, not
// panicked) while receivedGroupKeys - which is updated independently of
// dispatch - stays complete. The dropped notifications are expected to be
// re-delivered later by the reconcile worker's diff-sync, which is outside
// this test's scope.
func TestPendingQueueIsBounded(t *testing.T) {
	h := NewFuzzyWatchServiceListHolder("public")
	h.SetRequester(newFakeRequester())
	h.pendingLimitForTest(1)
	block := make(chan struct{})
	h.RegisterWatcher("public>>g>>svc*", func(model.FuzzyWatchChangeEvent) { <-block }, nil)
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("public@@g@@svc%d", i)
		h.HandleChangeNotify(key, constant.FUZZY_WATCH_CHANGED_TYPE_ADD_SERVICE)
	}
	ctx, _ := h.get("public>>g>>svc*")
	ctx.mu.Lock()
	pendingLen := len(ctx.pending)
	ctx.mu.Unlock()
	assert.LessOrEqual(t, pendingLen, 1, "pending must not grow past the bound")
	assert.Len(t, h.ReceivedGroupKeys("public>>g>>svc*"), 50, "state must be complete even when notifications drop")
	close(block)
}

// TestQueueDropsHealEvenWhenReconcileRunsMidDrain reproduces the P1 from
// review: while a drain is in flight, entry.syncedKeys is only an
// intermediate delivery state, so a reconcile pass that sees an empty diff
// mid-drain must not stamp the watcher's syncVersion as current. Sequence:
// an ADD blocks in the callback (syncedKeys not yet updated), a second ADD
// fills the bounded queue, two DELETEs revert receivedGroupKeys to empty but
// their tasks are dropped by the full queue. A reconcile pass at that moment
// sees both key sets empty; if it stamps the version, the post-drain pass
// skips the watcher and the dropped DELETEs are never replayed - breaking
// the guarantee that queue drops are healed by diff-sync.
func TestQueueDropsHealEvenWhenReconcileRunsMidDrain(t *testing.T) {
	h := NewFuzzyWatchServiceListHolder("public")
	h.SetRequester(newFakeRequester())
	h.pendingLimitForTest(1)

	sink := &eventSink{}
	var calls atomic.Int32
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	h.RegisterWatcher("public>>g>>svc*", func(ev model.FuzzyWatchChangeEvent) {
		sink.cb(ev)
		if calls.Add(1) == 1 {
			entered <- struct{}{}
			<-release
		}
	}, nil)
	ctx, ok := h.get("public>>g>>svc*")
	require.True(t, ok)

	// ADD svc1 starts delivering and blocks inside the callback, before
	// deliver records the key in syncedKeys.
	h.HandleChangeNotify("public@@g@@svc1", constant.FUZZY_WATCH_CHANGED_TYPE_ADD_SERVICE)
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("first ADD delivery never started")
	}
	// ADD svc2 fills the bounded queue (limit 1).
	h.HandleChangeNotify("public@@g@@svc2", constant.FUZZY_WATCH_CHANGED_TYPE_ADD_SERVICE)
	// Both DELETEs revert receivedGroupKeys to empty; their tasks are
	// dropped because the queue is full.
	h.HandleChangeNotify("public@@g@@svc1", constant.FUZZY_WATCH_CHANGED_TYPE_DELETE_SERVICE)
	h.HandleChangeNotify("public@@g@@svc2", constant.FUZZY_WATCH_CHANGED_TYPE_DELETE_SERVICE)

	// Reconcile pass mid-drain: receivedGroupKeys and syncedKeys are both
	// (transiently) empty, so the diff is empty. This must NOT stamp the
	// watcher's syncVersion, because deliveries are still in flight.
	ctx.mu.Lock()
	require.True(t, ctx.draining, "drain must still be in flight for this scenario")
	require.Empty(t, ctx.receivedGroupKeys)
	require.Empty(t, ctx.entries[0].syncedKeys)
	ctx.enqueueLocked(ctx.syncWatchersLocked()...)
	ctx.mu.Unlock()

	// Let the blocked ADD and the queued ADD deliver; syncedKeys now holds
	// both keys while receivedGroupKeys is empty.
	close(release)
	waitFor(t, func() bool {
		ctx.mu.Lock()
		defer ctx.mu.Unlock()
		return !ctx.draining && len(ctx.pending) == 0
	})

	// Subsequent reconcile passes must replay the dropped DELETEs as
	// diff-sync events and converge syncedKeys back to empty. Passes are
	// repeated like the bell-driven worker would (a diff task can itself be
	// dropped by the tight limit-1 queue; onDrop rings the bell and the next
	// pass heals the remainder).
	waitFor(t, func() bool {
		ctx.mu.Lock()
		defer ctx.mu.Unlock()
		if !ctx.draining {
			ctx.enqueueLocked(ctx.syncWatchersLocked()...)
		}
		return len(ctx.entries[0].syncedKeys) == 0
	})
	var deletes int
	for _, e := range sink.snapshot() {
		if e.ChangedType == constant.FUZZY_WATCH_CHANGED_TYPE_DELETE_SERVICE {
			deletes++
			assert.Equal(t, constant.FUZZY_WATCH_DIFF_SYNC_NOTIFY, e.SyncType)
		}
	}
	assert.Equal(t, 2, deletes, "both dropped DELETEs must be replayed by diff-sync")
}

// TestDeliverySkipsPerWatcherDuplicates asserts that deliver's per-watcher
// skip decision is driven by each watcher's own syncedKeys, not just
// pattern-level receivedGroupKeys: a manually queued duplicate ADD task
// (simulating a diff-sync racing a live push) must be skipped for a watcher
// that has already seen the key.
func TestDeliverySkipsPerWatcherDuplicates(t *testing.T) {
	h := NewFuzzyWatchServiceListHolder("public")
	h.SetRequester(newFakeRequester())
	var count atomic.Int32
	h.RegisterWatcher("public>>g>>svc*", func(model.FuzzyWatchChangeEvent) { count.Add(1) }, nil)
	h.HandleChangeNotify("public@@g@@svc1", constant.FUZZY_WATCH_CHANGED_TYPE_ADD_SERVICE)
	waitFor(t, func() bool { return count.Load() == 1 })
	ctx, _ := h.get("public>>g>>svc*")
	// Manually construct a duplicate ADD task (simulating a duplicate
	// produced by diff-sync racing a live push): deliver must skip it per
	// the target watcher's syncedKeys.
	ctx.mu.Lock()
	entries := append([]*watcherEntry(nil), ctx.entries...)
	ev := model.FuzzyWatchChangeEvent{ServiceName: "svc1", GroupName: "g", NamespaceId: "public",
		ChangedType: constant.FUZZY_WATCH_CHANGED_TYPE_ADD_SERVICE, SyncType: constant.FUZZY_WATCH_DIFF_SYNC_NOTIFY}
	ctx.enqueueLocked(notifyTask{event: &ev, targets: entries})
	ctx.mu.Unlock()
	time.Sleep(200 * time.Millisecond)
	assert.Equal(t, int32(1), count.Load(), "duplicate ADD must be skipped per watcher")
}

// TestDeliverySkipsDeleteForKeyWatcherNeverSaw asserts deliver's DELETE skip
// branch: a DELETE task must not invoke the callback for a watcher whose own
// syncedKeys never recorded the key, even though pattern-level
// receivedGroupKeys knew about it. A watcher ends up in exactly this state
// when its ADD delivery for the key was dropped by the bounded queue (see
// TestPendingQueueIsBounded); that scenario is reproduced directly here by
// enqueuing a DELETE task against a freshly registered entry that was never
// delivered the matching ADD, so its syncedKeys is still empty.
func TestDeliverySkipsDeleteForKeyWatcherNeverSaw(t *testing.T) {
	h := NewFuzzyWatchServiceListHolder("public")
	h.SetRequester(newFakeRequester())
	var count atomic.Int32
	h.RegisterWatcher("public>>g>>svc*", func(model.FuzzyWatchChangeEvent) { count.Add(1) }, nil)
	ctx, _ := h.get("public>>g>>svc*")

	ctx.mu.Lock()
	entries := append([]*watcherEntry(nil), ctx.entries...)
	require.Empty(t, entries[0].syncedKeys, "watcher must not have seen this key yet")
	ev := model.FuzzyWatchChangeEvent{ServiceName: "svc1", GroupName: "g", NamespaceId: "public",
		ChangedType: constant.FUZZY_WATCH_CHANGED_TYPE_DELETE_SERVICE, SyncType: constant.FUZZY_WATCH_DIFF_SYNC_NOTIFY}
	ctx.enqueueLocked(notifyTask{event: &ev, targets: entries})
	ctx.mu.Unlock()

	time.Sleep(200 * time.Millisecond)
	assert.Equal(t, int32(0), count.Load(), "DELETE for a key the watcher never saw ADD for must not fire")
}
