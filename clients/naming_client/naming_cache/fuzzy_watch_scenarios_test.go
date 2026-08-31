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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nacos-group/nacos-sdk-go/v3/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v3/common/remote/rpc/rpc_request"
	"github.com/nacos-group/nacos-sdk-go/v3/model"
)

// Rapid cancel/re-register churn on the same pattern must converge: the last
// registration standing always wins, and it ends up active and consistent
// with a working subscription - none of the interleaved re-registrations are
// silently lost behind an in-flight CANCEL/WATCH.
func TestConcurrentRegisterAndCancelConverges(t *testing.T) {
	r := newFakeRequester()
	h := newTestWorkerHolder(t, r, &fakeClock{now: time.Unix(0, 0)})
	const pattern = "public>>g>>svc*"
	id, err := h.RegisterWatcher(pattern, func(model.FuzzyWatchChangeEvent) {}, nil)
	require.NoError(t, err)
	for i := 0; i < 20; i++ {
		h.RemoveWatcherByID(pattern, id)
		id, err = h.RegisterWatcher(pattern, func(model.FuzzyWatchChangeEvent) {}, nil)
		require.NoError(t, err, "re-registration during in-flight cancel must never be lost")
	}
	got := make(chan model.FuzzyWatchChangeEvent, 1)
	final, err := h.RegisterWatcher(pattern, func(ev model.FuzzyWatchChangeEvent) { got <- ev }, nil)
	require.NoError(t, err)
	_ = final
	waitFor(t, func() bool {
		ctx, ok := h.get(pattern)
		if !ok {
			return false
		}
		ctx.mu.Lock()
		defer ctx.mu.Unlock()
		return ctx.consistentWithServer && !ctx.discard
	})
	h.HandleChangeNotify("public@@g@@svc1", constant.FUZZY_WATCH_CHANGED_TYPE_ADD_SERVICE)
	select {
	case <-got:
	case <-time.After(2 * time.Second):
		t.Fatal("watcher registered during churn lost its subscription")
	}
	_ = id
}

// A reconnect resync (which marks the pattern inconsistent, forcing a fresh
// WATCH) racing the removal of the last watcher (which wants a CANCEL) must
// resolve to a single desired state: once CANCEL is confirmed the pattern is
// gone for good, with no later ghost WATCH resurrecting it.
func TestReconnectResyncRacingCancelLeavesNoGhost(t *testing.T) {
	r := newFakeRequester()
	h := newTestWorkerHolder(t, r, &fakeClock{now: time.Unix(0, 0)})
	const pattern = "public>>g>>svc*"
	id, _ := h.RegisterWatcher(pattern, func(model.FuzzyWatchChangeEvent) {}, nil)
	assert.Equal(t, constant.FUZZY_WATCH_TYPE_WATCH, mustWatchCall(t, r).watchType)
	h.ResetConsistenceStatus()       // simulate reconnect: pattern needs resync
	h.RemoveWatcherByID(pattern, id) // cancel racing the resync
	// Once a CANCEL succeeds, the pattern must vanish, and no WATCH for it may
	// appear within the following observation window.
	deadline := time.Now().Add(2 * time.Second)
	for {
		select {
		case c := <-r.sent:
			if c.watchType == constant.FUZZY_WATCH_TYPE_CANCEL_WATCH {
				waitFor(t, func() bool { _, ok := h.get(pattern); return !ok })
				select {
				case ghost := <-r.sent:
					assert.NotEqual(t, constant.FUZZY_WATCH_TYPE_WATCH, ghost.watchType,
						"pattern re-watched after confirmed cancel: ghost watch")
				case <-time.After(300 * time.Millisecond):
				}
				return
			}
		case <-time.After(time.Until(deadline)):
			t.Fatal("cancel was never sent")
		}
	}
}

// A pattern that hits the server's match-count limit only after it was
// already registered and consistent must still notify OnLoadEvent once per
// suppression window, and resume notifying once the window elapses.
//
// pollInterval is set well above the 150ms "no repeat notification"
// observation window used below (unlike newTestWorkerHolder's default 10ms):
// once over-limit clears consistentWithServer, the worker retries every
// pollInterval with no back-off, and a poll cadence shorter than the
// observation window would let the background retries race ahead and drain
// the finite error script before the test can observe each step.
func TestMatchCountLimitAfterInitialSuccess(t *testing.T) {
	r := newFakeRequester()
	clock := &fakeClock{now: time.Unix(0, 0)}
	h := NewFuzzyWatchServiceListHolder("public")
	h.pollInterval = 300 * time.Millisecond
	h.failureBackoff = time.Millisecond
	h.allSyncInterval = 50 * time.Millisecond
	h.overLimitSuppress = time.Hour
	h.now = clock.Now
	h.SetRequester(r)
	h.Start()
	t.Cleanup(h.Shutdown)

	const pattern = "public>>g>>svc*"
	overLimitErr := func() error {
		return &FuzzyWatchServerError{ErrorCode: constant.ERROR_CODE_FUZZY_WATCH_MATCH_COUNT_OVER_LIMIT, Message: "over"}
	}
	setScript := func(errs ...error) {
		r.mu.Lock()
		r.script = errs
		r.mu.Unlock()
	}
	waitConsistent := func() {
		waitFor(t, func() bool {
			ctx, ok := h.get(pattern)
			if !ok {
				return false
			}
			ctx.mu.Lock()
			defer ctx.mu.Unlock()
			return ctx.consistentWithServer
		})
	}

	loads := make(chan model.FuzzyWatchLoadEvent, 4)
	h.RegisterWatcher(pattern, func(model.FuzzyWatchChangeEvent) {},
		func(ev model.FuzzyWatchLoadEvent) { loads <- ev })
	mustWatchCall(t, r) // initial WATCH succeeds -> consistent

	setScript(overLimitErr())
	clock.Advance(time.Minute) // cross allSyncInterval -> full sync re-sends WATCH
	mustWatchCall(t, r)
	select {
	case ev := <-loads:
		assert.Equal(t, constant.ERROR_CODE_FUZZY_WATCH_MATCH_COUNT_OVER_LIMIT, ev.ErrorCode)
	case <-time.After(2 * time.Second):
		t.Fatal("load event after post-registration over-limit not delivered")
	}
	// Within the suppression window (1h) a repeat over-limit must not notify again.
	setScript(overLimitErr())
	mustWatchCall(t, r)
	select {
	case <-loads:
		t.Fatal("suppression window violated")
	case <-time.After(150 * time.Millisecond):
	}

	// An intervening successful WATCH clears the suppression timestamp, so an
	// over-limit that follows notifies again well inside what would otherwise
	// still be the old suppression window.
	setScript() // next send succeeds
	mustWatchCall(t, r)
	waitConsistent()
	clock.Advance(time.Minute) // cross allSyncInterval -> force a resync now that the pattern is consistent
	setScript(overLimitErr())
	mustWatchCall(t, r)
	select {
	case ev := <-loads:
		assert.Equal(t, constant.ERROR_CODE_FUZZY_WATCH_MATCH_COUNT_OVER_LIMIT, ev.ErrorCode)
	case <-time.After(2 * time.Second):
		t.Fatal("load event not re-delivered after an intervening success reset the suppression window")
	}

	// Back inside a fresh suppression window, a repeat over-limit is silent again...
	setScript(overLimitErr())
	mustWatchCall(t, r)
	select {
	case <-loads:
		t.Fatal("suppression window violated")
	case <-time.After(150 * time.Millisecond):
	}
	// ...until the clock crosses the window, at which point it notifies again.
	clock.Advance(2 * time.Hour)
	setScript(overLimitErr())
	mustWatchCall(t, r)
	select {
	case <-loads:
	case <-time.After(2 * time.Second):
		t.Fatal("load event after suppression window not re-delivered")
	}
}

// Two watchers on the same pattern: canceling one must not disturb the
// other's live subscription, and the server-side CANCEL_WATCH RPC must only
// be sent once the last watcher goes - never while any watcher remains.
func TestWatcherScopedCancelKeepsOtherWatcher(t *testing.T) {
	r := newFakeRequester()
	h := newTestWorkerHolder(t, r, &fakeClock{now: time.Unix(0, 0)})
	const pattern = "public>>g>>svc*"
	got1 := make(chan model.FuzzyWatchChangeEvent, 4)
	got2 := make(chan model.FuzzyWatchChangeEvent, 4)
	id1, _ := h.RegisterWatcher(pattern, func(ev model.FuzzyWatchChangeEvent) { got1 <- ev }, nil)
	id2, _ := h.RegisterWatcher(pattern, func(ev model.FuzzyWatchChangeEvent) { got2 <- ev }, nil)
	mustWatchCall(t, r)
	h.RemoveWatcherByID(pattern, id1)
	h.HandleChangeNotify("public@@g@@svc1", constant.FUZZY_WATCH_CHANGED_TYPE_ADD_SERVICE)
	select {
	case <-got2:
	case <-time.After(2 * time.Second):
		t.Fatal("remaining watcher stopped receiving after sibling cancel")
	}
	select {
	case c := <-r.sent:
		t.Fatalf("no server RPC may be sent while watchers remain, got %+v", c)
	case <-time.After(150 * time.Millisecond):
	}
	h.RemoveWatcherByID(pattern, id2)
	assert.Equal(t, constant.FUZZY_WATCH_TYPE_CANCEL_WATCH, mustWatchCall(t, r).watchType)
	select {
	case ev := <-got1:
		t.Fatalf("canceled watcher must not receive new events, got %+v", ev)
	default:
	}
}

// A pattern whose dispatch queue is capacity 1 will drop notifications once a
// slow callback blocks the drain goroutine; once the callback unblocks, the
// reconcile worker's diff-sync healing must still deliver the full matched
// set to the watcher, even though individual pushes were dropped in transit.
func TestBackpressureDropHealsViaDiffSync(t *testing.T) {
	r := newFakeRequester()
	h := NewFuzzyWatchServiceListHolder("public")
	h.pendingLimitForTest(1)
	h.pollInterval = 10 * time.Millisecond
	h.failureBackoff = time.Millisecond
	h.now = time.Now
	h.SetRequester(r)
	h.Start()
	t.Cleanup(h.Shutdown)
	var mu sync.Mutex
	seen := map[string]struct{}{}
	block := make(chan struct{})
	first := true
	h.RegisterWatcher("public>>g>>svc*", func(ev model.FuzzyWatchChangeEvent) {
		if first {
			first = false
			<-block // stall the drain goroutine so later notifications get dropped
		}
		mu.Lock()
		seen[ev.ServiceName] = struct{}{}
		mu.Unlock()
	}, nil)
	mustWatchCall(t, r)
	for i := 0; i < 20; i++ {
		h.HandleChangeNotify(fmt.Sprintf("public@@g@@svc%d", i), constant.FUZZY_WATCH_CHANGED_TYPE_ADD_SERVICE)
	}
	close(block)
	waitFor(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(seen) == 20 // dropped notifications are healed by diff-sync
	})
}

// The re-sent WATCH after a reconnect must carry the pattern's full
// receivedGroupKeys snapshot (not an empty or stale one), since that is what
// lets the server compute an accurate diff against what the client already
// knows.
func TestResendCarriesReceivedGroupKeysSnapshot(t *testing.T) {
	r := newFakeRequester()
	h := newTestWorkerHolder(t, r, &fakeClock{now: time.Unix(0, 0)})
	const pattern = "public>>g>>svc*"
	_, err := h.RegisterWatcher(pattern, func(model.FuzzyWatchChangeEvent) {}, nil)
	require.NoError(t, err)
	first := mustWatchCall(t, r)
	assert.Empty(t, first.receivedGroupKeys, "first WATCH has no known matches yet")

	h.HandleSync(pattern, constant.FUZZY_WATCH_INIT_NOTIFY, []rpc_request.NamingFuzzyWatchSyncContext{
		{ServiceKey: "public@@g@@svc1", ChangedType: constant.FUZZY_WATCH_CHANGED_TYPE_ADD_SERVICE},
		{ServiceKey: "public@@g@@svc2", ChangedType: constant.FUZZY_WATCH_CHANGED_TYPE_ADD_SERVICE},
	}, 1, 1)

	waitFor(t, func() bool {
		ctx, ok := h.get(pattern)
		if !ok {
			return false
		}
		ctx.mu.Lock()
		defer ctx.mu.Unlock()
		return ctx.consistentWithServer
	})

	h.ResetConsistenceStatus()
	resend := mustWatchCall(t, r)
	assert.Equal(t, constant.FUZZY_WATCH_TYPE_WATCH, resend.watchType)
	assert.ElementsMatch(t, []string{"public@@g@@svc1", "public@@g@@svc2"}, resend.receivedGroupKeys,
		"resend must carry the current receivedGroupKeys snapshot")
}
