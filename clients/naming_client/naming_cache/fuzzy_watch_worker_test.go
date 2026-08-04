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
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nacos-group/nacos-sdk-go/v3/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v3/common/remote/rpc/rpc_request"
	"github.com/nacos-group/nacos-sdk-go/v3/model"
)

type fuzzyCall struct {
	pattern, watchType string
	receivedGroupKeys  []string
	isInitializing     bool
}

// fakeRequester scripts per-call errors and signals every send on a channel
// so tests wait deterministically instead of sleeping.
type fakeRequester struct {
	mu        sync.Mutex
	calls     []fuzzyCall
	script    []error // popped per call; empty => nil
	supported bool
	sent      chan fuzzyCall

	// blockCalls/release let a test pause SendFuzzyWatchRequest mid-flight -
	// after the call is recorded and visible on sent (so mustWatchCall can
	// observe it started), but before it returns - to deterministically
	// interleave a desired-state mutation (RemoveWatcherByID,
	// ResetConsistenceStatus, ...) with an in-flight RPC. blockCalls counts
	// down per call; release is closed by the test to unblock exactly the
	// calls it armed for blocking.
	blockCalls int
	release    chan struct{}
}

func newFakeRequester() *fakeRequester {
	return &fakeRequester{supported: true, sent: make(chan fuzzyCall, 64)}
}

func (f *fakeRequester) SendFuzzyWatchRequest(pattern, watchType string, keys []string, isInit bool) error {
	f.mu.Lock()
	call := fuzzyCall{pattern, watchType, append([]string(nil), keys...), isInit}
	f.calls = append(f.calls, call)
	var err error
	if len(f.script) > 0 {
		err = f.script[0]
		f.script = f.script[1:]
	}
	var wait chan struct{}
	if f.blockCalls > 0 {
		f.blockCalls--
		wait = f.release
	}
	f.mu.Unlock()
	f.sent <- call
	if wait != nil {
		<-wait
	}
	return err
}

func (f *fakeRequester) ServerSupportsFuzzyWatch() bool { return f.supported }

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time          { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *fakeClock) Advance(d time.Duration) { c.mu.Lock(); c.now = c.now.Add(d); c.mu.Unlock() }

func newTestWorkerHolder(t *testing.T, r FuzzyWatchRequester, clock *fakeClock) *FuzzyWatchServiceListHolder {
	t.Helper()
	h := NewFuzzyWatchServiceListHolder("public")
	h.pollInterval = 10 * time.Millisecond
	h.failureBackoff = time.Millisecond
	h.now = clock.Now
	h.SetRequester(r)
	h.Start()
	t.Cleanup(h.Shutdown)
	return h
}

func mustWatchCall(t *testing.T, r *fakeRequester) fuzzyCall {
	t.Helper()
	select {
	case c := <-r.sent:
		return c
	case <-time.After(2 * time.Second):
		t.Fatal("worker sent no request within 2s")
		return fuzzyCall{}
	}
}

// 注册是纯本地操作,RPC 只能来自 worker;WATCH 成功后 context 转 consistent,
// 不再重发(短暂等待窗口内无第二次调用)。
func TestRegisterIsLocalAndWorkerSendsWatch(t *testing.T) {
	r := newFakeRequester()
	h := newTestWorkerHolder(t, r, &fakeClock{now: time.Unix(0, 0)})
	id, err := h.RegisterWatcher("public>>g>>svc*", func(model.FuzzyWatchChangeEvent) {}, nil)
	require.NoError(t, err)
	require.NotZero(t, id)
	call := mustWatchCall(t, r)
	assert.Equal(t, constant.FUZZY_WATCH_TYPE_WATCH, call.watchType)
	assert.True(t, call.isInitializing, "first watch must request a full INIT sync")
	select {
	case c := <-r.sent:
		t.Fatalf("consistent pattern must not be re-sent, got %+v", c)
	case <-time.After(100 * time.Millisecond):
	}
}

// 场景1:WATCH 返回 over-limit —— 本地状态保留、OnLoadEvent 触发、后续服务端
// 推送仍被处理(失败不回滚)。
func TestOverLimitKeepsLocalStateAndNotifiesLoad(t *testing.T) {
	r := newFakeRequester()
	r.script = []error{&FuzzyWatchServerError{ErrorCode: constant.ERROR_CODE_FUZZY_WATCH_MATCH_COUNT_OVER_LIMIT, Message: "over limit"}}
	h := newTestWorkerHolder(t, r, &fakeClock{now: time.Unix(0, 0)})
	loads := make(chan model.FuzzyWatchLoadEvent, 1)
	events := make(chan model.FuzzyWatchChangeEvent, 1)
	_, err := h.RegisterWatcher("public>>g>>svc*",
		func(ev model.FuzzyWatchChangeEvent) { events <- ev },
		func(ev model.FuzzyWatchLoadEvent) { loads <- ev })
	require.NoError(t, err)
	mustWatchCall(t, r)
	select {
	case ev := <-loads:
		assert.Equal(t, constant.ERROR_CODE_FUZZY_WATCH_MATCH_COUNT_OVER_LIMIT, ev.ErrorCode)
	case <-time.After(2 * time.Second):
		t.Fatal("load event not delivered")
	}
	// 服务端已把 pattern 挂上并继续推送:本地必须仍能处理
	h.HandleSync("public>>g>>svc*", constant.FUZZY_WATCH_INIT_NOTIFY,
		[]rpc_request.NamingFuzzyWatchSyncContext{{ServiceKey: "public@@g@@svc1", ChangedType: constant.FUZZY_WATCH_CHANGED_TYPE_ADD_SERVICE}}, 1, 1)
	select {
	case ev := <-events:
		assert.Equal(t, "svc1", ev.ServiceName)
	case <-time.After(2 * time.Second):
		t.Fatal("push after failed WATCH must still be processed (no rollback)")
	}
}

// 场景4:瞬时失败(transport error)后 worker 退避重试,最终 consistent。
func TestTransientFailureRetriesUntilConsistent(t *testing.T) {
	r := newFakeRequester()
	r.script = []error{errors.New("connection reset")}
	h := newTestWorkerHolder(t, r, &fakeClock{now: time.Unix(0, 0)})
	_, err := h.RegisterWatcher("public>>g>>svc*", func(model.FuzzyWatchChangeEvent) {}, nil)
	require.NoError(t, err)
	first := mustWatchCall(t, r)
	second := mustWatchCall(t, r)
	assert.Equal(t, first.pattern, second.pattern, "worker must retry after transient failure")
	waitFor(t, func() bool {
		ctx, ok := h.get("public>>g>>svc*")
		if !ok {
			return false
		}
		ctx.mu.Lock()
		defer ctx.mu.Unlock()
		return ctx.consistentWithServer
	})
}

// 场景5:FINISH 前断连,重连后的 WATCH 仍带 isInitializing=true;FINISH 后转 false。
func TestIsInitializingSurvivesReconnectBeforeFinish(t *testing.T) {
	r := newFakeRequester()
	h := newTestWorkerHolder(t, r, &fakeClock{now: time.Unix(0, 0)})
	h.RegisterWatcher("public>>g>>svc*", func(model.FuzzyWatchChangeEvent) {}, nil)
	assert.True(t, mustWatchCall(t, r).isInitializing)
	h.ResetConsistenceStatus() // 模拟断连:FINISH 还没到
	assert.True(t, mustWatchCall(t, r).isInitializing, "reconnect before FINISH keeps initializing")
	h.HandleSync("public>>g>>svc*", constant.FINISH_FUZZY_WATCH_INIT_NOTIFY, nil, 1, 1)
	h.ResetConsistenceStatus()
	assert.False(t, mustWatchCall(t, r).isInitializing, "after FINISH the watch is no longer initializing")
}

// 最后一个 watcher 移除后 worker 发 CANCEL 并删除 context;
// CANCEL 在途期间复活(重新注册)则 context 保留并重新 WATCH(场景2雏形)。
func TestLastCancelSendsCancelAndRemovesPattern(t *testing.T) {
	r := newFakeRequester()
	h := newTestWorkerHolder(t, r, &fakeClock{now: time.Unix(0, 0)})
	id, _ := h.RegisterWatcher("public>>g>>svc*", func(model.FuzzyWatchChangeEvent) {}, nil)
	mustWatchCall(t, r)
	h.RemoveWatcherByID("public>>g>>svc*", id)
	call := mustWatchCall(t, r)
	assert.Equal(t, constant.FUZZY_WATCH_TYPE_CANCEL_WATCH, call.watchType)
	waitFor(t, func() bool { _, ok := h.get("public>>g>>svc*"); return !ok })
}

// MatchedServiceKeys 在 FINISH 前阻塞、可被 ctx 超时打断,FINISH 后返回快照。
func TestMatchedServiceKeysBlocksUntilFinish(t *testing.T) {
	r := newFakeRequester()
	h := newTestWorkerHolder(t, r, &fakeClock{now: time.Unix(0, 0)})
	h.RegisterWatcher("public>>g>>svc*", func(model.FuzzyWatchChangeEvent) {}, nil)
	shortCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := h.MatchedServiceKeys(shortCtx, "public>>g>>svc*")
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	h.HandleSync("public>>g>>svc*", constant.FUZZY_WATCH_INIT_NOTIFY,
		[]rpc_request.NamingFuzzyWatchSyncContext{{ServiceKey: "public@@g@@svc1", ChangedType: constant.FUZZY_WATCH_CHANGED_TYPE_ADD_SERVICE}}, 2, 1)
	h.HandleSync("public>>g>>svc*", constant.FINISH_FUZZY_WATCH_INIT_NOTIFY, nil, 2, 2)
	keys, err := h.MatchedServiceKeys(context.Background(), "public>>g>>svc*")
	require.NoError(t, err)
	assert.Equal(t, []string{"public@@g@@svc1"}, keys)
}

// ability 不支持:注册直接返错,不产生任何本地状态与 RPC。
func TestRegisterFailsFastWhenAbilityMissing(t *testing.T) {
	r := newFakeRequester()
	r.supported = false
	h := newTestWorkerHolder(t, r, &fakeClock{now: time.Unix(0, 0)})
	_, err := h.RegisterWatcher("public>>g>>svc*", func(model.FuzzyWatchChangeEvent) {}, nil)
	assert.ErrorIs(t, err, ErrFuzzyWatchNotSupported)
	_, ok := h.get("public>>g>>svc*")
	assert.False(t, ok)
}

// Regression for the lost-CANCEL race: a WATCH already in flight when the
// last watcher is removed must not clobber the pending CANCEL. The
// fakeRequester is armed to block the WATCH call after it is recorded (so
// mustWatchCall can observe it started) but before it returns, letting the
// test remove the watcher while the WATCH is still outstanding. Releasing it
// with a nil (success) error must not stop a CANCEL_WATCH from following
// promptly - specifically without waiting for an all-sync pass, since
// pollInterval/failureBackoff are the only timers this test's holder uses.
func TestRemovalDuringInFlightWatchStillSendsCancelPromptly(t *testing.T) {
	r := newFakeRequester()
	h := newTestWorkerHolder(t, r, &fakeClock{now: time.Unix(0, 0)})

	r.mu.Lock()
	r.blockCalls = 1
	r.release = make(chan struct{})
	r.mu.Unlock()

	id, err := h.RegisterWatcher("public>>g>>svc*", func(model.FuzzyWatchChangeEvent) {}, nil)
	require.NoError(t, err)

	firstCall := mustWatchCall(t, r)
	assert.Equal(t, constant.FUZZY_WATCH_TYPE_WATCH, firstCall.watchType)

	h.RemoveWatcherByID("public>>g>>svc*", id)
	close(r.release) // let the in-flight (now stale) WATCH return success

	cancelCall := mustWatchCall(t, r)
	assert.Equal(t, constant.FUZZY_WATCH_TYPE_CANCEL_WATCH, cancelCall.watchType,
		"pending removal must not be starved behind a stale successful WATCH")
	waitFor(t, func() bool { _, ok := h.get("public>>g>>svc*"); return !ok })
}

// Regression for the lost-reconnect-reset race: a WATCH already in flight
// when ResetConsistenceStatus fires (simulating a reconnect) must not mark
// the pattern consistent once it returns - the connection it succeeded on is
// gone. A fresh WATCH must follow promptly on the new connection.
func TestResetDuringInFlightWatchStillResendsPromptly(t *testing.T) {
	r := newFakeRequester()
	h := newTestWorkerHolder(t, r, &fakeClock{now: time.Unix(0, 0)})

	r.mu.Lock()
	r.blockCalls = 1
	r.release = make(chan struct{})
	r.mu.Unlock()

	_, err := h.RegisterWatcher("public>>g>>svc*", func(model.FuzzyWatchChangeEvent) {}, nil)
	require.NoError(t, err)

	firstCall := mustWatchCall(t, r)
	assert.Equal(t, constant.FUZZY_WATCH_TYPE_WATCH, firstCall.watchType)

	h.ResetConsistenceStatus()
	close(r.release) // let the stale (pre-reconnect) WATCH return success

	secondCall := mustWatchCall(t, r)
	assert.Equal(t, constant.FUZZY_WATCH_TYPE_WATCH, secondCall.watchType,
		"a stale WATCH success must not suppress the post-reconnect resend")

	waitFor(t, func() bool {
		ctx, ok := h.get("public>>g>>svc*")
		if !ok {
			return false
		}
		ctx.mu.Lock()
		defer ctx.mu.Unlock()
		return ctx.consistentWithServer
	})
}

// A watcher registered while a pattern's CANCEL is in flight must revive the
// pattern rather than let the confirmed CANCEL delete it out from under the
// new registration. Kills the removePatternIfDiscarded mutant that always
// removes on a successful CANCEL response regardless of current state
// (`remove := true`): under that mutant this test's final WATCH never
// arrives because the pattern (and its freshly appended entry) would have
// been deleted from the holder before the revive could take effect.
func TestReviveDuringCancelInFlightKeepsPatternAndResendsWatch(t *testing.T) {
	r := newFakeRequester()
	h := newTestWorkerHolder(t, r, &fakeClock{now: time.Unix(0, 0)})

	id, err := h.RegisterWatcher("public>>g>>svc*", func(model.FuzzyWatchChangeEvent) {}, nil)
	require.NoError(t, err)
	first := mustWatchCall(t, r)
	assert.Equal(t, constant.FUZZY_WATCH_TYPE_WATCH, first.watchType)

	r.mu.Lock()
	r.blockCalls = 1
	r.release = make(chan struct{})
	r.mu.Unlock()

	h.RemoveWatcherByID("public>>g>>svc*", id)
	cancelCall := mustWatchCall(t, r)
	assert.Equal(t, constant.FUZZY_WATCH_TYPE_CANCEL_WATCH, cancelCall.watchType)

	// Revive while the CANCEL response is still pending.
	_, err = h.RegisterWatcher("public>>g>>svc*", func(model.FuzzyWatchChangeEvent) {}, nil)
	require.NoError(t, err)

	close(r.release) // let the CANCEL_WATCH call return success (nil)

	// The pattern must survive the confirmed CANCEL and get re-WATCHed.
	call := mustWatchCall(t, r)
	assert.Equal(t, constant.FUZZY_WATCH_TYPE_WATCH, call.watchType, "revived pattern must be re-WATCHed")
	_, ok := h.get("public>>g>>svc*")
	assert.True(t, ok, "pattern must not be removed once revived before the CANCEL was confirmed")
}

// A pattern with one of two watchers removed must never be canceled: discard
// only flips once the LAST watcher goes. Forces a re-send via
// ResetConsistenceStatus (so the assertion isn't just "no RPC happened yet")
// and asserts the re-send is a WATCH.
func TestPartialRemovalNeverSendsCancelEvenAfterReset(t *testing.T) {
	r := newFakeRequester()
	h := newTestWorkerHolder(t, r, &fakeClock{now: time.Unix(0, 0)})

	id1, err := h.RegisterWatcher("public>>g>>svc*", func(model.FuzzyWatchChangeEvent) {}, nil)
	require.NoError(t, err)
	first := mustWatchCall(t, r)
	assert.Equal(t, constant.FUZZY_WATCH_TYPE_WATCH, first.watchType)

	_, err = h.RegisterWatcher("public>>g>>svc*", func(model.FuzzyWatchChangeEvent) {}, nil)
	require.NoError(t, err)

	h.RemoveWatcherByID("public>>g>>svc*", id1)

	// Observation window: nothing but healing (no RPC) should happen for a
	// pattern that's still consistent and not discarded.
	select {
	case c := <-r.sent:
		t.Fatalf("partial removal must not trigger any RPC, got %+v", c)
	case <-time.After(100 * time.Millisecond):
	}

	h.ResetConsistenceStatus()
	call := mustWatchCall(t, r)
	assert.Equal(t, constant.FUZZY_WATCH_TYPE_WATCH, call.watchType,
		"a pattern with a surviving watcher must never be CANCELed")
}

// TestCancelGateRequiresNonEmptyEntriesCheck directly exercises the
// WATCH/CANCEL_WATCH gate in executeSync by fabricating a context in the
// discard=true / entries-non-empty combination - unreachable through the
// public API (RemoveWatcherByID only ever sets discard alongside an empty
// entries slice, atomically under ctx.mu), but this is exactly the
// combination that distinguishes the full gate (`ctx.discard &&
// len(ctx.entries) == 0`) from a mutant that drops the length check
// (`ctx.discard` alone): the mutant would send CANCEL_WATCH here since
// discard is true, while the correct gate must send WATCH since a watcher is
// still registered.
func TestCancelGateRequiresNonEmptyEntriesCheck(t *testing.T) {
	r := newFakeRequester()
	h := newTestWorkerHolder(t, r, &fakeClock{now: time.Unix(0, 0)})

	h.mu.Lock()
	ctx := newFuzzyWatchContext("public>>g>>svc*", h)
	ctx.discard = true
	ctx.entries = []*watcherEntry{{id: 1, fn: func(model.FuzzyWatchChangeEvent) {}, syncedKeys: map[string]struct{}{}}}
	h.patterns.Set("public>>g>>svc*", ctx)
	h.mu.Unlock()

	call := mustWatchCall(t, r)
	assert.Equal(t, constant.FUZZY_WATCH_TYPE_WATCH, call.watchType,
		"discard alone must not trigger CANCEL_WATCH while entries is non-empty")
}
