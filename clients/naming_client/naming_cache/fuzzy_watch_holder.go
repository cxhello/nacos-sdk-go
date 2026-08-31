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
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"

	"github.com/nacos-group/nacos-sdk-go/v3/clients/cache"
	"github.com/nacos-group/nacos-sdk-go/v3/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v3/common/logger"
	"github.com/nacos-group/nacos-sdk-go/v3/common/remote/rpc/rpc_request"
	"github.com/nacos-group/nacos-sdk-go/v3/model"
)

// watcherEntry pairs a user callback with the id RegisterWatcher handed back
// to its caller, plus the per-watcher delivery state: syncedKeys is this
// watcher's own view of which serviceKeys it has been told ADD (so a
// duplicate ADD task - e.g. one that raced a diff-sync against a live push -
// can be skipped for this watcher specifically, and a DELETE for a key it
// was never told about is skipped too), and syncVersion tracks the
// ctx.syncVersion this watcher has caught up to (consumed by the reconcile
// worker, not this file). The id, not the callback's code pointer, is what
// identifies a registration: two distinct method values sharing a receiver
// type can share a code pointer in Go, so reflect.ValueOf(fn).Pointer() is
// not a valid way to tell two registrations apart.
type watcherEntry struct {
	id          uint64
	fn          func(model.FuzzyWatchChangeEvent)
	onLoadEvent func(model.FuzzyWatchLoadEvent) // nil until the reconcile worker wires it
	syncedKeys  map[string]struct{}             // guarded by ctx.mu
	syncVersion uint64

	// removed flips true when RemoveWatcherByID takes this entry out of
	// ctx.entries (guarded by ctx.mu). notifyTask snapshots entry pointers at
	// enqueue time, so a task can still be sitting in the dispatch queue when
	// its watcher is canceled; deliver re-checks this flag immediately before
	// invoking the callback so SDK-owned pending work never invokes a
	// registration after cancellation returned. A callback already executing
	// cannot be recalled - only queued, not-yet-started deliveries are cut off.
	removed bool
}

// notifyTask bundles one fired event (or, once the reconcile worker is
// wired, one load-progress event) together with the watcher snapshot that
// must observe it. Queuing whole tasks (rather than events and targets
// separately) is what lets enqueueLocked/drain preserve per-pattern delivery
// order across multiple HandleSync/HandleChangeNotify calls. event and
// loadEvent are mutually exclusive; deliver branches on which is set.
type notifyTask struct {
	event     *model.FuzzyWatchChangeEvent
	loadEvent *model.FuzzyWatchLoadEvent
	targets   []*watcherEntry
}

// fuzzyWatchContext is the per-pattern state: the pattern string itself, the
// set of serviceKeys the client has been told match it (receivedGroupKeys),
// the registered watchers (entries), a monotonically increasing syncVersion
// bumped on every successful applyChange (for the reconcile worker's
// diff-sync version comparison), whether the initial batch sync has
// finished, and the async dispatch queue (pending/draining, bounded by
// pendingLimit). Its own mutex guards all of it.
//
// The pending queue is bounded by pendingLimit (default 1024, copied in from
// the holder by newFuzzyWatchContext). When a pattern's callbacks fall
// behind and the queue fills, enqueueLocked drops the new notification
// rather than growing pending without bound. A dropped task never marks its
// key as seen in the affected watcher's syncedKeys - that only advances on
// actual delivery - while receivedGroupKeys stays complete independently of
// dispatch, since applyChange updates it before the enqueue. So a watcher
// that missed a delivery is detectable after the fact: its syncedKeys will
// differ from receivedGroupKeys for that key. That gap is not re-delivered
// here; it is healed out-of-band by the reconcile worker's
// syncWatchersLocked (fuzzy_watch_worker.go), which diffs a lagging
// watcher's syncedKeys against receivedGroupKeys and replays the missing
// ADD/DELETE events.
type fuzzyWatchContext struct {
	pattern           string
	receivedGroupKeys map[string]struct{}
	entries           []*watcherEntry
	initialized       bool
	syncVersion       uint64

	// consistentWithServer is true once the reconcile worker's most recent
	// WATCH RPC for this pattern succeeded AND desired state has not changed
	// since the snapshot that RPC was sent from - the "has not changed since"
	// half is enforced by epoch (see below), not by consistentWithServer
	// alone: a WATCH send races against concurrent desired-state mutations,
	// so its success is only applied if epoch is still the value read before
	// the send. The worker skips re-sending WATCH while consistentWithServer
	// is true, UNLESS the pattern is also discard && empty - a pattern
	// pending CANCEL must never be short-circuited by that fast path, or the
	// CANCEL is starved until the next full resync. ResetConsistenceStatus
	// (on reconnect), RemoveWatcherByID's last-watcher-removed path, and
	// notifyOverLimit's capacity rejection all clear consistentWithServer to
	// force a re-send. discard marks desired state as "not watched": set
	// when the last watcher is removed, cleared if a new watcher revives the
	// pattern before the in-flight CANCEL is confirmed. initDone is closed
	// exactly once, by HandleSync on FINISH_FUZZY_WATCH_INIT_NOTIFY, and lets
	// MatchedServiceKeys block until the initial batch sync completes.
	// lastOverLimitNotify timestamps the last capacity-rejection load event
	// fired for this pattern, so the worker can suppress repeats within
	// overLimitSuppress. epoch increments on every desired-state mutation
	// (reconnect reset, a discard flip in either direction) and lets
	// executeSync detect, when a WATCH RPC returns, whether the desired
	// state it read before sending is still current - if not, the response
	// must not flip consistentWithServer to true, since a newer desired
	// state needs its own send.
	consistentWithServer bool
	discard              bool
	initDone             chan struct{}
	lastOverLimitNotify  time.Time
	epoch                uint64

	// pending/draining implement the async dispatcher: HandleSync and
	// HandleChangeNotify enqueue tasks while holding mu, then release it.
	// Callbacks run later, on the single drain goroutine, never on the
	// caller's goroutine and never while mu is held - so a user callback
	// that blocks or re-enters the holder cannot deadlock or stall the
	// gRPC push-handler goroutine that must ACK the server request.
	pending      []notifyTask
	draining     bool
	pendingLimit int

	// onDrop, when set, is invoked once for every notification enqueueLocked
	// drops for exceeding pendingLimit. It is called synchronously with c.mu
	// held: it must be non-blocking and must not re-enter the holder or take
	// any holder/context lock (h.mu, or any fuzzyWatchContext's mu including
	// this one) - taking ctx.mu recursively self-deadlocks, and taking h.mu
	// inverts the holder's h.mu -> ctx.mu lock order (see RegisterWatcher).
	// newFuzzyWatchContext wires this to h.Bell, which satisfies the contract:
	// a non-blocking channel send that touches no lock.
	onDrop func()

	mu sync.Mutex
}

// newFuzzyWatchContext creates the per-pattern dispatch state, carrying the
// holder's configured queue bound onto the context so enqueueLocked doesn't
// need to reach back into the holder on every call. onDrop is wired to
// h.Bell, which satisfies the non-blocking, lock-free contract documented on
// the field: a dropped notification wakes the reconcile worker so it can
// heal the resulting per-watcher lag via syncWatchersLocked. initDone is
// created open and closed exactly once by HandleSync on FINISH.
func newFuzzyWatchContext(pattern string, h *FuzzyWatchServiceListHolder) *fuzzyWatchContext {
	return &fuzzyWatchContext{
		pattern:           pattern,
		receivedGroupKeys: make(map[string]struct{}),
		pendingLimit:      h.pendingLimit,
		onDrop:            h.Bell,
		initDone:          make(chan struct{}),
	}
}

func (c *fuzzyWatchContext) snapshotEntriesLocked() []*watcherEntry {
	out := make([]*watcherEntry, len(c.entries))
	copy(out, c.entries)
	return out
}

// enqueueLocked appends tasks, dropping any that would push pending past
// pendingLimit, and starts a drain goroutine when none is running. Caller
// must hold c.mu. Per-pattern ordering is preserved because at most one
// drain goroutine exists per context; callbacks therefore never run on (and
// never block) the RPC request-handler goroutine.
func (c *fuzzyWatchContext) enqueueLocked(tasks ...notifyTask) {
	if len(tasks) == 0 {
		return
	}
	for _, task := range tasks {
		if c.pendingLimit > 0 && len(c.pending) >= c.pendingLimit {
			// receivedGroupKeys was already updated for this key before the
			// enqueue (applyChange runs first), so dropping here does not
			// corrupt pattern-level state. It does leave a gap: the affected
			// watcher's syncedKeys only advances on actual delivery, so it
			// will now lag receivedGroupKeys for this key. The reconcile
			// worker's syncWatchersLocked (fuzzy_watch_worker.go) heals this
			// on its next pass. onDrop is only an observation hook - see its
			// field comment for the contract it must honor.
			logger.Warnf("fuzzy watch pattern:%s dispatch queue full (%d), dropping notification", c.pattern, c.pendingLimit)
			if c.onDrop != nil {
				c.onDrop()
			}
			continue
		}
		c.pending = append(c.pending, task)
	}
	if len(c.pending) > 0 && !c.draining {
		c.draining = true
		go c.drain()
	}
}

func (c *fuzzyWatchContext) drain() {
	for {
		c.mu.Lock()
		if len(c.pending) == 0 {
			c.draining = false
			c.mu.Unlock()
			return
		}
		batch := c.pending
		c.pending = nil
		c.mu.Unlock()
		for _, task := range batch {
			c.deliver(task)
		}
	}
}

// deliver dispatches one task to its snapshotted targets. The per-watcher
// ADD/DELETE skip decision and the syncedKeys update happen at delivery time
// under c.mu (not enqueue time), so queued ADD/DELETE sequences for the same
// key resolve in order. A task dropped by the queue bound (see enqueueLocked)
// never reaches here, so it never marks the key as seen in syncedKeys - that
// watcher's syncedKeys is left lagging receivedGroupKeys for this key until
// the reconcile worker's syncWatchersLocked heals it.
func (c *fuzzyWatchContext) deliver(task notifyTask) {
	if task.event == nil && task.loadEvent == nil {
		return
	}
	for _, entry := range task.targets {
		if task.loadEvent != nil {
			c.mu.Lock()
			removed := entry.removed
			c.mu.Unlock()
			if !removed && entry.onLoadEvent != nil {
				safeInvoke(func() { entry.onLoadEvent(*task.loadEvent) })
			}
			continue
		}
		ev := *task.event
		key := buildServiceKey(ev.NamespaceId, ev.GroupName, ev.ServiceName)
		c.mu.Lock()
		skip := entry.removed
		if !skip {
			switch ev.ChangedType {
			case constant.FUZZY_WATCH_CHANGED_TYPE_ADD_SERVICE:
				_, seen := entry.syncedKeys[key]
				skip = seen
			case constant.FUZZY_WATCH_CHANGED_TYPE_DELETE_SERVICE:
				_, seen := entry.syncedKeys[key]
				skip = !seen
			}
		}
		c.mu.Unlock()
		if skip {
			continue
		}
		// syncedKeys only advances when the callback completed normally: a
		// panicking callback leaves the key unsynced so the reconcile worker's
		// next diff-sync pass re-delivers it (Java parity: syncServiceKeys is
		// updated only after onEvent returns).
		if !safeInvoke(func() { entry.fn(ev) }) {
			continue
		}
		c.mu.Lock()
		switch ev.ChangedType {
		case constant.FUZZY_WATCH_CHANGED_TYPE_ADD_SERVICE:
			entry.syncedKeys[key] = struct{}{}
		case constant.FUZZY_WATCH_CHANGED_TYPE_DELETE_SERVICE:
			delete(entry.syncedKeys, key)
		}
		c.mu.Unlock()
	}
}

// safeInvoke isolates user-callback panics: a panicking watcher must not kill
// the process or starve other watchers sharing the drain goroutine. It reports
// whether fn completed without panicking, so deliver can withhold the
// delivered-state update for a failed callback.
func safeInvoke(fn func()) (completed bool) {
	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("fuzzy watch callback panic recovered: %v\n%s", r, string(debug.Stack()))
		}
	}()
	fn()
	return true
}

func buildServiceKey(namespace, group, service string) string {
	return namespace + constant.SERVICE_INFO_SPLITER + group + constant.SERVICE_INFO_SPLITER + service
}

// applyChange mutates receivedGroupKeys for a single serviceKey and reports
// the event to fire, if any. Called with c.mu held. An ADD for a key already
// known, or a DELETE for a key not known, is a no-op that fires nothing -
// this is what makes redo/duplicate syncs idempotent. syncType is copied
// verbatim onto the returned event so a callback can distinguish an initial
// batch sync from a post-init change notify. syncVersion is bumped on every
// actual change so the reconcile worker can tell a stale watcher apart from
// one that is caught up.
func (c *fuzzyWatchContext) applyChange(serviceKey, namespace, group, service, changedType, syncType string) (model.FuzzyWatchChangeEvent, bool) {
	switch changedType {
	case constant.FUZZY_WATCH_CHANGED_TYPE_ADD_SERVICE:
		if _, ok := c.receivedGroupKeys[serviceKey]; ok {
			return model.FuzzyWatchChangeEvent{}, false
		}
		c.receivedGroupKeys[serviceKey] = struct{}{}
	case constant.FUZZY_WATCH_CHANGED_TYPE_DELETE_SERVICE:
		if _, ok := c.receivedGroupKeys[serviceKey]; !ok {
			return model.FuzzyWatchChangeEvent{}, false
		}
		delete(c.receivedGroupKeys, serviceKey)
	default:
		logger.Warnf("fuzzy watch ignores unknown changedType:%s serviceKey:%s", changedType, serviceKey)
		return model.FuzzyWatchChangeEvent{}, false
	}
	c.syncVersion++
	return model.FuzzyWatchChangeEvent{
		ServiceName: service,
		GroupName:   group,
		NamespaceId: namespace,
		ChangedType: changedType,
		SyncType:    syncType,
	}, true
}

// FuzzyWatchServiceListHolder tracks every fuzzy watch pattern the client has
// registered and drives the server-push state machine that keeps each
// pattern's matched-service set in sync. It is shared between the client API
// (which registers/removes patterns and callbacks), the gRPC push handlers
// (which feed it sync / change-notify messages) and the redo listener (which
// reads back receivedGroupKeys on reconnect).
type FuzzyWatchServiceListHolder struct {
	namespace string
	patterns  cache.ConcurrentMap // groupKeyPattern -> *fuzzyWatchContext
	mu        sync.Mutex          // serializes pattern create/remove

	// nextCallbackID is an atomic counter handed out by RegisterWatcher so
	// every registration - even a repeat registration of the same func
	// value - gets its own identity to cancel by. atomic.Uint64 (rather than
	// a plain uint64 used with atomic.AddUint64) keeps its own alignment
	// guarantee, avoiding the 32-bit "unaligned 64-bit atomic operation"
	// panic that a plain uint64 field risks on GOARCH=386/arm.
	nextCallbackID atomic.Uint64

	// requester is the transport the reconcile worker sends WATCH/CANCEL_WATCH
	// RPCs through. It is nil until SetRequester is called (mirrors Java
	// registerNamingGrpcClientProxy, which runs after construction to avoid an
	// import cycle), so the worker loop tolerates a nil requester as a no-op.
	requester   FuzzyWatchRequester
	requesterMu sync.RWMutex

	bell     chan struct{} // cap 1: rings coalesce naturally
	stopCh   chan struct{}
	stopOnce sync.Once
	started  atomic.Bool

	// Reconcile cadence. Fields (not consts) so tests inject a fake clock and
	// short intervals; defaults mirror the Java client.
	now               func() time.Time
	pollInterval      time.Duration // 5s
	allSyncInterval   time.Duration // 3min
	overLimitSuppress time.Duration // 60s
	failureBackoff    time.Duration // 1s
	lastAllSync       time.Time

	// pendingLimit bounds every pattern context's dispatch queue (see
	// fuzzyWatchContext.pending). newFuzzyWatchContext copies it in at
	// context-creation time, so changing it later only affects patterns
	// registered afterward - existing contexts keep the bound they were
	// created with.
	pendingLimit int // 1024
}

// NewFuzzyWatchServiceListHolder creates an empty holder for the given namespace.
func NewFuzzyWatchServiceListHolder(namespace string) *FuzzyWatchServiceListHolder {
	return &FuzzyWatchServiceListHolder{
		namespace:         namespace,
		patterns:          cache.NewConcurrentMap(),
		pendingLimit:      1024,
		bell:              make(chan struct{}, 1),
		stopCh:            make(chan struct{}),
		now:               time.Now,
		pollInterval:      5 * time.Second,
		allSyncInterval:   3 * time.Minute,
		overLimitSuppress: 60 * time.Second,
		failureBackoff:    time.Second,
	}
}

// pendingLimitForTest tightens the per-pattern dispatch queue bound; existing
// contexts are not retrofitted, so call it before RegisterWatcher.
func (h *FuzzyWatchServiceListHolder) pendingLimitForTest(n int) { h.pendingLimit = n }

func (h *FuzzyWatchServiceListHolder) get(pattern string) (*fuzzyWatchContext, bool) {
	v, ok := h.patterns.Get(pattern)
	if !ok {
		return nil, false
	}
	return v.(*fuzzyWatchContext), true
}

// Patterns returns the currently registered groupKeyPatterns.
func (h *FuzzyWatchServiceListHolder) Patterns() []string {
	return h.patterns.Keys()
}

// ReceivedGroupKeys returns the serviceKeys currently known to match pattern.
// It is sent back to the server on redo so the server can compute a diff.
func (h *FuzzyWatchServiceListHolder) ReceivedGroupKeys(pattern string) []string {
	ctx, ok := h.get(pattern)
	if !ok {
		return nil
	}
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	keys := make([]string, 0, len(ctx.receivedGroupKeys))
	for k := range ctx.receivedGroupKeys {
		keys = append(keys, k)
	}
	return keys
}

// SetRequester wires the transport the reconcile worker sends WATCH/CANCEL_WATCH
// RPCs through. Called once after construction (see the requester field
// comment), so a concurrent RegisterWatcher racing this call either observes
// nil (fails fast with ErrFuzzyWatchNotSupported) or the fully-set requester -
// never a half-initialized one, since requesterMu serializes both.
func (h *FuzzyWatchServiceListHolder) SetRequester(r FuzzyWatchRequester) {
	h.requesterMu.Lock()
	defer h.requesterMu.Unlock()
	h.requester = r
}

func (h *FuzzyWatchServiceListHolder) getRequester() FuzzyWatchRequester {
	h.requesterMu.RLock()
	defer h.requesterMu.RUnlock()
	return h.requester
}

// Bell rings the reconcile worker without blocking; a ring already pending
// coalesces with this one, so callers never need to worry about flooding the
// worker with redundant wakeups.
func (h *FuzzyWatchServiceListHolder) Bell() {
	select {
	case h.bell <- struct{}{}:
	default: // a ring is already pending; coalesce
	}
}

// RegisterWatcher adds one watcher to pattern purely locally (reviving a
// pending-cancel pattern if needed), replays already-known matches to the new
// watcher only, and rings the reconcile worker. The server RPC outcome never
// affects the registration: capacity problems surface through onLoad, and
// transient failures are retried in the background until consistent.
func (h *FuzzyWatchServiceListHolder) RegisterWatcher(pattern string, cb func(model.FuzzyWatchChangeEvent), onLoad func(model.FuzzyWatchLoadEvent)) (uint64, error) {
	if cb == nil {
		return 0, errors.New("watchCallback cannot be nil!")
	}
	// Fast-fail before the ability check, which may block for its network
	// timeout; the authoritative closed-state check is the one below, made
	// under h.mu after the ability check returns.
	select {
	case <-h.stopCh:
		return 0, ErrFuzzyWatchClientClosed
	default:
	}
	r := h.getRequester()
	if r == nil || !r.ServerSupportsFuzzyWatch() {
		return 0, ErrFuzzyWatchNotSupported
	}
	h.mu.Lock()
	// Shutdown closes stopCh while holding h.mu, so this re-check linearizes
	// the registration commit with shutdown: without it, a Shutdown landing
	// during the ability check above would let a watcher commit after the
	// reconcile worker is already gone - a ghost handle that can never
	// establish a server-side WATCH.
	select {
	case <-h.stopCh:
		h.mu.Unlock()
		return 0, ErrFuzzyWatchClientClosed
	default:
	}
	var ctx *fuzzyWatchContext
	if v, ok := h.patterns.Get(pattern); ok {
		ctx = v.(*fuzzyWatchContext)
	} else {
		ctx = newFuzzyWatchContext(pattern, h)
		h.patterns.Set(pattern, ctx)
	}
	id := h.nextCallbackID.Add(1)
	ctx.mu.Lock()
	if ctx.discard {
		ctx.discard = false // revive a pattern whose CANCEL may be in flight
		ctx.epoch++         // invalidate any in-flight send's stale desired-state snapshot
	}
	entry := &watcherEntry{id: id, fn: cb, onLoadEvent: onLoad, syncedKeys: make(map[string]struct{})}
	ctx.entries = append(ctx.entries, entry)
	replay := make([]notifyTask, 0, len(ctx.receivedGroupKeys))
	for serviceKey := range ctx.receivedGroupKeys {
		namespace, group, service, err := parseServiceKey(serviceKey)
		if err != nil {
			continue
		}
		ev := model.FuzzyWatchChangeEvent{ServiceName: service, GroupName: group, NamespaceId: namespace,
			ChangedType: constant.FUZZY_WATCH_CHANGED_TYPE_ADD_SERVICE, SyncType: constant.FUZZY_WATCH_INIT_NOTIFY}
		replay = append(replay, notifyTask{event: &ev, targets: []*watcherEntry{entry}})
	}
	ctx.enqueueLocked(replay...)
	ctx.mu.Unlock()
	h.mu.Unlock()
	h.Bell()
	return id, nil
}

// RemoveWatcherByID removes exactly one registration. When the last watcher
// goes, the pattern flips to discard (desired state: not watched) and the
// worker sends the server-side CANCEL; local state stays until the server
// confirms, so pushes keep being handled meanwhile.
func (h *FuzzyWatchServiceListHolder) RemoveWatcherByID(pattern string, id uint64) {
	h.mu.Lock()
	ctx, ok := h.get(pattern)
	if !ok {
		h.mu.Unlock()
		return
	}
	ctx.mu.Lock()
	kept := ctx.entries[:0:0]
	for _, e := range ctx.entries {
		if e.id != id {
			kept = append(kept, e)
		} else {
			// Tasks already queued hold a pointer to this entry; marking it
			// removed is what stops deliver from invoking it later.
			e.removed = true
		}
	}
	ctx.entries = kept
	if len(ctx.entries) == 0 {
		ctx.discard = true
		ctx.consistentWithServer = false
		ctx.epoch++ // invalidate any in-flight WATCH send's stale desired-state snapshot
	}
	ctx.mu.Unlock()
	h.mu.Unlock()
	h.Bell()
}

// ResetConsistenceStatus marks every pattern as out of sync with the server -
// called on reconnect, since a fresh gRPC connection has no server-side WATCH
// state left to be consistent with - and rings the worker to re-send them
// all. Bumping epoch invalidates any WATCH send already in flight on the old
// connection: its eventual response must not mark the pattern consistent
// under the new one.
func (h *FuzzyWatchServiceListHolder) ResetConsistenceStatus() {
	for _, pattern := range h.Patterns() {
		if ctx, ok := h.get(pattern); ok {
			ctx.mu.Lock()
			ctx.consistentWithServer = false
			ctx.epoch++
			ctx.mu.Unlock()
		}
	}
	h.Bell()
}

// MatchedServiceKeys blocks until pattern's initial sync finishes (or waitCtx
// is done, whichever comes first) and then returns the current matched-key
// snapshot. It is the synchronous counterpart to the callback-based watcher
// API, for callers that need an immediate, complete match list.
func (h *FuzzyWatchServiceListHolder) MatchedServiceKeys(waitCtx context.Context, pattern string) ([]string, error) {
	pctx, ok := h.get(pattern)
	if !ok {
		return nil, errors.Errorf("pattern %s is not being fuzzy watched", pattern)
	}
	select {
	case <-pctx.initDone:
		return h.ReceivedGroupKeys(pattern), nil
	case <-waitCtx.Done():
		return nil, waitCtx.Err()
	}
}

// HandleSync processes a server-pushed NamingFuzzyWatchSyncRequest: a batch of
// matched services for pattern. INIT / DIFF batches add or remove keys by
// changedType and enqueue callbacks for async dispatch; a FINISH batch just
// marks the initial sync complete.
//
// A sync for a pattern the holder doesn't currently know about - the user
// already canceled it, or the server is replaying a stale/late batch after
// cancellation - is dropped via get() rather than create-on-demand:
// HandleSync must never resurrect a canceled pattern. The push handler
// still ACKs the server request either way; that framing lives in
// fuzzy_watch_handler.go and is unaffected by this.
func (h *FuzzyWatchServiceListHolder) HandleSync(pattern, syncType string, contexts []rpc_request.NamingFuzzyWatchSyncContext, totalBatch, currentBatch int) {
	ctx, ok := h.get(pattern)
	if !ok {
		logger.Warnf("fuzzy watch received sync for unknown pattern:%s (already canceled or never registered), ignoring", pattern)
		return
	}

	if syncType == constant.FINISH_FUZZY_WATCH_INIT_NOTIFY {
		ctx.mu.Lock()
		// initialized guards the close: FINISH can arrive more than once
		// across reconnects (each reconnect re-sends the initial batch), and
		// closing an already-closed channel panics.
		if !ctx.initialized {
			ctx.initialized = true
			close(ctx.initDone)
		}
		ctx.mu.Unlock()
		logger.Infof("fuzzy watch pattern:%s initial sync finished, batch %d/%d", pattern, currentBatch, totalBatch)
		return
	}

	ctx.mu.Lock()
	var events []model.FuzzyWatchChangeEvent
	for _, item := range contexts {
		namespace, group, service, err := parseServiceKey(item.ServiceKey)
		if err != nil {
			logger.Warnf("fuzzy watch skips malformed serviceKey:%s changedType:%s err:%v", item.ServiceKey, item.ChangedType, err)
			continue
		}
		if ev, ok := ctx.applyChange(item.ServiceKey, namespace, group, service, item.ChangedType, syncType); ok {
			events = append(events, ev)
		}
	}
	if len(events) > 0 {
		targets := ctx.snapshotEntriesLocked()
		tasks := make([]notifyTask, len(events))
		for i := range events {
			ev := events[i]
			tasks[i] = notifyTask{event: &ev, targets: targets}
		}
		ctx.enqueueLocked(tasks...)
	}
	ctx.mu.Unlock()
}

// HandleChangeNotify processes a server-pushed NamingFuzzyWatchChangeNotifyRequest:
// a single service added or deleted after the initial sync. The request carries
// no pattern, so the holder matches serviceKey against every registered pattern
// and applies the change to each one that matches.
func (h *FuzzyWatchServiceListHolder) HandleChangeNotify(serviceKey, changedType string) {
	namespace, group, service, err := parseServiceKey(serviceKey)
	if err != nil {
		logger.Warnf("fuzzy watch change-notify skips malformed serviceKey:%s changedType:%s err:%v", serviceKey, changedType, err)
		return
	}
	for _, pattern := range h.Patterns() {
		if !matchPattern(pattern, namespace, group, service) {
			continue
		}
		ctx, ok := h.get(pattern)
		if !ok {
			continue
		}
		ctx.mu.Lock()
		if ev, fired := ctx.applyChange(serviceKey, namespace, group, service, changedType, constant.FUZZY_WATCH_RESOURCE_CHANGED); fired {
			ctx.enqueueLocked(notifyTask{event: &ev, targets: ctx.snapshotEntriesLocked()})
		}
		ctx.mu.Unlock()
	}
}

// isInitialized reports whether pattern's initial sync has finished. Test helper.
func (h *FuzzyWatchServiceListHolder) isInitialized(pattern string) bool {
	ctx, ok := h.get(pattern)
	if !ok {
		return false
	}
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	return ctx.initialized
}

// parseServiceKey splits a serviceKey (namespace@@group@@service) into parts.
func parseServiceKey(serviceKey string) (namespace, group, service string, err error) {
	parts := strings.Split(serviceKey, constant.SERVICE_INFO_SPLITER)
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return "", "", "", errors.Errorf("serviceKey %q is not namespace%sgroup%sservice", serviceKey,
			constant.SERVICE_INFO_SPLITER, constant.SERVICE_INFO_SPLITER)
	}
	return parts[0], parts[1], parts[2], nil
}

// matchPattern reports whether a service (namespace/group/service) is covered
// by a groupKeyPattern (namespace>>groupPattern>>servicePattern). Namespace
// must be exact; group and service match by itemMatch (exact, "*", prefix "x*",
// suffix "*x" or contains "*x*").
func matchPattern(pattern, namespace, group, service string) bool {
	parts := strings.Split(pattern, constant.FUZZY_WATCH_PATTERN_SPLITTER)
	if len(parts) != 3 {
		return false
	}
	return parts[0] == namespace && itemMatch(parts[1], group) && itemMatch(parts[2], service)
}

// itemMatch mirrors com.alibaba.nacos.common.utils.FuzzyGroupKeyPattern.itemMatched
// (alibaba/nacos, develop branch), which supports five modes. Order matters:
// "*x*" (contains) must be tested before "*x" (suffix) and "x*" (prefix), since
// a contains pattern also satisfies HasPrefix("*")/HasSuffix("*").
// https://github.com/alibaba/nacos/blob/develop/common/src/main/java/com/alibaba/nacos/common/utils/FuzzyGroupKeyPattern.java
func itemMatch(pattern, value string) bool {
	switch {
	case pattern == "*": // match all
		return true
	case strings.HasPrefix(pattern, "*") && strings.HasSuffix(pattern, "*"): // contains "*x*"
		return strings.Contains(value, pattern[1:len(pattern)-1])
	case strings.HasPrefix(pattern, "*"): // suffix "*x"
		return strings.HasSuffix(value, pattern[1:])
	case strings.HasSuffix(pattern, "*"): // prefix "x*"
		return strings.HasPrefix(value, pattern[:len(pattern)-1])
	default: // exact
		return pattern == value
	}
}
