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
	"strings"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"

	"github.com/nacos-group/nacos-sdk-go/v3/clients/cache"
	"github.com/nacos-group/nacos-sdk-go/v3/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v3/common/logger"
	"github.com/nacos-group/nacos-sdk-go/v3/common/remote/rpc/rpc_request"
	"github.com/nacos-group/nacos-sdk-go/v3/model"
)

// registeredCallback pairs a user callback with the id RegisterPattern handed
// back to its caller. The id, not the callback's code pointer, is what
// identifies a registration: two distinct method values sharing a receiver
// type can share a code pointer in Go, so reflect.ValueOf(fn).Pointer() is
// not a valid way to tell two registrations apart.
type registeredCallback struct {
	id uint64
	fn func(model.FuzzyWatchChangeEvent)
}

// notifyTask bundles one fired event together with the callback snapshot
// that must observe it. Queuing whole tasks (rather than events and
// callbacks separately) is what lets enqueueLocked/drain preserve per-pattern
// delivery order across multiple HandleSync/HandleChangeNotify calls.
type notifyTask struct {
	event     model.FuzzyWatchChangeEvent
	callbacks []func(model.FuzzyWatchChangeEvent)
}

// fuzzyWatchContext is the per-pattern state: the set of serviceKeys the
// client has been told match the pattern (receivedGroupKeys), the registered
// callbacks, whether the initial batch sync has finished, and the async
// dispatch queue (pending/draining). Its own mutex guards all of it.
//
// Caveat: the pending queue has no backpressure. A callback that runs slower
// than events arrive lets pending grow without bound for that pattern. The
// older, now-replaced design called callbacks synchronously from the
// gRPC push-handler goroutine, which bounded memory but blocked that
// goroutine - and the server-required ACK - on user code; this design
// trades that hazard for an unbounded-queue one instead.
type fuzzyWatchContext struct {
	receivedGroupKeys map[string]struct{}
	callbacks         []registeredCallback
	initialized       bool

	// pending/draining implement the async dispatcher: HandleSync and
	// HandleChangeNotify enqueue tasks while holding mu, then release it.
	// Callbacks run later, on the single drain goroutine, never on the
	// caller's goroutine and never while mu is held - so a user callback
	// that blocks or re-enters the holder cannot deadlock or stall the
	// gRPC push-handler goroutine that must ACK the server request.
	pending  []notifyTask
	draining bool

	mu sync.Mutex
}

func (c *fuzzyWatchContext) snapshotCallbacks() []func(model.FuzzyWatchChangeEvent) {
	out := make([]func(model.FuzzyWatchChangeEvent), len(c.callbacks))
	for i, rc := range c.callbacks {
		out[i] = rc.fn
	}
	return out
}

// enqueueLocked appends tasks and starts a drain goroutine when none is
// running. Caller must hold c.mu. Per-pattern ordering is preserved because
// at most one drain goroutine exists per context; callbacks therefore never
// run on (and never block) the RPC request-handler goroutine.
func (c *fuzzyWatchContext) enqueueLocked(tasks ...notifyTask) {
	if len(tasks) == 0 {
		return
	}
	c.pending = append(c.pending, tasks...)
	if !c.draining {
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
			for _, cb := range task.callbacks {
				cb(task.event)
			}
		}
	}
}

// applyChange mutates receivedGroupKeys for a single serviceKey and reports
// the event to fire, if any. Called with c.mu held. An ADD for a key already
// known, or a DELETE for a key not known, is a no-op that fires nothing -
// this is what makes redo/duplicate syncs idempotent.
func (c *fuzzyWatchContext) applyChange(serviceKey, namespace, group, service, changedType string) (model.FuzzyWatchChangeEvent, bool) {
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
	return model.FuzzyWatchChangeEvent{
		ServiceName: service,
		GroupName:   group,
		NamespaceId: namespace,
		ChangedType: changedType,
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

	// nextCallbackID is an atomic counter handed out by RegisterPattern so
	// every registration - even a repeat registration of the same func
	// value - gets its own identity to cancel by. atomic.Uint64 (rather than
	// a plain uint64 used with atomic.AddUint64) keeps its own alignment
	// guarantee, avoiding the 32-bit "unaligned 64-bit atomic operation"
	// panic that a plain uint64 field risks on GOARCH=386/arm.
	nextCallbackID atomic.Uint64
}

// NewFuzzyWatchServiceListHolder creates an empty holder for the given namespace.
func NewFuzzyWatchServiceListHolder(namespace string) *FuzzyWatchServiceListHolder {
	return &FuzzyWatchServiceListHolder{
		namespace: namespace,
		patterns:  cache.NewConcurrentMap(),
	}
}

func (h *FuzzyWatchServiceListHolder) get(pattern string) (*fuzzyWatchContext, bool) {
	v, ok := h.patterns.Get(pattern)
	if !ok {
		return nil, false
	}
	return v.(*fuzzyWatchContext), true
}

// RegisterPattern records a callback for pattern, creating the pattern
// context if needed, and returns the id assigned to this registration plus
// whether the pattern context was newly created. Registration is
// unconditional - no code-pointer dedupe - so registering the same func
// value twice yields two ids and two independent deliveries per event,
// matching the Java client where two distinct watcher objects both fire.
// Callers that must avoid duplicate delivery are responsible for tracking
// the returned id and not registering twice.
//
// h.mu is held across both the pattern lookup/create and the callback
// append below, nesting ctx.mu inside it, so a concurrent RemovePattern can
// never evict the context between "found/created it" and "appended the
// callback to it". Without that, RegisterPattern could append to a context
// it just evicted a moment earlier and report success for a registration
// that will never receive an event. Lock order is always h.mu -> ctx.mu:
// drain() and the Handle* methods take ctx.mu but never acquire h.mu while
// holding it, so this nesting cannot deadlock.
func (h *FuzzyWatchServiceListHolder) RegisterPattern(pattern string, cb func(model.FuzzyWatchChangeEvent)) (id uint64, created bool) {
	h.mu.Lock()
	defer h.mu.Unlock()

	var ctx *fuzzyWatchContext
	if v, ok := h.patterns.Get(pattern); ok {
		ctx = v.(*fuzzyWatchContext)
	} else {
		ctx = &fuzzyWatchContext{receivedGroupKeys: make(map[string]struct{})}
		h.patterns.Set(pattern, ctx)
		created = true
	}

	id = h.nextCallbackID.Add(1)
	ctx.mu.Lock()
	ctx.callbacks = append(ctx.callbacks, registeredCallback{id: id, fn: cb})
	ctx.mu.Unlock()
	return id, created
}

// RemoveCallbackByID drops exactly the registration identified by id from
// pattern, leaving any other registrations on the same pattern untouched.
// It's exported for naming_client's registration-rollback path: if
// RegisterPattern succeeds locally but the follow-up server-side FuzzyWatch
// RPC fails, the caller undoes only the registration it just made instead of
// tearing down the whole pattern (which may have other, unrelated
// registrations). A no-op if pattern or id is unknown.
//
// Removal only stops future dispatch: it does not reach into notifyTasks
// already queued or already snapshotted into a task's callbacks slice
// (snapshotCallbacks is called under ctx.mu before enqueueLocked, so a task
// already built holds its own copy). A registration removed here may
// therefore still receive one or more events that were snapshotted before
// the removal took effect.
func (h *FuzzyWatchServiceListHolder) RemoveCallbackByID(pattern string, id uint64) {
	ctx, ok := h.get(pattern)
	if !ok {
		return
	}
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	kept := ctx.callbacks[:0:0]
	for _, existing := range ctx.callbacks {
		if existing.id != id {
			kept = append(kept, existing)
		}
	}
	ctx.callbacks = kept
}

// RemovePattern forgets a pattern entirely (its keys and callbacks). This
// only stops new work: it drops the context from the map so no future
// HandleSync/HandleChangeNotify can enqueue onto it, but it does not stop a
// drain goroutine already running against that context's own pending queue.
// Any notifyTasks enqueued before the removal keep being delivered to their
// snapshotted callbacks until that queue drains empty - a bounded amount of
// late delivery (only what was already queued), never an unbounded one,
// since removal guarantees nothing more is added after it runs.
func (h *FuzzyWatchServiceListHolder) RemovePattern(pattern string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.patterns.Remove(pattern)
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
		ctx.initialized = true
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
		if ev, ok := ctx.applyChange(item.ServiceKey, namespace, group, service, item.ChangedType); ok {
			events = append(events, ev)
		}
	}
	if len(events) > 0 {
		callbacks := ctx.snapshotCallbacks()
		tasks := make([]notifyTask, len(events))
		for i, ev := range events {
			tasks[i] = notifyTask{event: ev, callbacks: callbacks}
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
		if ev, fired := ctx.applyChange(serviceKey, namespace, group, service, changedType); fired {
			ctx.enqueueLocked(notifyTask{event: ev, callbacks: ctx.snapshotCallbacks()})
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
