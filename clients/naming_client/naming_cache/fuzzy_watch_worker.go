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
	"time"

	"github.com/pkg/errors"

	"github.com/nacos-group/nacos-sdk-go/v3/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v3/common/logger"
	"github.com/nacos-group/nacos-sdk-go/v3/model"
)

// Start launches the reconcile worker goroutine. Idempotent: a second call
// while already running is a no-op.
func (h *FuzzyWatchServiceListHolder) Start() {
	if !h.started.CompareAndSwap(false, true) {
		return
	}
	go h.loop()
}

// Shutdown stops the reconcile worker goroutine. Idempotent.
func (h *FuzzyWatchServiceListHolder) Shutdown() {
	h.stopOnce.Do(func() { close(h.stopCh) })
}

func (h *FuzzyWatchServiceListHolder) loop() {
	for {
		select {
		case <-h.stopCh:
			return
		case <-h.bell:
		case <-time.After(h.pollInterval):
		}
		h.executeSync()
	}
}

// executeSync is the single sender of WATCH/CANCEL RPCs. Serializing every
// lifecycle transition through this one goroutine is what removes the
// register/cancel/reconnect races by construction (Java parity:
// NamingFuzzyWatchServiceListHolder.executeNamingFuzzyWatch).
func (h *FuzzyWatchServiceListHolder) executeSync() {
	r := h.getRequester()
	if r == nil {
		return
	}
	now := h.now()
	needAllSync := now.Sub(h.lastAllSync) >= h.allSyncInterval
	for _, pattern := range h.Patterns() {
		ctx, ok := h.get(pattern)
		if !ok {
			continue
		}
		ctx.mu.Lock()
		// A discarded, now-empty context always needs its CANCEL sent: the
		// consistentWithServer fast path below is for an established WATCH,
		// and must never short-circuit a pending CANCEL, or the CANCEL would
		// wait up to allSyncInterval behind a stale "consistent" flag left
		// over from the WATCH that preceded the last watcher's removal.
		discardedEmpty := ctx.discard && len(ctx.entries) == 0
		if ctx.consistentWithServer && !discardedEmpty {
			ctx.enqueueLocked(ctx.syncWatchersLocked()...)
			if !needAllSync {
				ctx.mu.Unlock()
				continue
			}
		}
		watchType := constant.FUZZY_WATCH_TYPE_WATCH
		if discardedEmpty {
			watchType = constant.FUZZY_WATCH_TYPE_CANCEL_WATCH
		}
		keys := make([]string, 0, len(ctx.receivedGroupKeys))
		for k := range ctx.receivedGroupKeys {
			keys = append(keys, k)
		}
		isInitializing := !ctx.initialized
		// epoch is snapshotted together with the rest of the desired state
		// that this RPC represents, so the response handler below can tell
		// whether desired state moved on while the RPC was in flight.
		epoch := ctx.epoch
		ctx.mu.Unlock()

		err := r.SendFuzzyWatchRequest(pattern, watchType, keys, isInitializing)
		switch {
		case err == nil:
			if watchType == constant.FUZZY_WATCH_TYPE_CANCEL_WATCH {
				h.removePatternIfDiscarded(pattern)
			} else {
				ctx.mu.Lock()
				// Only apply the success if desired state has not moved on
				// since the snapshot this WATCH was sent from (e.g. a
				// concurrent RemoveWatcherByID or ResetConsistenceStatus) -
				// otherwise this stale response must not mark the pattern
				// consistent; the newer desired state gets its own send.
				if ctx.epoch == epoch {
					ctx.consistentWithServer = true
					ctx.lastOverLimitNotify = time.Time{}
				}
				ctx.mu.Unlock()
			}
		case isOverLimitError(err):
			h.notifyOverLimit(ctx, pattern, err)
		default:
			logger.Warnf("fuzzy watch pattern:%s %s failed, will retry: %v", pattern, watchType, err)
			// Java parity: brief in-loop backoff, then ring to retry. Blocking
			// the worker briefly is acceptable at this cadence and keeps a
			// single sender.
			select {
			case <-h.stopCh:
				return
			case <-time.After(h.failureBackoff):
			}
			h.Bell()
		}
	}
	if needAllSync {
		h.lastAllSync = now
	}
}

func isOverLimitError(err error) bool {
	var serverErr *FuzzyWatchServerError
	if !errors.As(err, &serverErr) {
		return false
	}
	return serverErr.ErrorCode == constant.ERROR_CODE_FUZZY_WATCH_PATTERN_OVER_LIMIT ||
		serverErr.ErrorCode == constant.ERROR_CODE_FUZZY_WATCH_MATCH_COUNT_OVER_LIMIT
}

// notifyOverLimit fans a capacity rejection out to the pattern's load
// watchers, suppressed to one notification per overLimitSuppress window. The
// pattern is marked inconsistent unconditionally (independent of the
// suppression window) so the regular poll keeps retrying it - without this,
// an over-limit hit against a previously consistent pattern (its match count
// grew past the server limit between polls) would otherwise wait out the
// full allSyncInterval before the next WATCH attempt.
func (h *FuzzyWatchServiceListHolder) notifyOverLimit(ctx *fuzzyWatchContext, pattern string, err error) {
	var serverErr *FuzzyWatchServerError
	errors.As(err, &serverErr)
	logger.Warnf("fuzzy watch pattern:%s suppressed by server capacity limit: %v", pattern, err)
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	ctx.consistentWithServer = false
	if !ctx.lastOverLimitNotify.IsZero() && h.now().Sub(ctx.lastOverLimitNotify) < h.overLimitSuppress {
		return
	}
	ctx.lastOverLimitNotify = h.now()
	load := model.FuzzyWatchLoadEvent{Pattern: pattern, ErrorCode: serverErr.ErrorCode}
	ctx.enqueueLocked(notifyTask{loadEvent: &load, targets: ctx.snapshotEntriesLocked()})
}

// removePatternIfDiscarded finalizes a confirmed CANCEL, but only if the
// pattern is still discarded and empty: a watcher registered while the CANCEL
// was in flight revives the pattern, which then gets re-WATCHed next round
// (Java parity: removePatternMatchCache re-checks under lock).
func (h *FuzzyWatchServiceListHolder) removePatternIfDiscarded(pattern string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	ctx, ok := h.get(pattern)
	if !ok {
		return
	}
	ctx.mu.Lock()
	remove := ctx.discard && len(ctx.entries) == 0
	ctx.mu.Unlock()
	if remove {
		h.patterns.Remove(pattern)
	}
}

// syncWatchersLocked heals per-watcher drift (dropped notifications, watchers
// added between pushes): any watcher whose syncVersion lags gets targeted
// ADD/DELETE diff events; watchers already in sync just have their version
// stamped. Caller holds c.mu.
func (c *fuzzyWatchContext) syncWatchersLocked() []notifyTask {
	var tasks []notifyTask
	for _, entry := range c.entries {
		if entry.syncVersion == c.syncVersion {
			continue
		}
		var diff []notifyTask
		for key := range c.receivedGroupKeys {
			if _, seen := entry.syncedKeys[key]; !seen {
				if task, ok := diffTask(key, constant.FUZZY_WATCH_CHANGED_TYPE_ADD_SERVICE, entry); ok {
					diff = append(diff, task)
				}
			}
		}
		for key := range entry.syncedKeys {
			if _, exists := c.receivedGroupKeys[key]; !exists {
				if task, ok := diffTask(key, constant.FUZZY_WATCH_CHANGED_TYPE_DELETE_SERVICE, entry); ok {
					diff = append(diff, task)
				}
			}
		}
		if len(diff) == 0 {
			entry.syncVersion = c.syncVersion
		} else {
			tasks = append(tasks, diff...)
		}
	}
	return tasks
}

func diffTask(serviceKey, changedType string, entry *watcherEntry) (notifyTask, bool) {
	namespace, group, service, err := parseServiceKey(serviceKey)
	if err != nil {
		return notifyTask{}, false
	}
	ev := model.FuzzyWatchChangeEvent{ServiceName: service, GroupName: group, NamespaceId: namespace,
		ChangedType: changedType, SyncType: constant.FUZZY_WATCH_DIFF_SYNC_NOTIFY}
	return notifyTask{event: &ev, targets: []*watcherEntry{entry}}, true
}
