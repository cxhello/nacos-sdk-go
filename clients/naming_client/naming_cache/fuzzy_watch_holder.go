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
	"reflect"
	"strings"
	"sync"

	"github.com/pkg/errors"

	"github.com/nacos-group/nacos-sdk-go/v3/clients/cache"
	"github.com/nacos-group/nacos-sdk-go/v3/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v3/common/logger"
	"github.com/nacos-group/nacos-sdk-go/v3/common/remote/rpc/rpc_request"
	"github.com/nacos-group/nacos-sdk-go/v3/model"
)

// fuzzyWatchContext is the per-pattern state: the set of serviceKeys the
// client has been told match the pattern (receivedGroupKeys), the user
// callbacks to notify on change, and whether the initial batch sync has
// finished. Its own mutex guards all three.
type fuzzyWatchContext struct {
	receivedGroupKeys map[string]struct{}
	callbacks         []func(model.FuzzyWatchChangeEvent)
	initialized       bool
	mu                sync.Mutex
}

func (c *fuzzyWatchContext) snapshotCallbacks() []func(model.FuzzyWatchChangeEvent) {
	out := make([]func(model.FuzzyWatchChangeEvent), len(c.callbacks))
	copy(out, c.callbacks)
	return out
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
}

// NewFuzzyWatchServiceListHolder creates an empty holder for the given namespace.
func NewFuzzyWatchServiceListHolder(namespace string) *FuzzyWatchServiceListHolder {
	return &FuzzyWatchServiceListHolder{
		namespace: namespace,
		patterns:  cache.NewConcurrentMap(),
	}
}

func (h *FuzzyWatchServiceListHolder) getOrCreate(pattern string) *fuzzyWatchContext {
	h.mu.Lock()
	defer h.mu.Unlock()
	if v, ok := h.patterns.Get(pattern); ok {
		return v.(*fuzzyWatchContext)
	}
	ctx := &fuzzyWatchContext{receivedGroupKeys: make(map[string]struct{})}
	h.patterns.Set(pattern, ctx)
	return ctx
}

func (h *FuzzyWatchServiceListHolder) get(pattern string) (*fuzzyWatchContext, bool) {
	v, ok := h.patterns.Get(pattern)
	if !ok {
		return nil, false
	}
	return v.(*fuzzyWatchContext), true
}

// RegisterPattern records a callback for pattern, creating the pattern context
// if needed. Registering the same callback code pointer twice for a pattern is
// deduped, so a repeated FuzzyWatch does not multiply notifications.
func (h *FuzzyWatchServiceListHolder) RegisterPattern(pattern string, cb func(model.FuzzyWatchChangeEvent)) {
	ctx := h.getOrCreate(pattern)
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	for _, existing := range ctx.callbacks {
		if sameCallback(existing, cb) {
			return
		}
	}
	ctx.callbacks = append(ctx.callbacks, cb)
}

// RemoveCallback drops one callback from a pattern and returns how many remain.
// The caller cancels the server-side watch once this reaches zero.
func (h *FuzzyWatchServiceListHolder) RemoveCallback(pattern string, cb func(model.FuzzyWatchChangeEvent)) int {
	ctx, ok := h.get(pattern)
	if !ok {
		return 0
	}
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	kept := ctx.callbacks[:0:0]
	for _, existing := range ctx.callbacks {
		if !sameCallback(existing, cb) {
			kept = append(kept, existing)
		}
	}
	ctx.callbacks = kept
	return len(kept)
}

// RemovePattern forgets a pattern entirely (its keys and callbacks).
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
// changedType and fire callbacks; a FINISH batch just marks the initial sync
// complete.
func (h *FuzzyWatchServiceListHolder) HandleSync(pattern, syncType string, contexts []rpc_request.NamingFuzzyWatchSyncContext, totalBatch, currentBatch int) {
	ctx := h.getOrCreate(pattern)
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
	callbacks := ctx.snapshotCallbacks()
	ctx.mu.Unlock()

	notify(callbacks, events)
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
		ev, fired := ctx.applyChange(serviceKey, namespace, group, service, changedType)
		callbacks := ctx.snapshotCallbacks()
		ctx.mu.Unlock()
		if fired {
			notify(callbacks, []model.FuzzyWatchChangeEvent{ev})
		}
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

// notify fans each event out to every callback. Callbacks are invoked outside
// any holder lock so a user callback may safely call back into the holder.
func notify(callbacks []func(model.FuzzyWatchChangeEvent), events []model.FuzzyWatchChangeEvent) {
	for _, ev := range events {
		for _, cb := range callbacks {
			cb(ev)
		}
	}
}

func sameCallback(a, b func(model.FuzzyWatchChangeEvent)) bool {
	return reflect.ValueOf(a).Pointer() == reflect.ValueOf(b).Pointer()
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
