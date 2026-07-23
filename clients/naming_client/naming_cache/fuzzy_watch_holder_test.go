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
	"sort"
	"sync"
	"testing"

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

func syncCtx(serviceKey, changedType string) rpc_request.NamingFuzzyWatchSyncContext {
	return rpc_request.NamingFuzzyWatchSyncContext{ServiceKey: serviceKey, ChangedType: changedType}
}

const testPattern = "public>>DEFAULT_GROUP>>order*"

func TestHolderInitBatchingAndFinish(t *testing.T) {
	holder := NewFuzzyWatchServiceListHolder("public")
	sink := &eventSink{}
	holder.RegisterPattern(testPattern, sink.cb)

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

	events := sink.snapshot()
	require.Len(t, events, 3, "one ADD callback per matched service across both batches")
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
	sink := &eventSink{}
	holder.RegisterPattern(testPattern, sink.cb)

	add := []rpc_request.NamingFuzzyWatchSyncContext{
		syncCtx("public@@DEFAULT_GROUP@@order-a", constant.FUZZY_WATCH_CHANGED_TYPE_ADD_SERVICE),
	}
	holder.HandleSync(testPattern, constant.FUZZY_WATCH_INIT_NOTIFY, add, 1, 1)
	// server re-sends the same key (e.g. redo DIFF): must not re-fire.
	holder.HandleSync(testPattern, constant.FUZZY_WATCH_DIFF_SYNC_NOTIFY, add, 1, 1)

	assert.Len(t, sink.snapshot(), 1, "duplicate ADD for a known key must not re-fire")
	assert.Len(t, holder.ReceivedGroupKeys(testPattern), 1)
}

func TestHolderDiffAddAndDelete(t *testing.T) {
	holder := NewFuzzyWatchServiceListHolder("public")
	sink := &eventSink{}
	holder.RegisterPattern(testPattern, sink.cb)

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

	events := sink.snapshot()
	require.Len(t, events, 4, "2 initial ADD + 1 DIFF ADD + 1 DIFF DELETE")
	last := events[3]
	assert.Equal(t, constant.FUZZY_WATCH_CHANGED_TYPE_DELETE_SERVICE, last.ChangedType)
	assert.Equal(t, "order-a", last.ServiceName)
}

func TestHolderUnknownDeleteDoesNotFire(t *testing.T) {
	holder := NewFuzzyWatchServiceListHolder("public")
	sink := &eventSink{}
	holder.RegisterPattern(testPattern, sink.cb)

	holder.HandleSync(testPattern, constant.FUZZY_WATCH_DIFF_SYNC_NOTIFY, []rpc_request.NamingFuzzyWatchSyncContext{
		syncCtx("public@@DEFAULT_GROUP@@order-unknown", constant.FUZZY_WATCH_CHANGED_TYPE_DELETE_SERVICE),
	}, 1, 1)

	assert.Empty(t, sink.snapshot(), "DELETE for an unknown key must not fire")
	assert.Empty(t, holder.ReceivedGroupKeys(testPattern))
}

func TestHolderMalformedServiceKeySkipped(t *testing.T) {
	holder := NewFuzzyWatchServiceListHolder("public")
	sink := &eventSink{}
	holder.RegisterPattern(testPattern, sink.cb)

	holder.HandleSync(testPattern, constant.FUZZY_WATCH_INIT_NOTIFY, []rpc_request.NamingFuzzyWatchSyncContext{
		syncCtx("this-is-not-a-valid-service-key", constant.FUZZY_WATCH_CHANGED_TYPE_ADD_SERVICE),
		syncCtx("public@@DEFAULT_GROUP@@order-a", constant.FUZZY_WATCH_CHANGED_TYPE_ADD_SERVICE),
	}, 1, 1)

	events := sink.snapshot()
	require.Len(t, events, 1, "malformed serviceKey is skipped, the valid one still fires")
	assert.Equal(t, "order-a", events[0].ServiceName)
	assert.Equal(t, []string{"public@@DEFAULT_GROUP@@order-a"}, holder.ReceivedGroupKeys(testPattern))
}

func TestHolderChangeNotifyMatchingPatternOnly(t *testing.T) {
	holder := NewFuzzyWatchServiceListHolder("public")
	orderSink := &eventSink{}
	userSink := &eventSink{}
	holder.RegisterPattern("public>>DEFAULT_GROUP>>order*", orderSink.cb)
	holder.RegisterPattern("public>>DEFAULT_GROUP>>user*", userSink.cb)

	// A change notify carries only a serviceKey; the holder matches it
	// against every registered pattern.
	holder.HandleChangeNotify("public@@DEFAULT_GROUP@@order-x", constant.FUZZY_WATCH_CHANGED_TYPE_ADD_SERVICE)

	assert.Len(t, orderSink.snapshot(), 1, "matching pattern receives the change")
	assert.Empty(t, userSink.snapshot(), "non-matching pattern is untouched")
	assert.Equal(t, []string{"public@@DEFAULT_GROUP@@order-x"}, holder.ReceivedGroupKeys("public>>DEFAULT_GROUP>>order*"))

	// duplicate ADD via change notify does not re-fire
	holder.HandleChangeNotify("public@@DEFAULT_GROUP@@order-x", constant.FUZZY_WATCH_CHANGED_TYPE_ADD_SERVICE)
	assert.Len(t, orderSink.snapshot(), 1)

	// delete removes and fires
	holder.HandleChangeNotify("public@@DEFAULT_GROUP@@order-x", constant.FUZZY_WATCH_CHANGED_TYPE_DELETE_SERVICE)
	assert.Len(t, orderSink.snapshot(), 2)
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
	sink := &eventSink{}
	// a contains-mode pattern: previously its post-init change-notify was dropped.
	holder.RegisterPattern("public>>DEFAULT_GROUP>>*order*", sink.cb)

	holder.HandleChangeNotify("public@@DEFAULT_GROUP@@my-order-service", constant.FUZZY_WATCH_CHANGED_TYPE_ADD_SERVICE)

	events := sink.snapshot()
	require.Len(t, events, 1, "contains-mode pattern must receive its change-notify")
	assert.Equal(t, "my-order-service", events[0].ServiceName)
	assert.Equal(t, constant.FUZZY_WATCH_CHANGED_TYPE_ADD_SERVICE, events[0].ChangedType)
}

func TestHandleChangeNotifySuffixPatternReceivesEvent(t *testing.T) {
	holder := NewFuzzyWatchServiceListHolder("public")
	sink := &eventSink{}
	holder.RegisterPattern("public>>DEFAULT_GROUP>>*service", sink.cb)

	holder.HandleChangeNotify("public@@DEFAULT_GROUP@@order-service", constant.FUZZY_WATCH_CHANGED_TYPE_ADD_SERVICE)

	require.Len(t, sink.snapshot(), 1, "suffix-mode pattern must receive its change-notify")
}

func TestHolderDuplicateRegisterDedupesCallback(t *testing.T) {
	holder := NewFuzzyWatchServiceListHolder("public")
	sink := &eventSink{}
	holder.RegisterPattern(testPattern, sink.cb)
	holder.RegisterPattern(testPattern, sink.cb) // same code pointer -> deduped

	holder.HandleChangeNotify("public@@DEFAULT_GROUP@@order-a", constant.FUZZY_WATCH_CHANGED_TYPE_ADD_SERVICE)
	assert.Len(t, sink.snapshot(), 1, "same callback registered twice fires once")
}

func TestHolderRemoveCallbackAndPattern(t *testing.T) {
	holder := NewFuzzyWatchServiceListHolder("public")
	// Two distinct func literals: distinct code pointers, so they are not
	// deduped against each other (unlike method values, which share a code
	// pointer regardless of receiver).
	var aCount, bCount int
	cbA := func(model.FuzzyWatchChangeEvent) { aCount++ }
	cbB := func(model.FuzzyWatchChangeEvent) { bCount++ }
	holder.RegisterPattern(testPattern, cbA)
	holder.RegisterPattern(testPattern, cbB)

	remaining := holder.RemoveCallback(testPattern, cbA)
	assert.Equal(t, 1, remaining, "one callback left after removing the other")

	holder.HandleChangeNotify("public@@DEFAULT_GROUP@@order-a", constant.FUZZY_WATCH_CHANGED_TYPE_ADD_SERVICE)
	assert.Equal(t, 0, aCount, "removed callback no longer fires")
	assert.Equal(t, 1, bCount)

	remaining = holder.RemoveCallback(testPattern, cbB)
	assert.Equal(t, 0, remaining)
	holder.RemovePattern(testPattern)
	assert.Empty(t, holder.ReceivedGroupKeys(testPattern))
	assert.Empty(t, holder.Patterns())
}
