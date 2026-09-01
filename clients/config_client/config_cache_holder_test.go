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

package config_client

import (
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/nacos-group/nacos-sdk-go/v3/common/filter"
	"github.com/nacos-group/nacos-sdk-go/v3/vo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestCacheData constructs a *cacheData directly for unit tests that only
// need to exercise cacheData's own methods (notifyListeners, etc.) without
// going through the holder.
func newTestCacheData(dataId, group, tenant string) *cacheData {
	return &cacheData{dataId: dataId, group: group, tenant: tenant}
}

// updateContent is a test-only helper mirroring the content fields that
// refreshContentAndCheck updates in production, so tests can set up a
// cacheData's content/md5 without duplicating cacheData's internal lock
// discipline inline in every test.
func (c *cacheData) updateContent(md5, content, encryptedDataKey string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.md5 = md5
	c.content = content
	c.encryptedDataKey = encryptedDataKey
}

// fakeDecryptFilter is a minimal filter.IConfigFilter used to prove that
// notifyListeners runs the filter chain and delivers its output (not the raw
// cacheData content) to listeners.
type fakeDecryptFilter struct{}

func (f *fakeDecryptFilter) DoFilter(param *vo.ConfigParam) error {
	param.Content = "decrypted:" + param.Content
	return nil
}

func (f *fakeDecryptFilter) GetOrder() int { return 0 }

func (f *fakeDecryptFilter) GetFilterName() string { return "fakeDecryptFilter" }

func TestConfigCacheHolderGetOrCreate_CreatesOnce(t *testing.T) {
	h := newConfigCacheHolder()
	seedCalls := 0
	seed := func() *cacheData {
		seedCalls++
		return &cacheData{dataId: "d", group: "g", tenant: "t"}
	}

	first := h.getOrCreate("k", seed)
	require.NotNil(t, first)
	assert.Equal(t, 1, seedCalls)

	second := h.getOrCreate("k", seed)
	assert.Same(t, first, second, "getOrCreate must return the same pointer for an existing key")
	assert.Equal(t, 1, seedCalls, "seed must not be invoked again for an existing entry")
}

func TestConfigCacheHolderGetOrCreate_RevivesDiscarded(t *testing.T) {
	h := newConfigCacheHolder()
	cd := h.getOrCreate("k", func() *cacheData { return &cacheData{} })
	cd.markDiscard()
	assert.True(t, cd.discard)

	revived := h.getOrCreate("k", func() *cacheData {
		t.Fatal("seed must not run when reviving an existing entry")
		return nil
	})
	assert.Same(t, cd, revived)
	assert.False(t, revived.discard, "getOrCreate must revive (discard=false) an existing entry")
}

func TestConfigCacheHolderRemoveIfDiscarded(t *testing.T) {
	h := newConfigCacheHolder()

	// Active entry (not discarded): must survive.
	active := h.getOrCreate("active", func() *cacheData { return &cacheData{} })
	active.addListener(func(namespace, group, dataId, data string) {}, "")
	h.removeIfDiscarded("active")
	_, ok := h.get("active")
	assert.True(t, ok, "removeIfDiscarded must not remove an active (non-discarded) entry")

	// Discarded but still has listeners attached: must survive.
	discardedWithListener := h.getOrCreate("discarded-with-listener", func() *cacheData { return &cacheData{} })
	discardedWithListener.addListener(func(namespace, group, dataId, data string) {}, "")
	discardedWithListener.mu.Lock()
	discardedWithListener.discard = true
	discardedWithListener.mu.Unlock()
	h.removeIfDiscarded("discarded-with-listener")
	_, ok = h.get("discarded-with-listener")
	assert.True(t, ok, "removeIfDiscarded must not remove a discarded entry that still has listeners")

	// Discarded and empty: must be removed.
	discardedEmpty := h.getOrCreate("discarded-empty", func() *cacheData { return &cacheData{} })
	discardedEmpty.markDiscard()
	h.removeIfDiscarded("discarded-empty")
	_, ok = h.get("discarded-empty")
	assert.False(t, ok, "removeIfDiscarded must remove a discarded entry with no listeners")

	// Unknown key: no-op, must not panic.
	assert.NotPanics(t, func() { h.removeIfDiscarded("does-not-exist") })
}

func TestConfigCacheHolderCountAndSnapshot(t *testing.T) {
	h := newConfigCacheHolder()
	assert.Equal(t, 0, h.count())

	h.getOrCreate("a", func() *cacheData { return &cacheData{dataId: "a"} })
	h.getOrCreate("b", func() *cacheData { return &cacheData{dataId: "b"} })
	assert.Equal(t, 2, h.count())

	snap := h.snapshot()
	assert.Len(t, snap, 2)
}

// TestConcurrentAddListenerAndMarkDiscard exercises addListener and
// markDiscard concurrently on the same *cacheData to ensure the internal
// mutex actually protects the listeners slice and discard flag under
// -race.
func TestConcurrentAddListenerAndMarkDiscard(t *testing.T) {
	h := newConfigCacheHolder()
	cd := h.getOrCreate("k", func() *cacheData { return &cacheData{dataId: "d", group: "g"} })

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		idx := strconv.Itoa(i)
		wg.Add(2)
		go func() {
			defer wg.Done()
			cd.addListener(func(namespace, group, dataId, data string) {}, idx)
		}()
		go func() {
			defer wg.Done()
			cd.markDiscard()
		}()
	}
	wg.Wait()

	// No assertion on final listeners length (racy by nature of the
	// interleaving), just confirm no data race was reported (enforced by
	// `go test -race`) and the struct is left in a consistent state.
	cd.mu.Lock()
	_ = cd.listeners
	_ = cd.discard
	cd.mu.Unlock()
}

// TestPanickingListenerIsReplayedNextRound covers the core watermark
// contract: a listener whose callback panics must not have its watermark
// advanced, so the same content is redelivered to it on the next round, and
// its panic must not prevent the other listener on the same key from being
// notified this round.
func TestPanickingListenerIsReplayedNextRound(t *testing.T) {
	cd := newTestCacheData("d", "g", "ns")
	var calls, panics atomic.Int32
	cd.addListener(func(ns, g, d, data string) { panics.Add(1); panic("boom") }, "")
	cd.addListener(func(ns, g, d, data string) { calls.Add(1) }, "")
	cd.updateContent("v1", "text", "")
	chain := filter.NewConfigFilterChainManager()

	cd.notifyListeners(chain)
	assert.Equal(t, int32(1), panics.Load())
	assert.Equal(t, int32(1), calls.Load())

	cd.notifyListeners(chain) // second round: panicking wrap replayed, normal wrap not re-delivered
	assert.Equal(t, int32(2), panics.Load())
	assert.Equal(t, int32(1), calls.Load())
}

// TestNotifyListenersSkipsWrapAlreadyAtWatermark asserts that a wrap whose
// lastCallMd5 already equals the entry's current md5 is not re-notified.
func TestNotifyListenersSkipsWrapAlreadyAtWatermark(t *testing.T) {
	cd := newTestCacheData("d", "g", "ns")
	var calls atomic.Int32
	cd.updateContent("v1", "text", "")
	cd.addListener(func(ns, g, d, data string) { calls.Add(1) }, "v1") // seeded at current md5

	cd.notifyListeners(filter.NewConfigFilterChainManager())

	assert.Equal(t, int32(0), calls.Load(), "a wrap already at the current watermark must not be notified")
}

// TestNotifyListenersDeliversFilterDecryptedContent asserts that
// notifyListeners runs the filter chain and hands listeners the
// filter-transformed content, not the raw cacheData content -- this is the
// decryption path for cipher- dataIds.
func TestNotifyListenersDeliversFilterDecryptedContent(t *testing.T) {
	cd := newTestCacheData("d", "g", "ns")
	var got string
	cd.addListener(func(ns, g, d, data string) { got = data }, "")
	cd.updateContent("v1", "cipher-text", "key1")

	chain := filter.NewConfigFilterChainManager()
	require.NoError(t, filter.RegisterConfigFilterToChain(chain, &fakeDecryptFilter{}))

	cd.notifyListeners(chain)

	assert.Equal(t, "decrypted:cipher-text", got)
}

// TestNotifyListenersBothListenersOnSameKeyReceiveChange closes the Task 3
// deferred minor: two independent listeners registered on the same key must
// both observe a content change in a single notifyListeners round.
func TestNotifyListenersBothListenersOnSameKeyReceiveChange(t *testing.T) {
	cd := newTestCacheData("d", "g", "ns")
	var got1, got2 string
	cd.addListener(func(ns, g, d, data string) { got1 = data }, "")
	cd.addListener(func(ns, g, d, data string) { got2 = data }, "")
	cd.updateContent("v1", "text", "")

	cd.notifyListeners(filter.NewConfigFilterChainManager())

	assert.Equal(t, "text", got1, "first listener must receive the change")
	assert.Equal(t, "text", got2, "second listener must receive the change")
}

// TestNotifyListenersSkipsDeliveryOnFilterChainError asserts that a filter
// chain error aborts the whole round without invoking any listener or
// advancing any watermark, so the round is retried next time.
func TestNotifyListenersSkipsDeliveryOnFilterChainError(t *testing.T) {
	cd := newTestCacheData("d", "g", "ns")
	var calls atomic.Int32
	cd.addListener(func(ns, g, d, data string) { calls.Add(1) }, "")
	cd.updateContent("v1", "text", "")

	cd.notifyListeners(&erroringFilterChain{})

	assert.Equal(t, int32(0), calls.Load(), "listener must not be invoked when the filter chain errors")
	cd.mu.Lock()
	watermark := cd.listeners[0].lastCallMd5
	cd.mu.Unlock()
	assert.Equal(t, "", watermark, "watermark must not advance when the filter chain errors")
}

// erroringFilterChain is a minimal filter.IConfigFilterChain whose
// DoFilters always fails, used to exercise notifyListeners' error path
// directly without depending on configFilterPriorityQueue internals.
type erroringFilterChain struct{}

func (c *erroringFilterChain) AddFilter(f filter.IConfigFilter) error { return nil }

func (c *erroringFilterChain) GetFilters() []filter.IConfigFilter { return nil }

func (c *erroringFilterChain) DoFilters(param *vo.ConfigParam) error { return assert.AnError }

func (c *erroringFilterChain) DoFilterByName(param *vo.ConfigParam, name string) error {
	return assert.AnError
}
