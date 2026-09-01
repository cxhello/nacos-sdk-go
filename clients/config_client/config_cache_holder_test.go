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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
