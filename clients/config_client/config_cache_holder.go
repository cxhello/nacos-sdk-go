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
	"sync"

	"github.com/nacos-group/nacos-sdk-go/v3/vo"
)

// cacheData holds the listen state for a single dataId/group/tenant tuple.
// It is always referenced by pointer so that listeners registered against
// the same key observe/update a single shared instance.
type cacheData struct {
	mu sync.Mutex

	dataId, group, tenant                       string
	content, contentType, md5, encryptedDataKey string
	taskId                                      int
	isSyncWithServer                            bool
	isInitializing                              bool
	discard                                     bool
	listeners                                   []*listenerWrap
}

// listenerWrap pairs a user listener callback with the md5 watermark it has
// last been notified with, so distinct listeners on the same key can be
// notified independently.
type listenerWrap struct {
	listener    vo.Listener
	lastCallMd5 string
}

// configCacheHolder is the pointer-based replacement for the previous
// cache.ConcurrentMap-backed by-value storage. Entries are never replaced in
// place; callers mutate the returned *cacheData under its own mu.
type configCacheHolder struct {
	mu      sync.RWMutex
	entries map[string]*cacheData
}

func newConfigCacheHolder() *configCacheHolder {
	return &configCacheHolder{
		entries: make(map[string]*cacheData),
	}
}

// get returns the entry for key, if any.
func (h *configCacheHolder) get(key string) (*cacheData, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	cData, ok := h.entries[key]
	return cData, ok
}

// getOrCreate returns the existing entry for key, reviving it (discard=false)
// if it was previously cancelled. If no entry exists yet, seed() is invoked
// to build one, which is then stored and returned. seed is only invoked while
// creating a brand-new entry.
func (h *configCacheHolder) getOrCreate(key string, seed func() *cacheData) *cacheData {
	h.mu.Lock()
	defer h.mu.Unlock()
	if cData, ok := h.entries[key]; ok {
		cData.mu.Lock()
		cData.discard = false
		cData.mu.Unlock()
		return cData
	}
	cData := seed()
	h.entries[key] = cData
	return cData
}

// snapshot returns all entries currently stored. Order is unspecified.
func (h *configCacheHolder) snapshot() []*cacheData {
	h.mu.RLock()
	defer h.mu.RUnlock()
	result := make([]*cacheData, 0, len(h.entries))
	for _, cData := range h.entries {
		result = append(result, cData)
	}
	return result
}

// removeIfDiscarded removes the entry for key only if it is still marked as
// discarded and has no listeners attached, re-checking both conditions while
// holding the lock to avoid racing with a concurrent revive/addListener.
func (h *configCacheHolder) removeIfDiscarded(key string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	cData, ok := h.entries[key]
	if !ok {
		return
	}
	cData.mu.Lock()
	shouldRemove := cData.discard && len(cData.listeners) == 0
	cData.mu.Unlock()
	if shouldRemove {
		delete(h.entries, key)
	}
}

// count returns the number of entries currently stored.
func (h *configCacheHolder) count() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.entries)
}

// addListener appends a listener to the entry, recording initialMd5 as its
// starting watermark so it is only notified once content actually changes
// relative to that baseline.
func (c *cacheData) addListener(l vo.Listener, initialMd5 string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.listeners = append(c.listeners, &listenerWrap{listener: l, lastCallMd5: initialMd5})
}

// reviveAndAddListener atomically (re)activates the entry and appends l as a
// new listener, in a single critical section: discard is reasserted to
// false, isInitializing is set to true, and the listener is appended using
// the entry's md5 (read under the same lock) as its initial watermark.
//
// This exists because getOrCreate's own revive step (discard=false) and a
// subsequent, separately-locked addListener call are two distinct critical
// sections; a concurrent CancelListenConfig/markDiscard could interleave
// between them and leave the entry with discard==true and a non-empty
// listeners slice -- an inconsistent state that downstream reap logic must
// never observe. Folding revive + append into one lock closes that window:
// whichever of markDiscard/reviveAndAddListener runs last atomically
// determines the resulting (discard, listeners) pair.
func (c *cacheData) reviveAndAddListener(l vo.Listener) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.discard = false
	c.isInitializing = true
	c.listeners = append(c.listeners, &listenerWrap{listener: l, lastCallMd5: c.md5})
}

// markDiscard flags the entry as cancelled: it stops being treated as synced
// and its listeners are dropped. The entry itself is left in the holder for
// removeIfDiscarded (or a future revive via getOrCreate) to reconcile.
func (c *cacheData) markDiscard() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.discard = true
	c.listeners = nil
	c.isSyncWithServer = false
}
