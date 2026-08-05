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

package naming_client

import (
	"context"
	"sync"

	"github.com/nacos-group/nacos-sdk-go/v3/clients/naming_client/naming_cache"
)

// Sentinel errors returned by INamingClient.FuzzyWatch, re-exported so
// callers can errors.Is them without importing the internal cache package.
var (
	ErrFuzzyWatchNotSupported = naming_cache.ErrFuzzyWatchNotSupported
	ErrFuzzyWatchClientClosed = naming_cache.ErrFuzzyWatchClientClosed
)

// FuzzyWatchHandle identifies one FuzzyWatch registration. Cancellation is
// watcher-scoped: Cancel removes only this registration, and the server-side
// watch is torn down by the reconcile worker after the pattern's last
// registration is gone. Handles are the registration identity because Go
// func values cannot be compared reliably.
type FuzzyWatchHandle struct {
	holder  *naming_cache.FuzzyWatchServiceListHolder
	pattern string
	id      uint64
	once    sync.Once
}

// Pattern returns the resolved groupKeyPattern (namespace>>group>>service).
func (h *FuzzyWatchHandle) Pattern() string { return h.pattern }

// Cancel removes this registration. Idempotent.
func (h *FuzzyWatchHandle) Cancel() {
	h.once.Do(func() { h.holder.RemoveWatcherByID(h.pattern, h.id) })
}

// MatchedServiceKeys blocks until the pattern's initial server sync finishes,
// then returns the matched serviceKeys (namespace@@group@@service). Cancel
// the context to bound the wait.
func (h *FuzzyWatchHandle) MatchedServiceKeys(ctx context.Context) ([]string, error) {
	return h.holder.MatchedServiceKeys(ctx, h.pattern)
}
