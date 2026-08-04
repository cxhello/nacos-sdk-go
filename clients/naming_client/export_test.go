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

import "github.com/nacos-group/nacos-sdk-go/v3/clients/naming_client/naming_cache"

// NewFuzzyWatchHandleForTest constructs a FuzzyWatchHandle directly against
// an already-registered watcher, for tests that exercise handle semantics
// (Cancel idempotency/scoping) without going through a full NamingClient.
func NewFuzzyWatchHandleForTest(h *naming_cache.FuzzyWatchServiceListHolder, pattern string, id uint64) *FuzzyWatchHandle {
	return &FuzzyWatchHandle{holder: h, pattern: pattern, id: id}
}
