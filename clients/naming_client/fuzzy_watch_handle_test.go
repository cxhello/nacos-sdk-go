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
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nacos-group/nacos-sdk-go/v3/clients/naming_client/naming_cache"
	"github.com/nacos-group/nacos-sdk-go/v3/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v3/model"
)

// The naming_client-level sentinels must be the exact same error values as
// their naming_cache originals, so a caller can errors.Is against the
// re-export without importing the internal cache package.
func TestFuzzyWatchSentinelErrorsAreReExported(t *testing.T) {
	assert.True(t, errors.Is(ErrFuzzyWatchNotSupported, naming_cache.ErrFuzzyWatchNotSupported))
	assert.True(t, errors.Is(ErrFuzzyWatchClientClosed, naming_cache.ErrFuzzyWatchClientClosed))
}

// fakeRequesterForClient is a minimal naming_cache.FuzzyWatchRequester for
// handle-level tests: it always reports fuzzy watch as supported and never
// actually sends anything, which is fine here since the reconcile worker is
// never started (RegisterWatcher/RemoveWatcherByID only touch local state).
type fakeRequesterForClient struct{ supported bool }

func newFakeRequesterForClient() *fakeRequesterForClient {
	return &fakeRequesterForClient{supported: true}
}

func (f *fakeRequesterForClient) SendFuzzyWatchRequest(groupKeyPattern, watchType string, receivedGroupKeys []string, isInitializing bool) error {
	return nil
}

func (f *fakeRequesterForClient) ServerSupportsFuzzyWatch() bool { return f.supported }

// Cancel is idempotent: a second Cancel on the same handle is a no-op, and
// canceling one handle must not affect another handle registered on the same
// pattern.
func TestFuzzyWatchHandleCancelIsIdempotentAndScoped(t *testing.T) {
	r := newFakeRequesterForClient()
	h := naming_cache.NewFuzzyWatchServiceListHolder("public")
	h.SetRequester(r)
	got := make(chan model.FuzzyWatchChangeEvent, 4)
	id1, err := h.RegisterWatcher("public>>g>>svc*", func(ev model.FuzzyWatchChangeEvent) { got <- ev }, nil)
	require.NoError(t, err)
	id2, err := h.RegisterWatcher("public>>g>>svc*", func(ev model.FuzzyWatchChangeEvent) { got <- ev }, nil)
	require.NoError(t, err)
	handle1 := NewFuzzyWatchHandleForTest(h, "public>>g>>svc*", id1)
	handle1.Cancel()
	handle1.Cancel() // idempotent: must not panic, must not affect id2
	_ = id2
	h.HandleChangeNotify("public@@g@@svc1", constant.FUZZY_WATCH_CHANGED_TYPE_ADD_SERVICE)
	select {
	case <-got:
	case <-time.After(2 * time.Second):
		t.Fatal("remaining watcher must keep receiving after another handle canceled")
	}
}
