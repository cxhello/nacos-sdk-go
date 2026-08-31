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
	"fmt"

	"github.com/pkg/errors"
)

// FuzzyWatchServerError is a non-success NamingFuzzyWatchResponse surfaced as
// an error with the server's errorCode preserved, so the reconcile worker can
// distinguish capacity rejections (suppress + notify load watchers) from
// transient failures (backoff + retry).
type FuzzyWatchServerError struct {
	ErrorCode int
	Message   string
}

func (e *FuzzyWatchServerError) Error() string {
	return fmt.Sprintf("fuzzy watch server error, errorCode:%d message:%s", e.ErrorCode, e.Message)
}

// FuzzyWatchRequester is the transport the reconcile worker sends
// WATCH/CANCEL_WATCH requests through. It is implemented by the gRPC naming
// proxy and injected via SetRequester after construction (mirrors Java
// NamingFuzzyWatchServiceListHolder.registerNamingGrpcClientProxy), which
// breaks the naming_cache -> naming_grpc import cycle.
type FuzzyWatchRequester interface {
	SendFuzzyWatchRequest(groupKeyPattern, watchType string, receivedGroupKeys []string, isInitializing bool) error
	ServerSupportsFuzzyWatch() bool
}

// ErrFuzzyWatchNotSupported is returned by RegisterWatcher when the connected
// server does not advertise the fuzzyWatch ability (2.x servers).
var ErrFuzzyWatchNotSupported = errors.New("fuzzy watch is not supported by the connected nacos server (requires nacos 3.x)")

// ErrFuzzyWatchClientClosed is returned by RegisterWatcher after the client's
// Shutdown has run: the reconcile worker is stopped, so a new watch could
// never be established.
var ErrFuzzyWatchClientClosed = errors.New("fuzzy watch rejected: the nacos client is already closed")
