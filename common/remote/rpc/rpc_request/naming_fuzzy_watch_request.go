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

package rpc_request

import (
	"strconv"
	"time"

	"github.com/nacos-group/nacos-sdk-go/v3/common/constant"
)

// NamingFuzzyWatchRequest is the client-initiated request that (un)registers
// a fuzzy watch pattern with the server. It is sent as the legacy JSON body,
// not a proto message: the server deserializes it with Jackson into
// com.alibaba.nacos.api.naming.remote.request.NamingFuzzyWatchRequest, whose
// boolean field isInitializing has no @JsonProperty override, so Jackson's
// bean-property convention derives the wire name "initializing" (the "is"
// prefix is stripped for boolean getters/setters). The proto definition's
// json_name is fixed as "isInitializing" and cannot be overridden per-field,
// so this request does not implement codec.ProtoConvertible.
type NamingFuzzyWatchRequest struct {
	*Request
	IsInitializing    bool     `json:"initializing"`
	Namespace         string   `json:"namespace"`
	GroupKeyPattern   string   `json:"groupKeyPattern"`
	ReceivedGroupKeys []string `json:"receivedGroupKeys"`
	WatchType         string   `json:"watchType"`
}

// NewNamingFuzzyWatchRequest builds a NamingFuzzyWatchRequest for
// registering (constant.FUZZY_WATCH_TYPE_WATCH) or cancelling
// (constant.FUZZY_WATCH_TYPE_CANCEL_WATCH) a fuzzy watch on groupKeyPattern.
func NewNamingFuzzyWatchRequest(namespace, groupKeyPattern, watchType string, receivedGroupKeys []string, isInitializing bool) *NamingFuzzyWatchRequest {
	request := Request{
		Headers: make(map[string]string, 8),
	}
	return &NamingFuzzyWatchRequest{
		Request:           &request,
		IsInitializing:    isInitializing,
		Namespace:         namespace,
		GroupKeyPattern:   groupKeyPattern,
		ReceivedGroupKeys: receivedGroupKeys,
		WatchType:         watchType,
	}
}

func (r *NamingFuzzyWatchRequest) GetRequestType() string {
	return constant.FUZZY_WATCH_REQUEST_NAME
}

// GetStringToSign mirrors NamingRequest's no-service-name case: a fuzzy
// watch request carries a namespace and a group key pattern, not a single
// service/group pair, so there is nothing to sign beyond the timestamp.
func (r *NamingFuzzyWatchRequest) GetStringToSign() string {
	return strconv.FormatInt(time.Now().Unix()*1000, 10)
}

// NamingFuzzyWatchSyncContext mirrors sdk-proto's
// NamingFuzzyWatchSyncRequestContext / Java's
// NamingFuzzyWatchSyncRequest.Context: one matched service and how it
// changed relative to the client's last known state for the pattern.
type NamingFuzzyWatchSyncContext struct {
	ServiceKey  string `json:"serviceKey"`
	ChangedType string `json:"changedType"`
}

// NamingFuzzyWatchSyncRequest is a server-push request: the server sends
// this to sync (a batch of) the services currently matching a
// groupKeyPattern the client is watching. It has no client-side "New..."
// constructor because the SDK never constructs one to send - only
// decodeProtoServerRequest builds it from the server's proto message.
type NamingFuzzyWatchSyncRequest struct {
	*Request
	SyncType        string                        `json:"syncType"`
	GroupKeyPattern string                        `json:"groupKeyPattern"`
	Contexts        []NamingFuzzyWatchSyncContext `json:"contexts"`
	TotalBatch      int                           `json:"totalBatch"`
	CurrentBatch    int                           `json:"currentBatch"`
}

func (r *NamingFuzzyWatchSyncRequest) GetRequestType() string {
	return constant.FUZZY_WATCH_SYNC_REQUEST_NAME
}

// NamingFuzzyWatchChangeNotifyRequest is a server-push request: the server
// sends this when a single service covered by an already-synced pattern is
// added or deleted (constant.FUZZY_WATCH_CHANGED_TYPE_ADD_SERVICE /
// FUZZY_WATCH_CHANGED_TYPE_DELETE_SERVICE), as opposed to the batch sync
// carried by NamingFuzzyWatchSyncRequest.
type NamingFuzzyWatchChangeNotifyRequest struct {
	*Request
	SyncType    string `json:"syncType"`
	ServiceKey  string `json:"serviceKey"`
	ChangedType string `json:"changedType"`
}

func (r *NamingFuzzyWatchChangeNotifyRequest) GetRequestType() string {
	return constant.FUZZY_WATCH_CHANGE_NOTIFY_REQUEST_NAME
}
