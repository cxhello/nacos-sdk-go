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

package naming_grpc

import (
	"github.com/nacos-group/nacos-sdk-go/v3/clients/naming_client/naming_cache"
	"github.com/nacos-group/nacos-sdk-go/v3/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v3/common/remote/rpc"
	"github.com/nacos-group/nacos-sdk-go/v3/common/remote/rpc/rpc_request"
	"github.com/nacos-group/nacos-sdk-go/v3/common/remote/rpc/rpc_response"
)

// FuzzyWatchSyncRequestHandler handles server-pushed NamingFuzzyWatchSyncRequest
// (INIT / DIFF / FINISH batch syncs) by driving the holder's state machine and
// acking with a NamingFuzzyWatchSyncResponse.
type FuzzyWatchSyncRequestHandler struct {
	fuzzyWatchHolder *naming_cache.FuzzyWatchServiceListHolder
}

func (*FuzzyWatchSyncRequestHandler) Name() string {
	return "FuzzyWatchSyncRequestHandler"
}

func (h *FuzzyWatchSyncRequestHandler) RequestReply(request rpc_request.IRequest, _ *rpc.RpcClient) rpc_response.IResponse {
	syncRequest, ok := request.(*rpc_request.NamingFuzzyWatchSyncRequest)
	if !ok {
		return nil
	}
	h.fuzzyWatchHolder.HandleSync(syncRequest.GroupKeyPattern, syncRequest.SyncType, syncRequest.Contexts,
		syncRequest.TotalBatch, syncRequest.CurrentBatch)
	return &rpc_response.NamingFuzzyWatchSyncResponse{
		Response: &rpc_response.Response{ResultCode: constant.RESPONSE_CODE_SUCCESS, Success: true},
	}
}

// FuzzyWatchChangeNotifyRequestHandler handles server-pushed
// NamingFuzzyWatchChangeNotifyRequest (a single service add/delete) and acks
// with a NamingFuzzyWatchChangeNotifyResponse.
type FuzzyWatchChangeNotifyRequestHandler struct {
	fuzzyWatchHolder *naming_cache.FuzzyWatchServiceListHolder
}

func (*FuzzyWatchChangeNotifyRequestHandler) Name() string {
	return "FuzzyWatchChangeNotifyRequestHandler"
}

func (h *FuzzyWatchChangeNotifyRequestHandler) RequestReply(request rpc_request.IRequest, _ *rpc.RpcClient) rpc_response.IResponse {
	changeRequest, ok := request.(*rpc_request.NamingFuzzyWatchChangeNotifyRequest)
	if !ok {
		return nil
	}
	h.fuzzyWatchHolder.HandleChangeNotify(changeRequest.ServiceKey, changeRequest.ChangedType)
	return &rpc_response.NamingFuzzyWatchChangeNotifyResponse{
		Response: &rpc_response.Response{ResultCode: constant.RESPONSE_CODE_SUCCESS, Success: true},
	}
}
