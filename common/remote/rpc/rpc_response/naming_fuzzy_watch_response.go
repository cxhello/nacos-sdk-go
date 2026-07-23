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

package rpc_response

// NamingFuzzyWatchResponse is the server's ack for a client-initiated
// NamingFuzzyWatchRequest (register/cancel a fuzzy watch pattern). It
// carries no fields beyond the base Response (resultCode/errorCode/
// message/requestId), matching Java's NamingFuzzyWatchResponse.
type NamingFuzzyWatchResponse struct {
	*Response
}

func (c *NamingFuzzyWatchResponse) GetResponseType() string {
	return "NamingFuzzyWatchResponse"
}

// NamingFuzzyWatchSyncResponse is the client's ack sent back after
// processing a server-pushed NamingFuzzyWatchSyncRequest.
type NamingFuzzyWatchSyncResponse struct {
	*Response
}

func (c *NamingFuzzyWatchSyncResponse) GetResponseType() string {
	return "NamingFuzzyWatchSyncResponse"
}

// NamingFuzzyWatchChangeNotifyResponse is the client's ack sent back after
// processing a server-pushed NamingFuzzyWatchChangeNotifyRequest.
type NamingFuzzyWatchChangeNotifyResponse struct {
	*Response
}

func (c *NamingFuzzyWatchChangeNotifyResponse) GetResponseType() string {
	return "NamingFuzzyWatchChangeNotifyResponse"
}
