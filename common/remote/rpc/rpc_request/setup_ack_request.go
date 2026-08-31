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

// SetupAckRequest is pushed by 2.2+ servers right after connection setup and
// carries the server's ability table (Java
// com.alibaba.nacos.api.remote.request.SetupAckRequest). Storing the table on
// the receiving connection is what lets feature code fail fast on servers
// that lack an ability instead of retrying forever.
type SetupAckRequest struct {
	*InternalRequest
	AbilityTable map[string]bool `json:"abilityTable"`
}

func (r *SetupAckRequest) GetRequestType() string {
	return "SetupAckRequest"
}
