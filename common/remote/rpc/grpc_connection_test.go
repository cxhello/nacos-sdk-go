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

package rpc

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nacos-group/nacos-sdk-go/v3/common/remote/rpc/rpc_request"
)

func TestConvertRequestProtoPath(t *testing.T) {
	req := rpc_request.NewHealthCheckRequest()
	req.Headers["k"] = "v"
	p := convertRequest(req)
	assert.Equal(t, "HealthCheckRequest", p.GetMetadata().GetType())
	assert.Equal(t, "v", p.GetMetadata().GetHeaders()["k"])
	var body map[string]interface{}
	require.NoError(t, json.Unmarshal(p.GetBody().GetValue(), &body))
	_, hasRequestId := body["requestId"]
	assert.True(t, hasRequestId, "proto path emits default values")
	_, hasModule := body["module"]
	assert.False(t, hasModule, "proto path body must come from protojson, not legacy struct")
}

func TestConvertRequestLegacyPath(t *testing.T) {
	req := &rpc_request.ConfigQueryRequest{ConfigRequest: rpc_request.NewConfigRequest("g", "d", "t")}
	p := convertRequest(req)
	assert.Equal(t, req.GetRequestType(), p.GetMetadata().GetType())
	assert.Equal(t, req.GetBody(req), string(p.GetBody().GetValue()), "legacy path must be byte-compatible with json.Marshal")
}
