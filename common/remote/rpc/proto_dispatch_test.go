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
	"testing"

	nacos_grpc_service "github.com/nacos-group/nacos-sdk-proto/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/nacos-group/nacos-sdk-go/v3/common/remote/rpc/rpc_response"
)

func protoPayload(typeName, body string) *nacos_grpc_service.Payload {
	return &nacos_grpc_service.Payload{
		Metadata: &nacos_grpc_service.Metadata{Type: typeName},
		Body:     &anypb.Any{Value: []byte(body)},
	}
}

func TestDecodeProtoResponseServerCheck(t *testing.T) {
	// 服务端真实形态：含 proto 中不存在的 success 派生字段
	resp, migrated, err := decodeProtoResponse(protoPayload("ServerCheckResponse",
		`{"resultCode":200,"connectionId":"c-1","success":true,"requestId":"r-1"}`))
	require.NoError(t, err)
	require.True(t, migrated)
	sc, ok := resp.(*rpc_response.ServerCheckResponse)
	require.True(t, ok, "downstream type assertions must keep working")
	assert.Equal(t, "c-1", sc.ConnectionId)
	assert.True(t, sc.IsSuccess())
	assert.Equal(t, "r-1", sc.RequestId)
}

func TestDecodeProtoResponseSuccessDerivedFromResultCode(t *testing.T) {
	// wire 无显式 success 时按 resultCode==200 推导（对齐 InnerResponseJsonUnmarshal）
	resp, migrated, err := decodeProtoResponse(protoPayload("HealthCheckResponse", `{"resultCode":200}`))
	require.NoError(t, err)
	require.True(t, migrated)
	assert.True(t, resp.IsSuccess())
}

func TestDecodeProtoResponseExplicitFailure(t *testing.T) {
	resp, migrated, err := decodeProtoResponse(protoPayload("ErrorResponse",
		`{"resultCode":500,"errorCode":403,"message":"forbidden","success":false}`))
	require.NoError(t, err)
	require.True(t, migrated)
	assert.False(t, resp.IsSuccess())
	assert.Equal(t, 403, resp.GetErrorCode())
}

func TestDecodeProtoResponseUnmigratedFallsThrough(t *testing.T) {
	_, migrated, err := decodeProtoResponse(protoPayload("ConfigQueryResponse", `{"resultCode":200}`))
	require.NoError(t, err)
	assert.False(t, migrated, "config/naming types stay on the legacy path until PR4/PR5")
}
