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

package codec

import (
	"encoding/json"
	"testing"

	nacos_grpc_service "github.com/nacos-group/nacos-sdk-proto/go"
	"github.com/nacos-group/nacos-sdk-proto/go/common"
	"github.com/nacos-group/nacos-sdk-proto/go/config"
	"github.com/nacos-group/nacos-sdk-proto/go/naming"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

func TestNewPayloadCodec(t *testing.T) {
	codec := NewPayloadCodec()
	assert.NotNil(t, codec)
	assert.NotEmpty(t, codec.registry)
}

func TestPayloadCodec_Encode_ServerCheckRequest(t *testing.T) {
	codec := NewPayloadCodec()
	msg := &common.ServerCheckRequest{
		RequestId: "test-123",
	}
	headers := map[string]string{"module": "internal"}

	payload, err := codec.Encode("ServerCheckRequest", msg, headers, "127.0.0.1")

	assert.NoError(t, err)
	assert.NotNil(t, payload)
	assert.Equal(t, "ServerCheckRequest", payload.GetMetadata().GetType())
	assert.Equal(t, "127.0.0.1", payload.GetMetadata().GetClientIp())
	assert.Equal(t, "internal", payload.GetMetadata().GetHeaders()["module"])
	assert.NotEmpty(t, payload.GetBody().GetValue())
}

func TestPayloadCodec_Decode_ServerCheckResponse(t *testing.T) {
	codec := NewPayloadCodec()
	jsonBytes := []byte(`{"resultCode":200,"requestId":"resp-456","connectionId":"conn-1","supportAbilityNegotiation":true}`)

	payload := &nacos_grpc_service.Payload{
		Metadata: &nacos_grpc_service.Metadata{
			Type: "ServerCheckResponse",
		},
		Body: &anypb.Any{
			Value: jsonBytes,
		},
	}

	msg, err := codec.Decode(payload)

	assert.NoError(t, err)
	resp, ok := msg.(*common.ServerCheckResponse)
	assert.True(t, ok)
	assert.Equal(t, "resp-456", resp.GetRequestId())
	assert.Equal(t, "conn-1", resp.GetConnectionId())
}

func TestPayloadCodec_Encode_Decode_Roundtrip_ConfigQuery(t *testing.T) {
	codec := NewPayloadCodec()

	original := &config.ConfigQueryRequest{
		RequestId: "req-789",
		DataId:    "app.properties",
		Group:     "DEFAULT_GROUP",
		Tenant:    "public",
	}

	payload, err := codec.Encode("ConfigQueryRequest", original, nil, "10.0.0.1")
	assert.NoError(t, err)

	decoded, err := codec.Decode(payload)
	assert.NoError(t, err)

	result, ok := decoded.(*config.ConfigQueryRequest)
	assert.True(t, ok)
	assert.Equal(t, "req-789", result.GetRequestId())
	assert.Equal(t, "app.properties", result.GetDataId())
	assert.Equal(t, "DEFAULT_GROUP", result.GetGroup())
	assert.Equal(t, "public", result.GetTenant())
}

func TestPayloadCodec_Encode_Decode_Roundtrip_InstanceRequest(t *testing.T) {
	codec := NewPayloadCodec()

	original := &naming.InstanceRequest{
		RequestId:   "req-naming-1",
		Namespace:   "public",
		ServiceName: "my-service",
		GroupName:   "DEFAULT_GROUP",
		Type:        "registerInstance",
		Instance: &naming.Instance{
			Ip:          "192.168.1.100",
			Port:        8080,
			Weight:      1.0,
			Healthy:     true,
			Enabled:     true,
			Ephemeral:   true,
			ClusterName: "DEFAULT",
			ServiceName: "my-service",
			Metadata:    map[string]string{"version": "1.0"},
		},
	}

	payload, err := codec.Encode("InstanceRequest", original, map[string]string{
		"module": "naming",
	}, "192.168.1.100")
	assert.NoError(t, err)

	decoded, err := codec.Decode(payload)
	assert.NoError(t, err)

	result, ok := decoded.(*naming.InstanceRequest)
	assert.True(t, ok)
	assert.Equal(t, "req-naming-1", result.GetRequestId())
	assert.Equal(t, "public", result.GetNamespace())
	assert.Equal(t, "my-service", result.GetServiceName())
	assert.Equal(t, "registerInstance", result.GetType())
	assert.Equal(t, "192.168.1.100", result.GetInstance().GetIp())
	assert.Equal(t, int32(8080), result.GetInstance().GetPort())
	assert.Equal(t, "1.0", result.GetInstance().GetMetadata()["version"])
}

func TestPayloadCodec_Decode_UnknownType(t *testing.T) {
	codec := NewPayloadCodec()

	payload := &nacos_grpc_service.Payload{
		Metadata: &nacos_grpc_service.Metadata{
			Type: "NonExistentRequest",
		},
		Body: &anypb.Any{
			Value: []byte(`{}`),
		},
	}

	msg, err := codec.Decode(payload)

	assert.Error(t, err)
	assert.Nil(t, msg)
	assert.Contains(t, err.Error(), "unknown message type")
}

func TestPayloadCodec_Decode_InvalidJSON(t *testing.T) {
	codec := NewPayloadCodec()

	payload := &nacos_grpc_service.Payload{
		Metadata: &nacos_grpc_service.Metadata{
			Type: "ServerCheckRequest",
		},
		Body: &anypb.Any{
			Value: []byte(`{not valid json`),
		},
	}

	msg, err := codec.Decode(payload)

	assert.Error(t, err)
	assert.Nil(t, msg)
}

func TestPayloadCodec_Encode_NilHeaders(t *testing.T) {
	codec := NewPayloadCodec()
	msg := &common.HealthCheckRequest{
		RequestId: "health-1",
	}

	payload, err := codec.Encode("HealthCheckRequest", msg, nil, "127.0.0.1")

	assert.NoError(t, err)
	assert.NotNil(t, payload)
	assert.Nil(t, payload.GetMetadata().GetHeaders())
}

func TestPayloadCodec_Register_CustomType(t *testing.T) {
	codec := NewPayloadCodec()

	codec.Register("CustomTestType", func() proto.Message {
		return &common.HealthCheckRequest{}
	})

	payload := &nacos_grpc_service.Payload{
		Metadata: &nacos_grpc_service.Metadata{
			Type: "CustomTestType",
		},
		Body: &anypb.Any{
			Value: []byte(`{"requestId":"custom-1"}`),
		},
	}

	msg, err := codec.Decode(payload)
	assert.NoError(t, err)
	assert.Equal(t, "custom-1", msg.(*common.HealthCheckRequest).GetRequestId())
}

func TestPayloadCodec_AllCommonTypesRegistered(t *testing.T) {
	codec := NewPayloadCodec()

	commonTypes := []string{
		"ClientDetectionRequest", "ClientDetectionResponse",
		"ConnectResetRequest", "ConnectResetResponse",
		"ConnectionSetupRequest",
		"ErrorResponse",
		"HealthCheckRequest", "HealthCheckResponse",
		"PushAckRequest",
		"ServerCheckRequest", "ServerCheckResponse",
		"ServerLoaderInfoRequest", "ServerLoaderInfoResponse",
		"ServerReloadRequest", "ServerReloadResponse",
		"SetupAckRequest", "SetupAckResponse",
	}

	for _, typeName := range commonTypes {
		factory, ok := codec.registry[typeName]
		assert.True(t, ok, "common type %s should be registered", typeName)
		assert.NotNil(t, factory(), "factory for %s should return non-nil", typeName)
	}
}

func TestPayloadCodec_AllNamingTypesRegistered(t *testing.T) {
	codec := NewPayloadCodec()

	namingTypes := []string{
		"InstanceRequest", "InstanceResponse",
		"BatchInstanceRequest", "BatchInstanceResponse",
		"ServiceQueryRequest", "QueryServiceResponse",
		"ServiceListRequest", "ServiceListResponse",
		"SubscribeServiceRequest", "SubscribeServiceResponse",
		"NotifySubscriberRequest", "NotifySubscriberResponse",
		"PersistentInstanceRequest",
		"NamingFuzzyWatchRequest", "NamingFuzzyWatchResponse",
		"NamingFuzzyWatchSyncRequest", "NamingFuzzyWatchSyncResponse",
		"NamingFuzzyWatchChangeNotifyRequest", "NamingFuzzyWatchChangeNotifyResponse",
	}

	for _, typeName := range namingTypes {
		factory, ok := codec.registry[typeName]
		assert.True(t, ok, "naming type %s should be registered", typeName)
		assert.NotNil(t, factory(), "factory for %s should return non-nil", typeName)
	}
}

func TestPayloadCodec_AllConfigTypesRegistered(t *testing.T) {
	codec := NewPayloadCodec()

	configTypes := []string{
		"ConfigQueryRequest", "ConfigQueryResponse",
		"ConfigPublishRequest", "ConfigPublishResponse",
		"ConfigRemoveRequest", "ConfigRemoveResponse",
		"ConfigBatchListenRequest", "ConfigChangeBatchListenResponse",
		"ConfigChangeNotifyRequest", "ConfigChangeNotifyResponse",
		"ConfigChangeClusterSyncRequest", "ConfigChangeClusterSyncResponse",
		"ClientConfigMetricRequest", "ClientConfigMetricResponse",
		"ConfigFuzzyWatchRequest", "ConfigFuzzyWatchResponse",
		"ConfigFuzzyWatchSyncRequest", "ConfigFuzzyWatchSyncResponse",
		"ConfigFuzzyWatchChangeNotifyRequest", "ConfigFuzzyWatchChangeNotifyResponse",
	}

	for _, typeName := range configTypes {
		factory, ok := codec.registry[typeName]
		assert.True(t, ok, "config type %s should be registered", typeName)
		assert.NotNil(t, factory(), "factory for %s should return non-nil", typeName)
	}
}

func TestPayloadCodec_AllLockTypesRegistered(t *testing.T) {
	codec := NewPayloadCodec()

	lockTypes := []string{
		"LockOperationRequest", "LockOperationResponse",
	}

	for _, typeName := range lockTypes {
		factory, ok := codec.registry[typeName]
		assert.True(t, ok, "lock type %s should be registered", typeName)
		assert.NotNil(t, factory(), "factory for %s should return non-nil", typeName)
	}
}

func TestEncodeEmitsDefaultValues(t *testing.T) {
	c := NewPayloadCodec()
	payload, err := c.Encode("HealthCheckRequest", &common.HealthCheckRequest{}, nil, "1.2.3.4")
	require.NoError(t, err)
	var m map[string]interface{}
	require.NoError(t, json.Unmarshal(payload.GetBody().GetValue(), &m))
	// 零值字段必须显式输出（PoC 结论：省略零值会让服务端无法区分未设置与零值）
	v, ok := m["requestId"]
	assert.True(t, ok, "requestId must be emitted even when zero-valued")
	assert.Equal(t, "", v)
}

func TestDecodeDiscardsUnknownFields(t *testing.T) {
	c := NewPayloadCodec()
	// 服务端 JSON 携带 proto 中不存在的派生字段 success（Java getter 序列化产物）
	body := []byte(`{"resultCode":200,"connectionId":"c-1","success":true}`)
	payload := &nacos_grpc_service.Payload{
		Metadata: &nacos_grpc_service.Metadata{Type: "ServerCheckResponse"},
		Body:     &anypb.Any{Value: body},
	}
	msg, err := c.Decode(payload)
	require.NoError(t, err, "unknown fields from server must be tolerated")
	resp, ok := msg.(*common.ServerCheckResponse)
	require.True(t, ok)
	assert.Equal(t, int32(200), resp.ResultCode)
	assert.Equal(t, "c-1", resp.ConnectionId)
}
