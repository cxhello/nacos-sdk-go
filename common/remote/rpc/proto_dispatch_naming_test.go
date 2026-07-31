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

	"github.com/nacos-group/nacos-sdk-proto/go/naming"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nacos-group/nacos-sdk-go/v3/common/remote/rpc/rpc_request"
	"github.com/nacos-group/nacos-sdk-go/v3/common/remote/rpc/rpc_response"
)

func TestDecodeProtoQueryServiceResponse(t *testing.T) {
	msg := &naming.QueryServiceResponse{
		ResultCode: 200, RequestId: "7",
		ServiceInfo: &naming.ServiceInfo{
			Name: "svc", GroupName: "g", Clusters: "",
			CacheMillis: 10000, LastRefTime: 1784787422078,
			Hosts: []*naming.Instance{{
				Ip: "192.168.100.1", Port: 8001, Weight: 10,
				Healthy: true, Enabled: true, Ephemeral: true,
				ClusterName: "DEFAULT", ServiceName: "g@@svc",
				Metadata: map[string]string{},
			}},
		},
	}
	payload, err := payloadCodec.Encode("QueryServiceResponse", msg, nil, "127.0.0.1")
	require.NoError(t, err)

	resp, migrated, err := decodeProtoResponse(payload)
	require.NoError(t, err)
	require.True(t, migrated)
	q, ok := resp.(*rpc_response.QueryServiceResponse)
	require.True(t, ok)
	assert.True(t, q.IsSuccess())
	assert.Equal(t, "svc", q.ServiceInfo.Name)
	assert.Equal(t, uint64(1784787422078), q.ServiceInfo.LastRefTime)
	require.Len(t, q.ServiceInfo.Hosts, 1)
	assert.Equal(t, uint64(8001), q.ServiceInfo.Hosts[0].Port)
	assert.True(t, q.ServiceInfo.Hosts[0].Enable)
	// proto ServiceInfo has no `valid` field; adapter pins it to true, which is
	// what every real server emits on the legacy JSON wire.
	assert.True(t, q.ServiceInfo.Valid)
}

func TestDecodeProtoServiceListResponse(t *testing.T) {
	msg := &naming.ServiceListResponse{ResultCode: 200, Count: 3, ServiceNames: []string{"a", "b", "c"}}
	payload, err := payloadCodec.Encode("ServiceListResponse", msg, nil, "127.0.0.1")
	require.NoError(t, err)
	resp, migrated, err := decodeProtoResponse(payload)
	require.NoError(t, err)
	require.True(t, migrated)
	l := resp.(*rpc_response.ServiceListResponse)
	assert.Equal(t, 3, l.Count)
	assert.Equal(t, []string{"a", "b", "c"}, l.ServiceNames)
}

func TestDecodeProtoInstanceResponse(t *testing.T) {
	msg := &naming.InstanceResponse{ResultCode: 200, RequestId: "1"}
	payload, err := payloadCodec.Encode("InstanceResponse", msg, nil, "127.0.0.1")
	require.NoError(t, err)

	resp, migrated, err := decodeProtoResponse(payload)
	require.NoError(t, err)
	require.True(t, migrated)
	i, ok := resp.(*rpc_response.InstanceResponse)
	require.True(t, ok)
	assert.True(t, i.IsSuccess())
	assert.Equal(t, "1", i.RequestId)
}

func TestDecodeProtoBatchInstanceResponse(t *testing.T) {
	msg := &naming.BatchInstanceResponse{ResultCode: 200, RequestId: "2"}
	payload, err := payloadCodec.Encode("BatchInstanceResponse", msg, nil, "127.0.0.1")
	require.NoError(t, err)

	resp, migrated, err := decodeProtoResponse(payload)
	require.NoError(t, err)
	require.True(t, migrated)
	b, ok := resp.(*rpc_response.BatchInstanceResponse)
	require.True(t, ok)
	assert.True(t, b.IsSuccess())
	assert.Equal(t, "2", b.RequestId)
}

func TestDecodeProtoSubscribeServiceResponse(t *testing.T) {
	msg := &naming.SubscribeServiceResponse{
		ResultCode: 200, RequestId: "3",
		ServiceInfo: &naming.ServiceInfo{
			Name: "svc", GroupName: "g", Clusters: "",
			CacheMillis: 10000, LastRefTime: 1784787422078,
			Hosts: []*naming.Instance{{
				Ip: "192.168.100.1", Port: 8001, Weight: 10,
				Healthy: true, Enabled: true, Ephemeral: true,
				ClusterName: "DEFAULT", ServiceName: "g@@svc",
				Metadata: map[string]string{},
			}},
		},
	}
	payload, err := payloadCodec.Encode("SubscribeServiceResponse", msg, nil, "127.0.0.1")
	require.NoError(t, err)

	resp, migrated, err := decodeProtoResponse(payload)
	require.NoError(t, err)
	require.True(t, migrated)
	s, ok := resp.(*rpc_response.SubscribeServiceResponse)
	require.True(t, ok)
	assert.True(t, s.IsSuccess())
	assert.Equal(t, "svc", s.ServiceInfo.Name)
	assert.Equal(t, uint64(1784787422078), s.ServiceInfo.LastRefTime)
	require.Len(t, s.ServiceInfo.Hosts, 1)
	assert.Equal(t, uint64(8001), s.ServiceInfo.Hosts[0].Port)
	assert.True(t, s.ServiceInfo.Hosts[0].Enable)
	assert.True(t, s.ServiceInfo.Valid)
}

func TestDecodeProtoNotifySubscriberRequest(t *testing.T) {
	msg := &naming.NotifySubscriberRequest{
		RequestId: "9", Namespace: "ns", ServiceName: "svc", GroupName: "g",
		ServiceInfo: &naming.ServiceInfo{Name: "svc", GroupName: "g", LastRefTime: 123,
			Hosts: []*naming.Instance{{Ip: "1.2.3.4", Port: 80, Enabled: true}}},
	}
	payload, err := payloadCodec.Encode("NotifySubscriberRequest", msg, nil, "127.0.0.1")
	require.NoError(t, err)

	req, decoded := decodeProtoServerRequest(payload)
	require.True(t, decoded)
	n, ok := req.(*rpc_request.NotifySubscriberRequest)
	require.True(t, ok)
	assert.Equal(t, "9", n.RequestId)
	assert.Equal(t, "ns", n.Namespace)
	assert.Equal(t, "svc", n.ServiceInfo.Name)
	assert.Equal(t, uint64(123), n.ServiceInfo.LastRefTime)
	require.Len(t, n.ServiceInfo.Hosts, 1)
	assert.True(t, n.ServiceInfo.Hosts[0].Enable)
}

// The proto Instance has no heartbeat lifecycle fields; the legacy JSON wire
// carried them because the server serializes Java's metadata-derived getters.
// The adapter must reproduce that derivation so callers reading
// model.Instance keep seeing the same values as on the JSON path.
func TestFromProtoInstanceHeartbeatDefaults(t *testing.T) {
	inst := fromProtoInstance(&naming.Instance{Ip: "1.1.1.1", Port: 8080})
	assert.Equal(t, 5000, inst.InstanceHeartBeatInterval)
	assert.Equal(t, 15000, inst.InstanceHeartBeatTimeOut)
	assert.Equal(t, 30000, inst.IpDeleteTimeout)
}

func TestFromProtoInstanceHeartbeatMetadataOverrides(t *testing.T) {
	inst := fromProtoInstance(&naming.Instance{Ip: "1.1.1.1", Port: 8080, Metadata: map[string]string{
		"preserved.heart.beat.interval": "2000",
		"preserved.heart.beat.timeout":  "6000",
		"preserved.ip.delete.timeout":   "9000",
	}})
	assert.Equal(t, 2000, inst.InstanceHeartBeatInterval)
	assert.Equal(t, 6000, inst.InstanceHeartBeatTimeOut)
	assert.Equal(t, 9000, inst.IpDeleteTimeout)
}

// Java only accepts values matching ^\d+$ (getMetaDataByKeyWithDefault);
// anything else falls back to the default.
func TestFromProtoInstanceHeartbeatInvalidMetadataFallsBack(t *testing.T) {
	inst := fromProtoInstance(&naming.Instance{Ip: "1.1.1.1", Port: 8080, Metadata: map[string]string{
		"preserved.heart.beat.interval": "abc",
		"preserved.heart.beat.timeout":  "-1",
		"preserved.ip.delete.timeout":   "",
	}})
	assert.Equal(t, 5000, inst.InstanceHeartBeatInterval)
	assert.Equal(t, 15000, inst.InstanceHeartBeatTimeOut)
	assert.Equal(t, 30000, inst.IpDeleteTimeout)
}

// strconv.Atoi accepts sign prefixes ("+1000", "-0") that Java's ^\d+$
// check rejects; both must fall back to the defaults for exact parity.
func TestFromProtoInstanceHeartbeatSignPrefixedMetadataFallsBack(t *testing.T) {
	inst := fromProtoInstance(&naming.Instance{Ip: "1.1.1.1", Port: 8080, Metadata: map[string]string{
		"preserved.heart.beat.interval": "+1000",
		"preserved.heart.beat.timeout":  "-0",
	}})
	assert.Equal(t, 5000, inst.InstanceHeartBeatInterval)
	assert.Equal(t, 15000, inst.InstanceHeartBeatTimeOut)
}
