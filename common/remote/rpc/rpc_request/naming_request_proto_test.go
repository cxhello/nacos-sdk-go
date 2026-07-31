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
	"testing"

	"github.com/nacos-group/nacos-sdk-proto/go/naming"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nacos-group/nacos-sdk-go/v3/model"
)

func sampleInstance() model.Instance {
	return model.Instance{
		InstanceId: "id-1", Ip: "192.168.100.1", Port: 8001, Weight: 10,
		Healthy: true, Enable: true, Ephemeral: true,
		ClusterName: "DEFAULT", ServiceName: "DEFAULT_GROUP@@svc",
		Metadata: map[string]string{"idc": "test1"},
	}
}

func TestInstanceRequestProtoMessage(t *testing.T) {
	r := NewInstanceRequest("ns", "svc", "g", "registerInstance", sampleInstance())
	r.RequestId = "42"
	msg, ok := r.ProtoMessage().(*naming.InstanceRequest)
	require.True(t, ok)
	assert.Equal(t, "42", msg.RequestId)
	assert.Equal(t, "ns", msg.Namespace)
	assert.Equal(t, "svc", msg.ServiceName)
	assert.Equal(t, "g", msg.GroupName)
	assert.Equal(t, "registerInstance", msg.Type)
	require.NotNil(t, msg.Instance)
	assert.Equal(t, "192.168.100.1", msg.Instance.Ip)
	assert.Equal(t, int32(8001), msg.Instance.Port)
	assert.Equal(t, float64(10), msg.Instance.Weight)
	assert.True(t, msg.Instance.Enabled)
	assert.Equal(t, map[string]string{"idc": "test1"}, msg.Instance.Metadata)
}

func TestBatchInstanceRequestProtoMessage(t *testing.T) {
	r := NewBatchInstanceRequest("ns", "svc", "g", "batchRegisterInstance",
		[]model.Instance{sampleInstance(), sampleInstance()})
	msg, ok := r.ProtoMessage().(*naming.BatchInstanceRequest)
	require.True(t, ok)
	assert.Len(t, msg.Instances, 2)
	assert.Equal(t, "batchRegisterInstance", msg.Type)
}

func TestSubscribeServiceRequestProtoMessage(t *testing.T) {
	r := NewSubscribeServiceRequest("ns", "svc", "g", "c1,c2", true)
	msg, ok := r.ProtoMessage().(*naming.SubscribeServiceRequest)
	require.True(t, ok)
	assert.True(t, msg.Subscribe)
	assert.Equal(t, "c1,c2", msg.Clusters)
}

func TestServiceQueryRequestProtoMessage(t *testing.T) {
	r := NewServiceQueryRequest("ns", "svc", "g", "DEFAULT", true, 0)
	msg, ok := r.ProtoMessage().(*naming.ServiceQueryRequest)
	require.True(t, ok)
	assert.Equal(t, "DEFAULT", msg.Cluster)
	assert.True(t, msg.HealthyOnly)
	assert.Equal(t, int32(0), msg.UdpPort)
}

func TestServiceListRequestProtoMessage(t *testing.T) {
	r := NewServiceListRequest("ns", "", "g", 1, 100, "")
	msg, ok := r.ProtoMessage().(*naming.ServiceListRequest)
	require.True(t, ok)
	assert.Equal(t, int32(1), msg.PageNo)
	assert.Equal(t, int32(100), msg.PageSize)
}
