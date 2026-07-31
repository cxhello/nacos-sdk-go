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
	"github.com/nacos-group/nacos-sdk-proto/go/naming"
	"google.golang.org/protobuf/proto"

	"github.com/nacos-group/nacos-sdk-go/v3/model"
)

// ProtoMessage implementations for naming requests. Field mapping is 1:1
// with the legacy JSON body; `module` is a server-side derived field and
// does not exist in the proto definitions (same rule as the PR2 batch).

func toProtoInstance(i model.Instance) *naming.Instance {
	return &naming.Instance{
		InstanceId:  i.InstanceId,
		Ip:          i.Ip,
		Port:        int32(i.Port),
		Weight:      i.Weight,
		Healthy:     i.Healthy,
		Enabled:     i.Enable,
		Ephemeral:   i.Ephemeral,
		ClusterName: i.ClusterName,
		ServiceName: i.ServiceName,
		Metadata:    i.Metadata,
	}
}

func toProtoInstances(list []model.Instance) []*naming.Instance {
	out := make([]*naming.Instance, 0, len(list))
	for _, i := range list {
		out = append(out, toProtoInstance(i))
	}
	return out
}

func (r *InstanceRequest) ProtoMessage() proto.Message {
	return &naming.InstanceRequest{
		RequestId:   r.RequestId,
		Namespace:   r.Namespace,
		ServiceName: r.ServiceName,
		GroupName:   r.GroupName,
		Type:        r.Type,
		Instance:    toProtoInstance(r.Instance),
	}
}

func (r *BatchInstanceRequest) ProtoMessage() proto.Message {
	return &naming.BatchInstanceRequest{
		RequestId:   r.RequestId,
		Namespace:   r.Namespace,
		ServiceName: r.ServiceName,
		GroupName:   r.GroupName,
		Type:        r.Type,
		Instances:   toProtoInstances(r.Instances),
	}
}

func (r *SubscribeServiceRequest) ProtoMessage() proto.Message {
	return &naming.SubscribeServiceRequest{
		RequestId:   r.RequestId,
		Namespace:   r.Namespace,
		ServiceName: r.ServiceName,
		GroupName:   r.GroupName,
		Subscribe:   r.Subscribe,
		Clusters:    r.Clusters,
	}
}

func (r *ServiceQueryRequest) ProtoMessage() proto.Message {
	return &naming.ServiceQueryRequest{
		RequestId:   r.RequestId,
		Namespace:   r.Namespace,
		ServiceName: r.ServiceName,
		GroupName:   r.GroupName,
		Cluster:     r.Cluster,
		HealthyOnly: r.HealthyOnly,
		UdpPort:     int32(r.UdpPort),
	}
}

func (r *ServiceListRequest) ProtoMessage() proto.Message {
	return &naming.ServiceListRequest{
		RequestId:   r.RequestId,
		Namespace:   r.Namespace,
		ServiceName: r.ServiceName,
		GroupName:   r.GroupName,
		PageNo:      int32(r.PageNo),
		PageSize:    int32(r.PageSize),
		Selector:    r.Selector,
	}
}
