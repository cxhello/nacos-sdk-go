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
	"strconv"

	"github.com/nacos-group/nacos-sdk-proto/go/naming"

	"github.com/nacos-group/nacos-sdk-go/v3/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v3/model"
)

// Java defaults for the metadata-derived heartbeat lifecycle fields
// (com.alibaba.nacos.api.common.Constants: DEFAULT_HEART_BEAT_INTERVAL /
// DEFAULT_HEART_BEAT_TIMEOUT / DEFAULT_IP_DELETE_TIMEOUT, in milliseconds).
const (
	defaultHeartBeatInterval = 5000
	defaultHeartBeatTimeout  = 15000
	defaultIpDeleteTimeout   = 30000
)

// metadataIntWithDefault mirrors Java Instance#getMetaDataByKeyWithDefault:
// only a plain non-negative integer string overrides the default; anything
// missing or malformed falls back.
func metadataIntWithDefault(metadata map[string]string, key string, def int) int {
	v, ok := metadata[key]
	if !ok || v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		return def
	}
	return n
}

// fromProtoInstance adapts a proto Instance to the legacy model struct.
// The proto definition has no instanceHeartBeatInterval /
// instanceHeartBeatTimeOut / ipDeleteTimeout fields: on the legacy JSON
// wire the server serializes Java's metadata-derived getters, so the
// adapter reproduces that derivation (preserved.* metadata keys with the
// Java defaults) to keep the exported model.Instance fields identical
// across both wire formats.
func fromProtoInstance(i *naming.Instance) model.Instance {
	if i == nil {
		return model.Instance{}
	}
	return model.Instance{
		InstanceId:  i.InstanceId,
		Ip:          i.Ip,
		Port:        uint64(i.Port),
		Weight:      i.Weight,
		Healthy:     i.Healthy,
		Enable:      i.Enabled,
		Ephemeral:   i.Ephemeral,
		ClusterName: i.ClusterName,
		ServiceName: i.ServiceName,
		Metadata:    i.Metadata,
		InstanceHeartBeatInterval: metadataIntWithDefault(i.Metadata,
			constant.HEART_BEAT_INTERVAL, defaultHeartBeatInterval),
		InstanceHeartBeatTimeOut: metadataIntWithDefault(i.Metadata,
			constant.HEART_BEAT_TIMEOUT, defaultHeartBeatTimeout),
		IpDeleteTimeout: metadataIntWithDefault(i.Metadata,
			constant.IP_DELETE_TIMEOUT, defaultIpDeleteTimeout),
	}
}

// fromProtoServiceInfo adapts a proto ServiceInfo to model.Service. The
// proto has no `valid` field (Java derived); every real server emits
// valid=true on the legacy wire and the SDK never branches on it, so it
// is pinned to true for wire compatibility.
func fromProtoServiceInfo(si *naming.ServiceInfo) model.Service {
	if si == nil {
		return model.Service{}
	}
	hosts := make([]model.Instance, 0, len(si.Hosts))
	for _, h := range si.Hosts {
		hosts = append(hosts, fromProtoInstance(h))
	}
	return model.Service{
		Name:                     si.Name,
		GroupName:                si.GroupName,
		Clusters:                 si.Clusters,
		CacheMillis:              uint64(si.CacheMillis),
		Hosts:                    hosts,
		LastRefTime:              uint64(si.LastRefTime),
		Checksum:                 si.Checksum,
		AllIPs:                   si.AllIps,
		ReachProtectionThreshold: si.ReachProtectionThreshold,
		Valid:                    true,
	}
}
