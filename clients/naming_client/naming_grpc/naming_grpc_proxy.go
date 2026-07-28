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
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/nacos-group/nacos-sdk-go/v3/clients/naming_client/naming_cache"
	"github.com/nacos-group/nacos-sdk-go/v3/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v3/common/logger"
	"github.com/nacos-group/nacos-sdk-go/v3/common/monitor"
	"github.com/nacos-group/nacos-sdk-go/v3/common/nacos_server"
	"github.com/nacos-group/nacos-sdk-go/v3/common/remote/rpc"
	"github.com/nacos-group/nacos-sdk-go/v3/common/remote/rpc/rpc_request"
	"github.com/nacos-group/nacos-sdk-go/v3/common/remote/rpc/rpc_response"
	"github.com/nacos-group/nacos-sdk-go/v3/common/security"
	"github.com/nacos-group/nacos-sdk-go/v3/inner/uuid"
	"github.com/nacos-group/nacos-sdk-go/v3/model"
	"github.com/nacos-group/nacos-sdk-go/v3/util"
)

// NamingGrpcProxy ...
type NamingGrpcProxy struct {
	clientConfig      constant.ClientConfig
	nacosServer       *nacos_server.NacosServer
	rpcClient         rpc.IRpcClient
	eventListener     *ConnectionEventListener
	serviceInfoHolder *naming_cache.ServiceInfoHolder
	// send performs the actual server round-trip; it defaults to
	// requestToServer and is swapped out in tests so the request sequence can
	// be asserted without a live rpc client.
	send func(request rpc_request.IRequest) (rpc_response.IResponse, error)
	// redoMu is the redo-cache monitor: it mirrors Java NamingGrpcRedoService's
	// synchronized(registeredInstances), serializing every read-modify-write of
	// the instance redo cache across RegisterInstance, BatchRegisterInstance,
	// and DeregisterInstance. The batch-deregister path additionally holds it
	// across the republish send, mirroring Java NamingGrpcClientProxy's
	// batchDeregisterService, so a concurrent register's cache write cannot
	// interleave between the retained-set computation and the cache write it
	// is derived from. Send ordering is NOT guaranteed: Register and
	// BatchRegister release the mutex before sending, so a request already in
	// flight when a deregister runs can still land afterwards and leave the
	// server briefly different from the redo cache — the same inversion
	// exists in Java, where doRegisterService/doBatchRegisterService also run
	// outside the monitor; the redo replay reconverges the server on the next
	// reconnect.
	redoMu sync.Mutex
}

// NewNamingGrpcProxy create naming grpc proxy
func NewNamingGrpcProxy(ctx context.Context, clientCfg constant.ClientConfig, nacosServer *nacos_server.NacosServer,
	serviceInfoHolder *naming_cache.ServiceInfoHolder) (*NamingGrpcProxy, error) {
	srvProxy := NamingGrpcProxy{
		clientConfig:      clientCfg,
		nacosServer:       nacosServer,
		serviceInfoHolder: serviceInfoHolder,
	}
	srvProxy.send = srvProxy.requestToServer

	uid, err := uuid.NewV4()
	if err != nil {
		return nil, err
	}

	labels := map[string]string{
		constant.LABEL_SOURCE: constant.LABEL_SOURCE_SDK,
		constant.LABEL_MODULE: constant.LABEL_MODULE_NAMING,
	}

	iRpcClient, err := rpc.CreateClient(ctx, uid.String(), rpc.GRPC, labels, srvProxy.nacosServer, &clientCfg.TLSCfg, clientCfg.AppConnLabels)
	if err != nil {
		return nil, err
	}

	srvProxy.rpcClient = iRpcClient

	rpcClient := srvProxy.rpcClient.GetRpcClient()
	rpcClient.Start()

	rpcClient.RegisterServerRequestHandler(func() rpc_request.IRequest {
		return &rpc_request.NotifySubscriberRequest{NamingRequest: &rpc_request.NamingRequest{}}
	}, &rpc.NamingPushRequestHandler{ServiceInfoHolder: serviceInfoHolder})

	srvProxy.eventListener = NewConnectionEventListener(&srvProxy)
	rpcClient.RegisterConnectionListener(srvProxy.eventListener)

	return &srvProxy, nil
}

func (proxy *NamingGrpcProxy) requestToServer(request rpc_request.IRequest) (rpc_response.IResponse, error) {
	start := time.Now()
	proxy.nacosServer.InjectSecurityInfo(request.GetHeaders(), security.BuildNamingResourceByRequest(request))
	response, err := proxy.rpcClient.GetRpcClient().Request(request, int64(proxy.clientConfig.TimeoutMs))
	monitor.GetNamingRequestMonitor(constant.GRPC, request.GetRequestType(), rpc_response.GetGrpcResponseStatusCode(response)).Observe(float64(time.Now().Nanosecond() - start.Nanosecond()))
	return response, err
}

// RegisterInstance registers one instance and caches it in single-request
// shape for reconnect redo (Java parity: registerServiceForEphemeral). The
// server keeps ONE publication per (connection, service) and a second
// registerInstance on the same client REPLACES it (verified on Nacos
// 2.5.2/3.1.0/3.2.0, issue #866): to publish several instances of one
// service from one client, use BatchRegisterInstance.
func (proxy *NamingGrpcProxy) RegisterInstance(serviceName string, groupName string, instance model.Instance) (bool, error) {
	logger.Infof("register instance namespaceId:<%s>,serviceName:<%s> with instance:<%s>",
		proxy.clientConfig.NamespaceId, serviceName, util.ToJsonString(instance))
	proxy.redoMu.Lock()
	proxy.eventListener.CacheInstanceForRedo(serviceName, groupName, instance)
	proxy.redoMu.Unlock()
	instanceRequest := rpc_request.NewInstanceRequest(proxy.clientConfig.NamespaceId, serviceName, groupName, "registerInstance", instance)
	response, err := proxy.send(instanceRequest)
	if err != nil {
		return false, err
	}
	return response.IsSuccess(), nil
}

// BatchRegisterInstance wholesale-replaces the service publication and caches
// the list in batch shape for redo. instances may legally be empty ([] clears
// the publication server-side, probe-verified 2.5.2/3.2.0) but must not be
// nil: nil marshals to JSON null, which 3.x rejects with an NPE.
func (proxy *NamingGrpcProxy) BatchRegisterInstance(serviceName string, groupName string, instances []model.Instance) (bool, error) {
	if instances == nil {
		instances = make([]model.Instance, 0)
	}
	logger.Infof("batch register instance namespaceId:<%s>,serviceName:<%s> with instance:<%s>",
		proxy.clientConfig.NamespaceId, serviceName, util.ToJsonString(instances))
	proxy.redoMu.Lock()
	proxy.eventListener.CacheInstancesForRedo(serviceName, groupName, instances)
	proxy.redoMu.Unlock()
	batchInstanceRequest := rpc_request.NewBatchInstanceRequest(proxy.clientConfig.NamespaceId, serviceName, groupName, "batchRegisterInstance", instances)
	response, err := proxy.send(batchInstanceRequest)
	if err != nil {
		return false, err
	}
	return response.IsSuccess(), nil
}

// DeregisterInstance mirrors Java's deregisterServiceForEphemeral: when the
// service was last published in batch shape, the instance is removed from the
// retained list and the remainder is re-published as a batch (a plain
// deregisterInstance can never shrink a batch publication - it is a server-
// side no-op against batch shape, probe-verified 2.5.2/3.2.0). Only a
// single-shape (or never-registered) service sends a plain deregister.
//
// redoMu is taken FIRST, before the shape check: this makes the check and the
// subsequent cache mutation+send atomic with respect to concurrent
// RegisterInstance/BatchRegisterInstance/DeregisterInstance calls, so there is
// no shape-flip window to retry around (unlike a check-then-lock design).
func (proxy *NamingGrpcProxy) DeregisterInstance(serviceName string, groupName string, instance model.Instance) (bool, error) {
	logger.Infof("deregister instance namespaceId:<%s>,serviceName:<%s> with instance:<%s:%d@%s>",
		proxy.clientConfig.NamespaceId, serviceName, instance.Ip, instance.Port, instance.ClusterName)
	proxy.redoMu.Lock()
	if batchInstances, isBatch := proxy.eventListener.GetBatchInstancesForRedo(serviceName, groupName); isBatch {
		defer proxy.redoMu.Unlock()
		// Remove the first ip:port match from the retained batch (Java parity:
		// getRetainInstance compares only Ip and Port and removes a single
		// match) - unchanged when the instance is unknown, empty when it was
		// the last member. The cache+send stays inline (rather than delegating
		// to BatchRegisterInstance) because that method takes redoMu itself,
		// and Go's sync.Mutex is not reentrant.
		retained := make([]model.Instance, 0, len(batchInstances))
		removed := false
		for _, inst := range batchInstances {
			if !removed && inst.Ip == instance.Ip && inst.Port == instance.Port {
				removed = true
				continue
			}
			retained = append(retained, inst)
		}
		// retained may legally be an empty (non-nil) slice after the last
		// member is deregistered: the batch redo entry lingers rather than
		// being deleted, mirroring Java - a later redo replay simply resends
		// this harmless empty batch, which keeps the publication cleared.
		proxy.eventListener.CacheInstancesForRedo(serviceName, groupName, retained)
		batchInstanceRequest := rpc_request.NewBatchInstanceRequest(proxy.clientConfig.NamespaceId, serviceName, groupName, "batchRegisterInstance", retained)
		response, err := proxy.send(batchInstanceRequest)
		if err != nil {
			return false, err
		}
		return response.IsSuccess(), nil
	}
	proxy.eventListener.RemoveInstanceForRedo(serviceName, groupName, instance)
	proxy.redoMu.Unlock()
	instanceRequest := rpc_request.NewInstanceRequest(proxy.clientConfig.NamespaceId, serviceName, groupName, "deregisterInstance", instance)
	response, err := proxy.send(instanceRequest)
	if err != nil {
		return false, err
	}
	return response.IsSuccess(), nil
}

// GetServiceList ...
func (proxy *NamingGrpcProxy) GetServiceList(pageNo uint32, pageSize uint32, groupName, namespaceId string, selector *model.ExpressionSelector) (model.ServiceList, error) {
	var selectorStr string
	if selector != nil {
		switch selector.Type {
		case "label":
			selectorStr = util.ToJsonString(selector)
		default:
			break
		}
	}
	response, err := proxy.send(rpc_request.NewServiceListRequest(namespaceId, "",
		groupName, int(pageNo), int(pageSize), selectorStr))
	if err != nil {
		return model.ServiceList{}, err
	}
	serviceListResponse := response.(*rpc_response.ServiceListResponse)
	return model.ServiceList{
		Count: int64(serviceListResponse.Count),
		Doms:  serviceListResponse.ServiceNames,
	}, nil
}

// ServerHealthy ...
func (proxy *NamingGrpcProxy) ServerHealthy() bool {
	return proxy.rpcClient.GetRpcClient().IsRunning()
}

// QueryInstancesOfService ...
func (proxy *NamingGrpcProxy) QueryInstancesOfService(serviceName, groupName, cluster string, udpPort int, healthyOnly bool) (*model.Service, error) {
	response, err := proxy.send(rpc_request.NewServiceQueryRequest(proxy.clientConfig.NamespaceId, serviceName, groupName, cluster,
		healthyOnly, udpPort))
	if err != nil {
		return nil, err
	}
	queryServiceResponse := response.(*rpc_response.QueryServiceResponse)
	return &queryServiceResponse.ServiceInfo, nil
}

func (proxy *NamingGrpcProxy) IsSubscribed(serviceName, groupName string, clusters string) bool {
	return proxy.eventListener.IsSubscriberCached(util.GetServiceCacheKey(util.GetGroupName(serviceName, groupName), clusters))
}

// Subscribe ...
func (proxy *NamingGrpcProxy) Subscribe(serviceName, groupName string, clusters string) (model.Service, error) {
	logger.Infof("Subscribe Service namespaceId:<%s>, serviceName:<%s>, groupName:<%s>, clusters:<%s>",
		proxy.clientConfig.NamespaceId, serviceName, groupName, clusters)
	proxy.eventListener.CacheSubscriberForRedo(util.GetGroupName(serviceName, groupName), clusters)
	request := rpc_request.NewSubscribeServiceRequest(proxy.clientConfig.NamespaceId, serviceName,
		groupName, clusters, true)
	request.Headers["app"] = proxy.clientConfig.AppName
	response, err := proxy.send(request)
	if err != nil {
		return model.Service{}, err
	}
	subscribeServiceResponse := response.(*rpc_response.SubscribeServiceResponse)
	return subscribeServiceResponse.ServiceInfo, nil
}

// Unsubscribe ...
func (proxy *NamingGrpcProxy) Unsubscribe(serviceName, groupName, clusters string) error {
	logger.Infof("Unsubscribe Service namespaceId:<%s>, serviceName:<%s>, groupName:<%s>, clusters:<%s>",
		proxy.clientConfig.NamespaceId, serviceName, groupName, clusters)
	proxy.eventListener.RemoveSubscriberForRedo(util.GetGroupName(serviceName, groupName), clusters)
	response, err := proxy.send(rpc_request.NewSubscribeServiceRequest(proxy.clientConfig.NamespaceId, serviceName, groupName,
		clusters, false))
	if err == nil && response == nil {
		err = errors.Errorf("unsubscribe %s got nil response", util.GetGroupName(serviceName, groupName))
	}
	if err == nil && !response.IsSuccess() {
		// The rpc client returns (response, nil) for a response the server
		// answered but did not accept; that is still a failed unsubscribe
		// and must not be silently treated as success.
		err = errors.Errorf("unsubscribe %s failed, resultCode:%d message:%s",
			util.GetGroupName(serviceName, groupName), response.GetResultCode(), response.GetMessage())
	}
	if err != nil {
		// the server-side unsubscribe did not take effect, so the server
		// keeps pushing updates for this subscription; restore the redo
		// cache entry so a reconnect keeps re-subscribing until a later
		// unsubscribe call actually succeeds.
		proxy.eventListener.CacheSubscriberForRedo(util.GetGroupName(serviceName, groupName), clusters)
	}
	return err
}

func (proxy *NamingGrpcProxy) CloseClient() {
	logger.Info("Close Nacos Go SDK Client...")
	proxy.rpcClient.GetRpcClient().Shutdown()
}
