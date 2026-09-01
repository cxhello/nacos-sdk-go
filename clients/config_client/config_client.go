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

package config_client

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/nacos-group/nacos-sdk-go/v3/common/security"

	"github.com/nacos-group/nacos-sdk-go/v3/clients/cache"
	"github.com/nacos-group/nacos-sdk-go/v3/clients/nacos_client"
	"github.com/nacos-group/nacos-sdk-go/v3/common/constant"
	nacos_inner_encryption "github.com/nacos-group/nacos-sdk-go/v3/common/encryption"
	"github.com/nacos-group/nacos-sdk-go/v3/common/filter"
	"github.com/nacos-group/nacos-sdk-go/v3/common/logger"
	"github.com/nacos-group/nacos-sdk-go/v3/common/monitor"
	"github.com/nacos-group/nacos-sdk-go/v3/common/nacos_error"
	"github.com/nacos-group/nacos-sdk-go/v3/common/remote/rpc/rpc_request"
	"github.com/nacos-group/nacos-sdk-go/v3/common/remote/rpc/rpc_response"
	"github.com/nacos-group/nacos-sdk-go/v3/inner/uuid"
	"github.com/nacos-group/nacos-sdk-go/v3/model"
	"github.com/nacos-group/nacos-sdk-go/v3/util"
	"github.com/nacos-group/nacos-sdk-go/v3/vo"
	"github.com/pkg/errors"
)

const (
	perTaskConfigSize = 3000
	executorErrDelay  = 5 * time.Second
)

type ConfigClient struct {
	ctx    context.Context
	cancel context.CancelFunc
	nacos_client.INacosClient
	configFilterChainManager filter.IConfigFilterChain
	mutex                    sync.Mutex
	configProxy              IConfigProxy
	configCacheDir           string
	lastAllSyncTime          time.Time
	holder                   *configCacheHolder
	uid                      string
	listenExecute            chan struct{}
	isClosed                 bool
}

// notifyListenersIfChanged notifies every listener registered on cData whose
// own md5 watermark differs from cData's current md5, and advances that
// listener's watermark to the current md5. This is an interim, whole-entry
// notify path kept behaviorally equivalent to the previous single-listener
// executeListener(): Task 5 will replace it with real per-listener delivery
// semantics.
func (client *ConfigClient) notifyListenersIfChanged(cData *cacheData) {
	cData.mu.Lock()
	dataId, group, tenant := cData.dataId, cData.group, cData.tenant
	content := cData.content
	encryptedDataKey := cData.encryptedDataKey
	md5 := cData.md5
	toNotify := make([]*listenerWrap, 0, len(cData.listeners))
	for _, lw := range cData.listeners {
		if lw.lastCallMd5 != md5 {
			lw.lastCallMd5 = md5
			toNotify = append(toNotify, lw)
		}
	}
	cData.mu.Unlock()

	if len(toNotify) == 0 {
		return
	}

	param := &vo.ConfigParam{
		DataId:           dataId,
		Content:          content,
		EncryptedDataKey: encryptedDataKey,
		UsageType:        vo.ResponseType,
	}
	if err := client.configFilterChainManager.DoFilters(param); err != nil {
		logger.Errorf("do filters failed ,dataId=%s,group=%s,tenant=%s,err:%+v ", dataId, group, tenant, err)
		return
	}
	decryptedContent := param.Content
	for _, lw := range toNotify {
		listener := lw.listener
		go listener(tenant, group, dataId, decryptedContent)
	}
}

func NewConfigClientWithRamCredentialProvider(nc nacos_client.INacosClient, provider security.RamCredentialProvider) (*ConfigClient, error) {
	config := &ConfigClient{}
	config.ctx, config.cancel = context.WithCancel(context.Background())
	config.INacosClient = nc
	clientConfig, err := nc.GetClientConfig()
	if err != nil {
		config.cancel()
		return nil, err
	}
	serverConfig, err := nc.GetServerConfig()
	if err != nil {
		config.cancel()
		return nil, err
	}
	httpAgent, err := nc.GetHttpAgent()
	if err != nil {
		config.cancel()
		return nil, err
	}

	if err = initLogger(clientConfig); err != nil {
		config.cancel()
		return nil, err
	}
	clientConfig.CacheDir = clientConfig.CacheDir + string(os.PathSeparator) + "config"
	config.configCacheDir = clientConfig.CacheDir

	if config.configProxy, err = NewConfigProxyWithRamCredentialProvider(config.ctx, serverConfig, clientConfig, httpAgent, provider); err != nil {
		config.cancel()
		return nil, err
	}

	config.configFilterChainManager = filter.NewConfigFilterChainManager()

	if clientConfig.OpenKMS {
		kmsEncryptionHandler := nacos_inner_encryption.NewKmsHandler()
		nacos_inner_encryption.RegisterConfigEncryptionKmsPlugins(kmsEncryptionHandler, clientConfig)
		encryptionFilter := filter.NewDefaultConfigEncryptionFilter(kmsEncryptionHandler)
		err := filter.RegisterConfigFilterToChain(config.configFilterChainManager, encryptionFilter)
		if err != nil {
			logger.Error(err)
		}
	}

	uid, err := uuid.NewV4()
	if err != nil {
		config.cancel()
		return nil, err
	}

	config.uid = uid.String()
	config.holder = newConfigCacheHolder()
	config.listenExecute = make(chan struct{})
	config.startInternal()
	return config, err
}

func NewConfigClient(nc nacos_client.INacosClient) (*ConfigClient, error) {
	return NewConfigClientWithRamCredentialProvider(nc, nil)
}

func initLogger(clientConfig constant.ClientConfig) error {
	return logger.InitLogger(logger.BuildLoggerConfig(clientConfig))
}

func (client *ConfigClient) GetConfig(param vo.ConfigParam) (content string, err error) {
	content, encryptedDataKey, err := client.getConfigInner(param)
	if err != nil {
		return "", err
	}
	deepCopyParam := param.DeepCopy()
	deepCopyParam.EncryptedDataKey = encryptedDataKey
	deepCopyParam.Content = content
	deepCopyParam.UsageType = vo.ResponseType
	if err = client.configFilterChainManager.DoFilters(deepCopyParam); err != nil {
		return "", err
	}
	content = deepCopyParam.Content
	return content, nil
}

func (client *ConfigClient) getConfigInner(param vo.ConfigParam) (content, encryptedDataKey string, err error) {
	if len(param.DataId) <= 0 {
		err = errors.New("[client.GetConfig] param.dataId can not be empty")
		return "", "", err
	}
	if len(param.Group) <= 0 {
		param.Group = constant.DEFAULT_GROUP
	}

	clientConfig, _ := client.GetClientConfig()
	cacheKey := util.GetConfigCacheKey(param.DataId, param.Group, clientConfig.NamespaceId)
	content = cache.GetFailover(cacheKey, client.configCacheDir)
	if len(content) > 0 {
		logger.Warnf("%s %s %s is using failover content!", clientConfig.NamespaceId, param.Group, param.DataId)
		encryptedDataKey = cache.GetFailoverEncryptedDataKey(cacheKey, client.configCacheDir)
		return content, encryptedDataKey, nil
	}
	response, err := client.configProxy.queryConfig(param.DataId, param.Group, clientConfig.NamespaceId,
		clientConfig.TimeoutMs, false, client)
	if err != nil {
		logger.Errorf("get config from server error:%v, dataId=%s, group=%s, namespaceId=%s", err,
			param.DataId, param.Group, clientConfig.NamespaceId)

		if clientConfig.DisableUseSnapShot {
			return "", "", errors.Errorf("get config from remote nacos server fail, and is not allowed to read local file, err:%v", err)
		}

		cacheContent, cacheErr := cache.ReadConfigFromFile(cacheKey, client.configCacheDir)
		if cacheErr != nil {
			return "", "", errors.Errorf("read config from both server and cache fail, err=%v，dataId=%s, group=%s, namespaceId=%s",
				cacheErr, param.DataId, param.Group, clientConfig.NamespaceId)
		}

		if !strings.HasPrefix(param.DataId, nacos_inner_encryption.CipherPrefix) {
			return cacheContent, "", nil
		}
		encryptedDataKey, cacheErr = cache.ReadEncryptedDataKeyFromFile(cacheKey, client.configCacheDir)
		if cacheErr != nil {
			return "", "", errors.Errorf("read encryptedDataKey from server and cache fail, err=%v，dataId=%s, group=%s, namespaceId=%s",
				cacheErr, param.DataId, param.Group, clientConfig.NamespaceId)
		}

		logger.Warnf("read config from cache success, dataId=%s, group=%s, namespaceId=%s", param.DataId, param.Group, clientConfig.NamespaceId)
		return cacheContent, encryptedDataKey, nil
	}
	if response != nil && response.Response != nil && !response.IsSuccess() {
		return response.Content, response.EncryptedDataKey, errors.New(response.GetMessage())
	}
	encryptedDataKey = response.EncryptedDataKey
	content = response.Content
	return content, encryptedDataKey, nil
}

func (client *ConfigClient) PublishConfig(param vo.ConfigParam) (published bool, err error) {
	if len(param.DataId) <= 0 {
		err = errors.New("[client.PublishConfig] param.dataId can not be empty")
		return
	}
	if len(param.Content) <= 0 {
		err = errors.New("[client.PublishConfig] param.content can not be empty")
		return
	}

	if len(param.Group) <= 0 {
		param.Group = constant.DEFAULT_GROUP
	}

	param.UsageType = vo.RequestType
	if err = client.configFilterChainManager.DoFilters(&param); err != nil {
		return false, err
	}

	clientConfig, _ := client.GetClientConfig()
	request := rpc_request.NewConfigPublishRequest(param.Group, param.DataId, clientConfig.NamespaceId, param.Content, param.CasMd5)
	request.AdditionMap["tag"] = param.Tag
	request.AdditionMap["config_tags"] = param.ConfigTags
	request.AdditionMap["appName"] = param.AppName
	request.AdditionMap["betaIps"] = param.BetaIps
	request.AdditionMap["type"] = param.Type
	request.AdditionMap["src_user"] = param.SrcUser
	request.AdditionMap["encryptedDataKey"] = param.EncryptedDataKey
	rpcClient := client.configProxy.getRpcClient(client)
	response, err := client.configProxy.requestProxy(rpcClient, request, constant.DEFAULT_TIMEOUT_MILLS)
	if err != nil {
		return false, err
	}
	if response != nil {
		return client.buildResponse(response)
	}
	return false, err
}

func (client *ConfigClient) DeleteConfig(param vo.ConfigParam) (deleted bool, err error) {
	if len(param.DataId) <= 0 {
		err = errors.New("[client.DeleteConfig] param.dataId can not be empty")
	}
	if len(param.Group) <= 0 {
		param.Group = constant.DEFAULT_GROUP
	}
	if err != nil {
		return false, err
	}
	clientConfig, _ := client.GetClientConfig()
	request := rpc_request.NewConfigRemoveRequest(param.Group, param.DataId, clientConfig.NamespaceId)
	rpcClient := client.configProxy.getRpcClient(client)
	response, err := client.configProxy.requestProxy(rpcClient, request, constant.DEFAULT_TIMEOUT_MILLS)
	if err != nil {
		return false, err
	}
	if response != nil {
		return client.buildResponse(response)
	}
	return false, err
}

// CancelListenConfig cancels a previously registered listen for the given
// key. If the key was never listened on, this is a no-op returning nil. The
// entry (if any) is marked discarded rather than removed outright; reconciling
// removal from the holder is left to removeIfDiscarded/the executor.
func (client *ConfigClient) CancelListenConfig(param vo.ConfigParam) (err error) {
	clientConfig, err := client.GetClientConfig()
	if err != nil {
		logger.Errorf("[checkConfigInfo.GetClientConfig] failed,err:%+v", err)
		return
	}
	key := util.GetConfigCacheKey(param.DataId, param.Group, clientConfig.NamespaceId)
	if cData, ok := client.holder.get(key); ok {
		cData.markDiscard()
		client.asyncNotifyListenConfig()
	}
	logger.Infof("Cancel listen config DataId:%s Group:%s", param.DataId, param.Group)
	return nil
}

// ListenConfig registers OnChange to be notified about changes to the
// dataId/group/namespace identified by param. Calling it repeatedly for the
// same key appends additional independent listeners rather than replacing
// the previous one; calling it again after CancelListenConfig revives the
// entry.
func (client *ConfigClient) ListenConfig(param vo.ConfigParam) (err error) {
	if len(param.DataId) <= 0 {
		err = errors.New("[client.ListenConfig] DataId can not be empty")
		return err
	}
	if len(param.Group) <= 0 {
		err = errors.New("[client.ListenConfig] Group can not be empty")
		return err
	}
	clientConfig, err := client.GetClientConfig()
	if err != nil {
		err = errors.New("[checkConfigInfo.GetClientConfig] failed")
		return err
	}

	key := util.GetConfigCacheKey(param.DataId, param.Group, clientConfig.NamespaceId)
	// Computed ahead of getOrCreate: getOrCreate already holds the holder's
	// write lock while invoking seed, so calling back into holder.count()
	// (which itself locks) from inside seed would deadlock.
	taskId := client.holder.count() / perTaskConfigSize

	cData := client.holder.getOrCreate(key, func() *cacheData {
		content, innerErr := cache.ReadConfigFromFile(key, client.configCacheDir)
		if innerErr != nil {
			logger.Warn(innerErr)
		}
		encryptedDataKey, _ := cache.ReadEncryptedDataKeyFromFile(key, client.configCacheDir)
		var md5Str string
		if len(content) > 0 {
			md5Str = util.Md5(content)
		}
		return &cacheData{
			isInitializing:   true,
			dataId:           param.DataId,
			group:            param.Group,
			tenant:           clientConfig.NamespaceId,
			content:          content,
			md5:              md5Str,
			encryptedDataKey: encryptedDataKey,
			taskId:           taskId,
		}
	})

	// Revive (discard=false) and append must happen in one critical section:
	// see reviveAndAddListener's doc comment for why a separate lock/unlock
	// around isInitializing/md5 followed by a separately-locked addListener
	// call is unsafe against a concurrent CancelListenConfig.
	cData.reviveAndAddListener(param.OnChange)
	return nil
}

func (client *ConfigClient) SearchConfig(param vo.SearchConfigParam) (*model.ConfigPage, error) {
	return client.searchConfigInner(param)
}

func (client *ConfigClient) CloseClient() {
	client.mutex.Lock()
	defer client.mutex.Unlock()

	if client.isClosed {
		return
	}
	client.configProxy.getRpcClient(client).Shutdown()
	client.cancel()
	client.isClosed = true
}

func (client *ConfigClient) searchConfigInner(param vo.SearchConfigParam) (*model.ConfigPage, error) {
	if param.Search != "accurate" && param.Search != "blur" {
		return nil, errors.New("[client.searchConfigInner] param.search must be accurate or blur")
	}
	if param.PageNo <= 0 {
		param.PageNo = 1
	}
	if param.PageSize <= 0 {
		param.PageSize = 10
	}
	clientConfig, _ := client.GetClientConfig()
	configItems, err := client.configProxy.searchConfigProxy(param, clientConfig.NamespaceId, clientConfig.AccessKey, clientConfig.SecretKey)
	if err != nil {
		logger.Errorf("search config from server error:%+v ", err)
		if _, ok := err.(*nacos_error.NacosError); ok {
			nacosErr := err.(*nacos_error.NacosError)
			if nacosErr.ErrorCode() == "404" {
				return nil, errors.New("config not found")
			}
			if nacosErr.ErrorCode() == "403" {
				return nil, errors.New("get config forbidden")
			}
		}
		return nil, err
	}
	return configItems, nil
}

func (client *ConfigClient) startInternal() {
	go func() {
		timer := time.NewTimer(executorErrDelay)
		defer timer.Stop()
		for {
			select {
			case <-client.listenExecute:
				client.executeConfigListen()
			case <-timer.C:
				client.executeConfigListen()
			case <-client.ctx.Done():
				return
			}
			timer.Reset(executorErrDelay)
		}
	}()
}

func (client *ConfigClient) executeConfigListen() {
	var (
		needAllSync    = time.Since(client.lastAllSyncTime) >= constant.ALL_SYNC_INTERNAL
		hasChangedKeys = false
	)

	listenTaskMap := client.buildListenTask(needAllSync)
	if len(listenTaskMap) == 0 {
		return
	}

	for taskId, caches := range listenTaskMap {
		request := buildConfigBatchListenRequest(caches)
		rpcClient := client.configProxy.createRpcClient(client.ctx, fmt.Sprintf("%d", taskId), client)
		iResponse, err := client.configProxy.requestProxy(rpcClient, request, 3000)
		if err != nil {
			logger.Warnf("ConfigBatchListenRequest failure, err:%v", err)
			continue
		}
		if iResponse == nil {
			logger.Warnf("ConfigBatchListenRequest failure, response is nil")
			continue
		}
		if !iResponse.IsSuccess() {
			logger.Warnf("ConfigBatchListenRequest failure, error code:%d", iResponse.GetErrorCode())
			continue
		}
		response, ok := iResponse.(*rpc_response.ConfigChangeBatchListenResponse)
		if !ok {
			continue
		}

		if len(response.ChangedConfigs) > 0 {
			hasChangedKeys = true
		}
		changeKeys := make(map[string]struct{}, len(response.ChangedConfigs))
		for _, v := range response.ChangedConfigs {
			changeKey := util.GetConfigCacheKey(v.DataId, v.Group, v.Tenant)
			changeKeys[changeKey] = struct{}{}
			if cData, ok := client.holder.get(changeKey); ok {
				cData.mu.Lock()
				isInitializing := cData.isInitializing
				cData.mu.Unlock()
				client.refreshContentAndCheck(cData, !isInitializing)
			}
		}

		for _, cData := range client.holder.snapshot() {
			cData.mu.Lock()
			changeKey := util.GetConfigCacheKey(cData.dataId, cData.group, cData.tenant)
			if _, ok := changeKeys[changeKey]; !ok {
				cData.isSyncWithServer = true
			} else {
				cData.isInitializing = true
			}
			cData.mu.Unlock()
		}

	}
	if needAllSync {
		client.lastAllSyncTime = time.Now()
	}

	if hasChangedKeys {
		client.asyncNotifyListenConfig()
	}
	monitor.GetListenConfigCountMonitor().Set(float64(client.holder.count()))
}

func buildConfigBatchListenRequest(caches []*cacheData) *rpc_request.ConfigBatchListenRequest {
	request := rpc_request.NewConfigBatchListenRequest(len(caches))
	for _, cData := range caches {
		cData.mu.Lock()
		ctx := model.ConfigListenContext{Group: cData.group, Md5: cData.md5, DataId: cData.dataId, Tenant: cData.tenant}
		cData.mu.Unlock()
		request.ConfigListenContexts = append(request.ConfigListenContexts, ctx)
	}
	return request
}

func (client *ConfigClient) refreshContentAndCheck(cData *cacheData, notify bool) {
	cData.mu.Lock()
	dataId, group, tenant := cData.dataId, cData.group, cData.tenant
	cData.mu.Unlock()

	configQueryResponse, err := client.configProxy.queryConfig(dataId, group, tenant,
		constant.DEFAULT_TIMEOUT_MILLS, notify, client)
	if err != nil {
		logger.Errorf("refresh content and check md5 fail ,dataId=%s,group=%s,tenant=%s ", dataId, group, tenant)
		return
	}
	if configQueryResponse != nil && configQueryResponse.Response != nil && !configQueryResponse.IsSuccess() {
		logger.Errorf("refresh cached config from server error:%v, dataId=%s, group=%s", configQueryResponse.GetMessage(),
			dataId, group)
		return
	}

	cData.mu.Lock()
	cData.content = configQueryResponse.Content
	cData.contentType = configQueryResponse.ContentType
	cData.encryptedDataKey = configQueryResponse.EncryptedDataKey
	if notify {
		logger.Infof("[config_rpc_client] [data-received] dataId=%s, group=%s, tenant=%s, md5=%s, content=%s, type=%s",
			dataId, group, tenant, cData.md5, util.TruncateContent(cData.content), cData.contentType)
	}
	cData.md5 = util.Md5(cData.content)
	cData.mu.Unlock()

	client.notifyListenersIfChanged(cData)
}

func (client *ConfigClient) buildListenTask(needAllSync bool) map[int][]*cacheData {
	listenTaskMap := make(map[int][]*cacheData, 8)

	for _, cData := range client.holder.snapshot() {
		cData.mu.Lock()
		isSyncWithServer := cData.isSyncWithServer
		taskId := cData.taskId
		cData.mu.Unlock()

		if isSyncWithServer {
			client.notifyListenersIfChanged(cData)
			if !needAllSync {
				continue
			}
		}
		listenTaskMap[taskId] = append(listenTaskMap[taskId], cData)
	}
	return listenTaskMap
}

func (client *ConfigClient) asyncNotifyListenConfig() {
	go func() {
		client.listenExecute <- struct{}{}
	}()
}

func (client *ConfigClient) buildResponse(response rpc_response.IResponse) (bool, error) {
	if response.IsSuccess() {
		return response.IsSuccess(), nil
	}
	return false, errors.New(response.GetMessage())
}
