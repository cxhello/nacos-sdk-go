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
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/nacos-group/nacos-sdk-go/v3/common/security"
	"github.com/nacos-group/nacos-sdk-go/v3/util"

	"github.com/nacos-group/nacos-sdk-go/v3/common/remote/rpc"
	"github.com/nacos-group/nacos-sdk-go/v3/common/remote/rpc/rpc_request"
	"github.com/nacos-group/nacos-sdk-go/v3/common/remote/rpc/rpc_response"
	"github.com/nacos-group/nacos-sdk-go/v3/model"

	"github.com/nacos-group/nacos-sdk-go/v3/clients/nacos_client"
	"github.com/nacos-group/nacos-sdk-go/v3/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v3/common/http_agent"
	"github.com/nacos-group/nacos-sdk-go/v3/vo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var serverConfigWithOptions = constant.NewServerConfig("127.0.0.1", 8848)

var clientConfigWithOptions = constant.NewClientConfig(
	constant.WithTimeoutMs(10*1000),
	constant.WithBeatInterval(2*1000),
	constant.WithNotLoadCacheAtStart(true),
	constant.WithAccessKey("LTAxxx"),
	constant.WithSecretKey("EdPxxx"),
	constant.WithOpenKMS(true),
	constant.WithKMSVersion(constant.KMSv1),
	constant.WithRegionId("cn-hangzhou"),
)

var clientTLsConfigWithOptions = constant.NewClientConfig(
	constant.WithTimeoutMs(10*1000),
	constant.WithBeatInterval(2*1000),
	constant.WithNotLoadCacheAtStart(true),

	/*constant.WithTLS(constant.TLSConfig{
		Enable:   true,
		TrustAll: false,
		CaFile:   "mse-nacos-ca.cer",
	}),*/
)

var localConfigTest = vo.ConfigParam{
	DataId:  "dataId",
	Group:   "group",
	Content: "content",
}

func createConfigClientTest() *ConfigClient {
	nc := nacos_client.NacosClient{}
	_ = nc.SetServerConfig([]constant.ServerConfig{*serverConfigWithOptions})
	_ = nc.SetClientConfig(*clientConfigWithOptions)
	_ = nc.SetHttpAgent(&http_agent.HttpAgent{})
	client, _ := NewConfigClient(&nc)
	client.configProxy = &MockConfigProxy{}
	return client
}

func createConfigClientTestTls() *ConfigClient {
	nc := nacos_client.NacosClient{}
	_ = nc.SetServerConfig([]constant.ServerConfig{*serverConfigWithOptions})
	_ = nc.SetClientConfig(*clientTLsConfigWithOptions)
	_ = nc.SetHttpAgent(&http_agent.HttpAgent{})
	client, _ := NewConfigClient(&nc)
	client.configProxy = &MockConfigProxy{}
	return client
}

func createConfigClientCommon() *ConfigClient {
	nc := nacos_client.NacosClient{}
	_ = nc.SetServerConfig([]constant.ServerConfig{*serverConfigWithOptions})
	_ = nc.SetClientConfig(*clientConfigWithOptions)
	_ = nc.SetHttpAgent(&http_agent.HttpAgent{})
	client, _ := NewConfigClient(&nc)
	client.configProxy = &MockConfigProxy{}
	return client
}

func createConfigClientForKms() *ConfigClient {
	nc := nacos_client.NacosClient{}
	_ = nc.SetServerConfig([]constant.ServerConfig{*serverConfigWithOptions})
	_ = nc.SetClientConfig(*clientConfigWithOptions)
	_ = nc.SetHttpAgent(&http_agent.HttpAgent{})
	client, _ := NewConfigClient(&nc)
	client.configProxy = &MockConfigProxyForUsingLocalDiskCache{}
	return client
}

type MockConfigProxyForUsingLocalDiskCache struct {
	MockConfigProxy
}

func (m *MockConfigProxyForUsingLocalDiskCache) queryConfig(dataId, group, tenant string, timeout uint64, notify bool, client *ConfigClient) (*rpc_response.ConfigQueryResponse, error) {
	return nil, errors.New("mock err for using localCache")
}

type MockConfigProxy struct {
}

func (m *MockConfigProxy) queryConfig(dataId, group, tenant string, timeout uint64, notify bool, client *ConfigClient) (*rpc_response.ConfigQueryResponse, error) {
	cacheKey := util.GetConfigCacheKey(dataId, group, tenant)
	if IsLimited(cacheKey) {
		return nil, errors.New("request is limited")
	}
	return &rpc_response.ConfigQueryResponse{Content: "hello world", Response: &rpc_response.Response{Success: true}}, nil
}
func (m *MockConfigProxy) searchConfigProxy(param vo.SearchConfigParam, tenant, accessKey, secretKey string) (*model.ConfigPage, error) {
	return &model.ConfigPage{TotalCount: 1}, nil
}
func (m *MockConfigProxy) requestProxy(rpcClient *rpc.RpcClient, request rpc_request.IRequest, timeoutMills uint64) (rpc_response.IResponse, error) {
	return &rpc_response.MockResponse{Response: &rpc_response.Response{Success: true}}, nil
}
func (m *MockConfigProxy) createRpcClient(ctx context.Context, taskId string, client *ConfigClient) *rpc.RpcClient {
	return &rpc.RpcClient{}
}
func (m *MockConfigProxy) getRpcClient(client *ConfigClient) *rpc.RpcClient {
	return &rpc.RpcClient{}
}

func Test_GetConfig(t *testing.T) {
	client := createConfigClientTest()
	success, err := client.PublishConfig(vo.ConfigParam{
		DataId:  localConfigTest.DataId,
		Group:   localConfigTest.Group,
		Content: "hello world"})

	assert.Nil(t, err)
	assert.True(t, success)

	content, err := client.GetConfig(vo.ConfigParam{
		DataId: localConfigTest.DataId,
		Group:  localConfigTest.Group})

	assert.Nil(t, err)
	assert.Equal(t, "hello world", content)
}

func Test_SearchConfig(t *testing.T) {
	client := createConfigClientTest()
	_, _ = client.PublishConfig(vo.ConfigParam{
		DataId:  localConfigTest.DataId,
		Group:   "DEFAULT_GROUP",
		Content: "hello world"})
	configPage, err := client.SearchConfig(vo.SearchConfigParam{
		Search:   "accurate",
		DataId:   localConfigTest.DataId,
		Group:    "DEFAULT_GROUP",
		PageNo:   1,
		PageSize: 10,
	})
	assert.Nil(t, err)
	assert.NotEmpty(t, configPage)
}

func Test_GetConfigTls(t *testing.T) {
	client := createConfigClientTestTls()
	_, _ = client.PublishConfig(vo.ConfigParam{
		DataId:  localConfigTest.DataId,
		Group:   "DEFAULT_GROUP",
		Content: "hello world"})
	configPage, err := client.SearchConfig(vo.SearchConfigParam{
		Search:   "accurate",
		DataId:   localConfigTest.DataId,
		Group:    "DEFAULT_GROUP",
		PageNo:   1,
		PageSize: 10,
	})
	assert.Nil(t, err)
	assert.NotEmpty(t, configPage)

}

// only using by ak sk for cipher config of aliyun kms
/*
func TestPublishAndGetConfigByUsingLocalCache(t *testing.T) {
	param := vo.ConfigParam{
		DataId:  "cipher-kms-aes-256-usingCache" + strconv.Itoa(rand.Int()),
		Group:   "DEFAULT",
		Content: "content加密&&" + strconv.Itoa(rand.Int()),
	}
	t.Run("PublishAndGetConfigByUsingLocalCache", func(t *testing.T) {
		commonClient := createConfigClientCommon()
		_, err := commonClient.PublishConfig(param)
		assert.Nil(t, err)

		time.Sleep(2 * time.Second)
		configQueryContent, err := commonClient.GetConfig(param)
		assert.Nil(t, err)
		assert.Equal(t, param.Content, configQueryContent)

		usingKmsCacheClient := createConfigClientForKms()
		configQueryContentByUsingCache, err := usingKmsCacheClient.GetConfig(param)
		assert.Nil(t, err)
		assert.Equal(t, param.Content, configQueryContentByUsingCache)

		newCipherContent := param.Content + "new"
		param.Content = newCipherContent
		err = commonClient.ListenConfig(vo.ConfigParam{
			DataId: param.DataId,
			Group:  param.Group,
			OnChange: func(namespace, group, dataId, data string) {
				t.Log("origin data: " + newCipherContent + "; new data: " + data)
				assert.Equal(t, newCipherContent, data)
			},
		})
		assert.Nil(t, err)

		result, err := commonClient.PublishConfig(param)
		assert.Nil(t, err)
		assert.True(t, result)

		time.Sleep(2 * time.Second)
		newContentCommon, err := commonClient.GetConfig(param)
		assert.Nil(t, err)
		assert.Equal(t, param.Content, newContentCommon)
		newContentKms, err := usingKmsCacheClient.GetConfig(param)
		assert.Nil(t, err)
		assert.Equal(t, param.Content, newContentKms)
	})
}
*/

// PublishConfig
func Test_PublishConfigWithoutDataId(t *testing.T) {
	client := createConfigClientTest()
	_, err := client.PublishConfig(vo.ConfigParam{
		DataId:  "",
		Group:   "group",
		Content: "content",
	})
	assert.NotNil(t, err)
}

func Test_PublishConfigWithoutContent(t *testing.T) {
	client := createConfigClientTest()
	_, err := client.PublishConfig(vo.ConfigParam{
		DataId:  localConfigTest.DataId,
		Group:   "group",
		Content: "",
	})
	assert.NotNil(t, err)
}

func Test_PublishConfig(t *testing.T) {

	client := createConfigClientTest()

	success, err := client.PublishConfig(vo.ConfigParam{
		DataId:  localConfigTest.DataId,
		Group:   "group",
		SrcUser: "nacos-client-go",
		Content: "hello world"})

	assert.Nil(t, err)
	assert.True(t, success)
}

// DeleteConfig
func Test_DeleteConfig(t *testing.T) {

	client := createConfigClientTest()

	success, err := client.PublishConfig(vo.ConfigParam{
		DataId:  localConfigTest.DataId,
		Group:   "group",
		Content: "hello world!"})

	assert.Nil(t, err)
	assert.True(t, success)

	success, err = client.DeleteConfig(vo.ConfigParam{
		DataId: localConfigTest.DataId,
		Group:  "group"})

	assert.Nil(t, err)
	assert.True(t, success)
}

func Test_DeleteConfigWithoutDataId(t *testing.T) {
	client := createConfigClientTest()
	success, err := client.DeleteConfig(vo.ConfigParam{
		DataId: "",
		Group:  "group",
	})
	assert.NotNil(t, err)
	assert.Equal(t, false, success)
}

func TestListen(t *testing.T) {
	t.Run("TestListenConfig", func(t *testing.T) {
		client := createConfigClientTest()
		err := client.ListenConfig(vo.ConfigParam{
			DataId: localConfigTest.DataId,
			Group:  localConfigTest.Group,
			OnChange: func(namespace, group, dataId, data string) {
			},
		})
		assert.Nil(t, err)
	})
	// ListenConfig no dataId
	t.Run("TestListenConfigNoDataId", func(t *testing.T) {
		listenConfigParam := vo.ConfigParam{
			Group: localConfigTest.Group,
			OnChange: func(namespace, group, dataId, data string) {
			},
		}
		client := createConfigClientTest()
		err := client.ListenConfig(listenConfigParam)
		assert.Error(t, err)
	})
}

// CancelListenConfig
func TestCancelListenConfig(t *testing.T) {
	//Multiple listeners listen for different configurations, cancel one
	t.Run("TestMultipleListenersCancelOne", func(t *testing.T) {
		client := createConfigClientTest()
		var err error
		listenConfigParam := vo.ConfigParam{
			DataId: localConfigTest.DataId,
			Group:  localConfigTest.Group,
			OnChange: func(namespace, group, dataId, data string) {
			},
		}

		listenConfigParam1 := vo.ConfigParam{
			DataId: localConfigTest.DataId + "1",
			Group:  localConfigTest.Group,
			OnChange: func(namespace, group, dataId, data string) {
			},
		}
		_ = client.ListenConfig(listenConfigParam)

		_ = client.ListenConfig(listenConfigParam1)

		err = client.CancelListenConfig(listenConfigParam)
		assert.Nil(t, err)
	})
}

// TestListenConfigAppendsSecondListener guards against the historical bug
// where a second ListenConfig call on the same key silently replaced (and
// therefore dropped) the first listener instead of appending to it.
func TestListenConfigAppendsSecondListener(t *testing.T) {
	client := createConfigClientTest()
	got := make(chan string, 2)
	for i := 0; i < 2; i++ {
		idx := strconv.Itoa(i)
		require.NoError(t, client.ListenConfig(vo.ConfigParam{DataId: "d", Group: "g",
			OnChange: func(ns, g, d, data string) { got <- idx + ":" + data }}))
	}
	clientConfig, err := client.GetClientConfig()
	require.NoError(t, err)
	cd, ok := client.holder.get(util.GetConfigCacheKey("d", "g", clientConfig.NamespaceId))
	require.True(t, ok)
	assert.Len(t, cd.listeners, 2)
}

// TestCancelListenConfigMarksDiscard verifies CancelListenConfig does not
// remove the cache entry outright (the executor/removeIfDiscarded reconcile
// it later); it must simply mark it discarded.
func TestCancelListenConfigMarksDiscard(t *testing.T) {
	client := createConfigClientTest()
	p := vo.ConfigParam{DataId: "d", Group: "g", OnChange: func(ns, g, d, data string) {}}
	require.NoError(t, client.ListenConfig(p))
	require.NoError(t, client.CancelListenConfig(p))

	clientConfig, err := client.GetClientConfig()
	require.NoError(t, err)
	key := util.GetConfigCacheKey("d", "g", clientConfig.NamespaceId)
	cd, ok := client.holder.get(key)
	require.True(t, ok, "entry must still be present after cancel")
	assert.True(t, cd.discard)
}

// TestCancelListenConfigOnUnknownKeyIsNoop verifies cancelling a key that was
// never listened on is a no-op that returns nil, per the brief.
func TestCancelListenConfigOnUnknownKeyIsNoop(t *testing.T) {
	client := createConfigClientTest()
	err := client.CancelListenConfig(vo.ConfigParam{DataId: "never-listened", Group: "g"})
	assert.NoError(t, err)
}

// TestCancelThenListenRevives verifies that a Cancel followed by a Listen on
// the same key revives the existing entry (discard flips back to false)
// rather than leaking a second entry, and that the revived entry survives a
// subsequent removeIfDiscarded sweep.
func TestCancelThenListenRevives(t *testing.T) {
	client := createConfigClientTest()
	p := vo.ConfigParam{DataId: "d", Group: "g", OnChange: func(ns, g, d, data string) {}}
	require.NoError(t, client.ListenConfig(p))
	require.NoError(t, client.CancelListenConfig(p))
	clientConfig, err := client.GetClientConfig()
	require.NoError(t, err)
	key := util.GetConfigCacheKey("d", "g", clientConfig.NamespaceId)
	cd, _ := client.holder.get(key)
	assert.True(t, cd.discard)
	require.NoError(t, client.ListenConfig(p))
	assert.False(t, cd.discard)
	client.holder.removeIfDiscarded(key)
	_, ok := client.holder.get(key)
	assert.True(t, ok, "revived entry must survive removeIfDiscarded")
}

// TestListenCancelConcurrentNeverLeavesDiscardedWithListeners is a
// regression test for a TOCTOU between getOrCreate's internal revive
// (discard=false) and a subsequently, separately-locked addListener call in
// ListenConfig: a concurrent CancelListenConfig (markDiscard) could
// interleave between the two and leave the entry with discard==true and a
// non-empty listeners slice. That combination must never be observable,
// since Task 4's reap wiring treats discard==true as "safe to remove once
// listeners is empty" and would otherwise build on an inconsistent
// intermediate state. An observer goroutine samples the entry continuously
// while two producer goroutines hammer ListenConfig/CancelListenConfig
// concurrently, so a transient (not just a final) violation would be
// caught.
func TestListenCancelConcurrentNeverLeavesDiscardedWithListeners(t *testing.T) {
	client := createConfigClientTest()
	p := vo.ConfigParam{DataId: "race-d", Group: "race-g", OnChange: func(ns, g, d, data string) {}}

	clientConfig, err := client.GetClientConfig()
	require.NoError(t, err)
	key := util.GetConfigCacheKey(p.DataId, p.Group, clientConfig.NamespaceId)

	const iterations = 500

	var producers sync.WaitGroup
	producers.Add(2)
	go func() {
		defer producers.Done()
		for i := 0; i < iterations; i++ {
			_ = client.ListenConfig(p)
		}
	}()
	go func() {
		defer producers.Done()
		for i := 0; i < iterations; i++ {
			_ = client.CancelListenConfig(p)
		}
	}()

	stop := make(chan struct{})
	var violation atomic.Bool
	var observer sync.WaitGroup
	observer.Add(1)
	go func() {
		defer observer.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if cData, ok := client.holder.get(key); ok {
				cData.mu.Lock()
				if cData.discard && len(cData.listeners) > 0 {
					violation.Store(true)
				}
				cData.mu.Unlock()
			}
		}
	}()

	producers.Wait()
	close(stop)
	observer.Wait()

	assert.False(t, violation.Load(), "entry must never be observed with discard==true and non-empty listeners")

	// Also assert the final resting state is consistent.
	if cData, ok := client.holder.get(key); ok {
		cData.mu.Lock()
		finalInconsistent := cData.discard && len(cData.listeners) > 0
		cData.mu.Unlock()
		assert.False(t, finalInconsistent, "final state must not be discard==true with non-empty listeners")
	}
}

type MockAccessKeyCredentialProvider struct {
	accessKey         string
	secretKey         string
	signatureRegionId string
}

func (provider *MockAccessKeyCredentialProvider) MatchProvider() bool {
	return true
}

func (provider *MockAccessKeyCredentialProvider) Init() error {
	return nil
}

func (provider *MockAccessKeyCredentialProvider) GetCredentialsForNacosClient() security.RamContext {
	ramContext := security.RamContext{
		AccessKey:         provider.accessKey,
		SecretKey:         provider.secretKey,
		SignatureRegionId: "",
	}
	return ramContext
}

func Test_ConfigClientWithProvider(t *testing.T) {
	nc := nacos_client.NacosClient{}
	_ = nc.SetServerConfig([]constant.ServerConfig{*serverConfigWithOptions})
	clientConfigWithOptions.AccessKey = ""
	clientConfigWithOptions.SecretKey = ""
	_ = nc.SetClientConfig(*clientConfigWithOptions)
	_ = nc.SetHttpAgent(&http_agent.HttpAgent{})
	provider := &MockAccessKeyCredentialProvider{
		accessKey: "LTAxxx",
		secretKey: "EdPxxx",
	}
	client, _ := NewConfigClientWithRamCredentialProvider(&nc, provider)
	client.configProxy = &MockConfigProxy{}
	success, err := client.PublishConfig(vo.ConfigParam{
		DataId:  localConfigTest.DataId,
		Group:   localConfigTest.Group,
		Content: "hello world"})

	assert.Nil(t, err)
	assert.True(t, success)

	content, err := client.GetConfig(vo.ConfigParam{
		DataId: localConfigTest.DataId,
		Group:  localConfigTest.Group})

	assert.Nil(t, err)
	assert.Equal(t, "hello world", content)
}
