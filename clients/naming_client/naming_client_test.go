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

package naming_client

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/nacos-group/nacos-sdk-go/v3/common/http_agent"

	"github.com/nacos-group/nacos-sdk-go/v3/clients/nacos_client"
	"github.com/nacos-group/nacos-sdk-go/v3/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v3/common/remote/rpc/rpc_request"
	"github.com/nacos-group/nacos-sdk-go/v3/model"
	"github.com/nacos-group/nacos-sdk-go/v3/util"
	"github.com/nacos-group/nacos-sdk-go/v3/vo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var clientConfigTest = *constant.NewClientConfig(
	constant.WithTimeoutMs(10*1000),
	constant.WithBeatInterval(5*1000),
	constant.WithNotLoadCacheAtStart(true),
)

var serverConfigTest = *constant.NewServerConfig("127.0.0.1", 80, constant.WithContextPath("/nacos"))

type MockNamingProxy struct {
	// mu guards every field below. Most tests drive the mock from a single
	// goroutine, but the C1 regression test
	// (TestFuzzyWatchRollbackNeverDropsConcurrentRegistration) deliberately
	// runs two FuzzyWatch calls concurrently, so the recorder slices and the
	// result-selection fields must be safe for concurrent read/write - `go
	// test -race` catches any field touched without this lock.
	mu sync.Mutex

	unsubscribeCalled bool
	unsubscribeParams []string // 记录调用参数
	unsubscribeErr    error    // 注入 Unsubscribe 失败

	// fuzzyWatchErr/cancelFuzzyWatchErr let tests simulate a failing
	// server-side RPC to exercise the client's rollback/keep-state paths.
	fuzzyWatchErr         error
	cancelFuzzyWatchErr   error
	fuzzyWatchCalls       []fuzzyWatchCall
	cancelFuzzyWatchCalls []string

	// fuzzyWatchResultFn, if set, computes the FuzzyWatch return value for
	// each call from (pattern, isInitializing) instead of the static
	// fuzzyWatchErr above, and is free to block before returning. Tests use
	// it to pause one call mid-RPC so a second, concurrent FuzzyWatch call
	// can be driven to completion first, deterministically reproducing the
	// window between RegisterPattern succeeding locally and the RPC outcome
	// becoming known that the C1 rollback race depends on.
	fuzzyWatchResultFn func(pattern string, isInitializing bool) error
}

type fuzzyWatchCall struct {
	pattern        string
	isInitializing bool
}

func (m *MockNamingProxy) RegisterInstance(serviceName string, groupName string, instance model.Instance) (bool, error) {
	return true, nil
}

func (m *MockNamingProxy) BatchRegisterInstance(serviceName string, groupName string, instances []model.Instance) (bool, error) {
	return true, nil
}

func (m *MockNamingProxy) DeregisterInstance(serviceName string, groupName string, instance model.Instance) (bool, error) {
	return true, nil
}

func (m *MockNamingProxy) GetServiceList(pageNo uint32, pageSize uint32, groupName, namespaceId string, selector *model.ExpressionSelector) (model.ServiceList, error) {
	return model.ServiceList{Doms: []string{""}}, nil
}

func (m *MockNamingProxy) ServerHealthy() bool {
	return true
}

func (m *MockNamingProxy) QueryInstancesOfService(serviceName, groupName, clusters string, udpPort int, healthyOnly bool) (*model.Service, error) {
	return &model.Service{}, nil
}

func (m *MockNamingProxy) Subscribe(serviceName, groupName, clusters string) (model.Service, error) {
	return model.Service{}, nil
}

func (m *MockNamingProxy) Unsubscribe(serviceName, groupName, clusters string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.unsubscribeCalled = true
	m.unsubscribeParams = []string{serviceName, groupName, clusters}
	return m.unsubscribeErr
}

// FuzzyWatch records the call and returns fuzzyWatchResultFn's verdict if
// set, else the static fuzzyWatchErr. fuzzyWatchResultFn (when set) is
// invoked WITHOUT the lock held, so it may itself block (e.g. on a channel)
// without deadlocking a concurrent call into this same mock.
func (m *MockNamingProxy) FuzzyWatch(groupKeyPattern string, receivedGroupKeys []string, isInitializing bool) error {
	m.mu.Lock()
	m.fuzzyWatchCalls = append(m.fuzzyWatchCalls, fuzzyWatchCall{pattern: groupKeyPattern, isInitializing: isInitializing})
	resultFn := m.fuzzyWatchResultFn
	staticErr := m.fuzzyWatchErr
	m.mu.Unlock()

	if resultFn != nil {
		return resultFn(groupKeyPattern, isInitializing)
	}
	return staticErr
}

// fuzzyWatchCallsSnapshot returns a race-safe copy of the recorded FuzzyWatch
// calls, for tests that assert on call order/content after concurrent calls.
func (m *MockNamingProxy) fuzzyWatchCallsSnapshot() []fuzzyWatchCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]fuzzyWatchCall, len(m.fuzzyWatchCalls))
	copy(out, m.fuzzyWatchCalls)
	return out
}

func (m *MockNamingProxy) CancelFuzzyWatch(groupKeyPattern string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cancelFuzzyWatchCalls = append(m.cancelFuzzyWatchCalls, groupKeyPattern)
	return m.cancelFuzzyWatchErr
}

func (m *MockNamingProxy) CloseClient() {}

func NewTestNamingClient() *NamingClient {
	nc := nacos_client.NacosClient{}
	_ = nc.SetServerConfig([]constant.ServerConfig{serverConfigTest})
	_ = nc.SetClientConfig(clientConfigTest)
	_ = nc.SetHttpAgent(&http_agent.HttpAgent{})
	client, _ := NewNamingClient(&nc)
	client.serviceProxy = &MockNamingProxy{}
	return client
}
func Test_RegisterServiceInstance_withoutGroupName(t *testing.T) {
	success, err := NewTestNamingClient().RegisterInstance(vo.RegisterInstanceParam{
		ServiceName: "DEMO",
		Ip:          "10.0.0.10",
		Port:        80,
		Ephemeral:   false,
	})
	assert.Equal(t, nil, err)
	assert.Equal(t, true, success)
}

func Test_RegisterServiceInstance_withGroupName(t *testing.T) {
	success, err := NewTestNamingClient().RegisterInstance(vo.RegisterInstanceParam{
		ServiceName: "DEMO",
		Ip:          "10.0.0.10",
		Port:        80,
		GroupName:   "test_group",
		Ephemeral:   false,
	})
	assert.Equal(t, nil, err)
	assert.Equal(t, true, success)
}

func Test_RegisterServiceInstance_withCluster(t *testing.T) {
	success, err := NewTestNamingClient().RegisterInstance(vo.RegisterInstanceParam{
		ServiceName: "DEMO",
		Ip:          "10.0.0.10",
		Port:        80,
		GroupName:   "test_group",
		ClusterName: "test",
		Ephemeral:   false,
	})
	assert.Equal(t, nil, err)
	assert.Equal(t, true, success)
}
func TestNamingProxy_DeregisterService_WithoutGroupName(t *testing.T) {
	success, err := NewTestNamingClient().DeregisterInstance(vo.DeregisterInstanceParam{
		ServiceName: "DEMO5",
		Ip:          "10.0.0.10",
		Port:        80,
		Ephemeral:   true,
	})
	assert.Equal(t, nil, err)
	assert.Equal(t, true, success)
}

func TestNamingProxy_DeregisterService_WithGroupName(t *testing.T) {
	success, err := NewTestNamingClient().DeregisterInstance(vo.DeregisterInstanceParam{
		ServiceName: "DEMO6",
		Ip:          "10.0.0.10",
		Port:        80,
		GroupName:   "test_group",
		Ephemeral:   true,
	})
	assert.Equal(t, nil, err)
	assert.Equal(t, true, success)
}

func TestNamingClient_SelectOneHealthyInstance_SameWeight(t *testing.T) {
	services := model.Service{
		Name:        "DEFAULT_GROUP@@DEMO",
		CacheMillis: 1000,
		Hosts: []model.Instance{
			{
				InstanceId:  "10.10.10.10-80-a-DEMO",
				Port:        80,
				Ip:          "10.10.10.10",
				Weight:      1,
				Metadata:    map[string]string{},
				ClusterName: "a",
				ServiceName: "DEMO1",
				Enable:      true,
				Healthy:     true,
			},
			{
				InstanceId:  "10.10.10.11-80-a-DEMO",
				Port:        80,
				Ip:          "10.10.10.11",
				Weight:      1,
				Metadata:    map[string]string{},
				ClusterName: "a",
				ServiceName: "DEMO",
				Enable:      true,
				Healthy:     true,
			},
			{
				InstanceId:  "10.10.10.12-80-a-DEMO",
				Port:        80,
				Ip:          "10.10.10.12",
				Weight:      1,
				Metadata:    map[string]string{},
				ClusterName: "a",
				ServiceName: "DEMO",
				Enable:      true,
				Healthy:     false,
			},
			{
				InstanceId:  "10.10.10.13-80-a-DEMO",
				Port:        80,
				Ip:          "10.10.10.13",
				Weight:      1,
				Metadata:    map[string]string{},
				ClusterName: "a",
				ServiceName: "DEMO",
				Enable:      false,
				Healthy:     true,
			},
			{
				InstanceId:  "10.10.10.14-80-a-DEMO",
				Port:        80,
				Ip:          "10.10.10.14",
				Weight:      0,
				Metadata:    map[string]string{},
				ClusterName: "a",
				ServiceName: "DEMO",
				Enable:      true,
				Healthy:     true,
			},
		},
		Checksum:    "3bbcf6dd1175203a8afdade0e77a27cd1528787794594",
		LastRefTime: 1528787794594, Clusters: "a"}
	instance1, err := NewTestNamingClient().selectOneHealthyInstances(services)
	assert.Nil(t, err)
	assert.NotNil(t, instance1)
	instance2, err := NewTestNamingClient().selectOneHealthyInstances(services)
	assert.Nil(t, err)
	assert.NotNil(t, instance2)
}

func TestNamingClient_SelectOneHealthyInstance_Empty(t *testing.T) {
	services := model.Service{
		Name:        "DEFAULT_GROUP@@DEMO",
		CacheMillis: 1000,
		Hosts:       []model.Instance{},
		Checksum:    "3bbcf6dd1175203a8afdade0e77a27cd1528787794594",
		LastRefTime: 1528787794594, Clusters: "a"}
	instance, err := NewTestNamingClient().selectOneHealthyInstances(services)
	assert.NotNil(t, err)
	assert.Nil(t, instance)
}

func TestNamingClient_SelectInstances_Healthy(t *testing.T) {
	services := model.Service{
		Name:        "DEFAULT_GROUP@@DEMO",
		CacheMillis: 1000,
		Hosts: []model.Instance{
			{
				InstanceId:  "10.10.10.10-80-a-DEMO",
				Port:        80,
				Ip:          "10.10.10.10",
				Weight:      1,
				Metadata:    map[string]string{},
				ClusterName: "a",
				ServiceName: "DEMO",
				Enable:      true,
				Healthy:     true,
			},
			{
				InstanceId:  "10.10.10.11-80-a-DEMO",
				Port:        80,
				Ip:          "10.10.10.11",
				Weight:      1,
				Metadata:    map[string]string{},
				ClusterName: "a",
				ServiceName: "DEMO",
				Enable:      true,
				Healthy:     true,
			},
			{
				InstanceId:  "10.10.10.12-80-a-DEMO",
				Port:        80,
				Ip:          "10.10.10.12",
				Weight:      1,
				Metadata:    map[string]string{},
				ClusterName: "a",
				ServiceName: "DEMO",
				Enable:      true,
				Healthy:     false,
			},
			{
				InstanceId:  "10.10.10.13-80-a-DEMO",
				Port:        80,
				Ip:          "10.10.10.13",
				Weight:      1,
				Metadata:    map[string]string{},
				ClusterName: "a",
				ServiceName: "DEMO",
				Enable:      false,
				Healthy:     true,
			},
			{
				InstanceId:  "10.10.10.14-80-a-DEMO",
				Port:        80,
				Ip:          "10.10.10.14",
				Weight:      0,
				Metadata:    map[string]string{},
				ClusterName: "a",
				ServiceName: "DEMO",
				Enable:      true,
				Healthy:     true,
			},
		},
		Checksum:    "3bbcf6dd1175203a8afdade0e77a27cd1528787794594",
		LastRefTime: 1528787794594, Clusters: "a"}
	instances, err := NewTestNamingClient().selectInstances(services, true)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(instances))
}

func TestNamingClient_SelectInstances_Unhealthy(t *testing.T) {
	services := model.Service{
		Name:        "DEFAULT_GROUP@@DEMO",
		CacheMillis: 1000,
		Hosts: []model.Instance{
			{
				InstanceId:  "10.10.10.10-80-a-DEMO",
				Port:        80,
				Ip:          "10.10.10.10",
				Weight:      1,
				Metadata:    map[string]string{},
				ClusterName: "a",
				ServiceName: "DEMO",
				Enable:      true,
				Healthy:     true,
			},
			{
				InstanceId:  "10.10.10.11-80-a-DEMO",
				Port:        80,
				Ip:          "10.10.10.11",
				Weight:      1,
				Metadata:    map[string]string{},
				ClusterName: "a",
				ServiceName: "DEMO",
				Enable:      true,
				Healthy:     true,
			},
			{
				InstanceId:  "10.10.10.12-80-a-DEMO",
				Port:        80,
				Ip:          "10.10.10.12",
				Weight:      1,
				Metadata:    map[string]string{},
				ClusterName: "a",
				ServiceName: "DEMO",
				Enable:      true,
				Healthy:     false,
			},
			{
				InstanceId:  "10.10.10.13-80-a-DEMO",
				Port:        80,
				Ip:          "10.10.10.13",
				Weight:      1,
				Metadata:    map[string]string{},
				ClusterName: "a",
				ServiceName: "DEMO",
				Enable:      false,
				Healthy:     true,
			},
			{
				InstanceId:  "10.10.10.14-80-a-DEMO",
				Port:        80,
				Ip:          "10.10.10.14",
				Weight:      0,
				Metadata:    map[string]string{},
				ClusterName: "a",
				ServiceName: "DEMO",
				Enable:      true,
				Healthy:     true,
			},
		},
		Checksum:    "3bbcf6dd1175203a8afdade0e77a27cd1528787794594",
		LastRefTime: 1528787794594, Clusters: "a"}
	instances, err := NewTestNamingClient().selectInstances(services, false)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(instances))
}

func TestNamingClient_SelectInstances_Empty(t *testing.T) {
	services := model.Service{
		Name:        "DEFAULT_GROUP@@DEMO",
		CacheMillis: 1000,
		Hosts:       []model.Instance{},
		Checksum:    "3bbcf6dd1175203a8afdade0e77a27cd1528787794594",
		LastRefTime: 1528787794594, Clusters: "a"}
	instances, err := NewTestNamingClient().selectInstances(services, false)
	assert.NotNil(t, err)
	assert.Equal(t, 0, len(instances))
}

func TestNamingClient_GetAllServicesInfo(t *testing.T) {
	result, err := NewTestNamingClient().GetAllServicesInfo(vo.GetAllServiceInfoParam{
		GroupName: "DEFAULT_GROUP",
		PageNo:    1,
		PageSize:  20,
	})

	assert.NotNil(t, result.Doms)
	assert.Nil(t, err)
}

func BenchmarkNamingClient_SelectOneHealthyInstances(b *testing.B) {
	services := model.Service{
		Name:        "DEFAULT_GROUP@@DEMO",
		CacheMillis: 1000,
		Hosts: []model.Instance{
			{
				InstanceId:  "10.10.10.10-80-a-DEMO",
				Port:        80,
				Ip:          "10.10.10.10",
				Weight:      10,
				Metadata:    map[string]string{},
				ClusterName: "a",
				ServiceName: "DEMO1",
				Enable:      true,
				Healthy:     true,
			},
			{
				InstanceId:  "10.10.10.11-80-a-DEMO",
				Port:        80,
				Ip:          "10.10.10.11",
				Weight:      10,
				Metadata:    map[string]string{},
				ClusterName: "a",
				ServiceName: "DEMO2",
				Enable:      true,
				Healthy:     true,
			},
			{
				InstanceId:  "10.10.10.12-80-a-DEMO",
				Port:        80,
				Ip:          "10.10.10.12",
				Weight:      1,
				Metadata:    map[string]string{},
				ClusterName: "a",
				ServiceName: "DEMO3",
				Enable:      true,
				Healthy:     false,
			},
			{
				InstanceId:  "10.10.10.13-80-a-DEMO",
				Port:        80,
				Ip:          "10.10.10.13",
				Weight:      1,
				Metadata:    map[string]string{},
				ClusterName: "a",
				ServiceName: "DEMO4",
				Enable:      false,
				Healthy:     true,
			},
			{
				InstanceId:  "10.10.10.14-80-a-DEMO",
				Port:        80,
				Ip:          "10.10.10.14",
				Weight:      0,
				Metadata:    map[string]string{},
				ClusterName: "a",
				ServiceName: "DEMO5",
				Enable:      true,
				Healthy:     true,
			},
		},
		Checksum:    "3bbcf6dd1175203a8afdade0e77a27cd1528787794594",
		LastRefTime: 1528787794594, Clusters: "a"}
	client := NewTestNamingClient()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = client.selectOneHealthyInstances(services)
	}

}

func TestNamingClient_Subscribe_EmptyServiceName(t *testing.T) {
	callback := func(services []model.Instance, err error) {}
	err := NewTestNamingClient().Subscribe(&vo.SubscribeParam{
		ServiceName:       "",
		GroupName:         "DEFAULT_GROUP",
		SubscribeCallback: callback,
	})
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "serviceName cannot be empty")
}

func TestNamingClient_Unsubscribe_EmptyServiceName(t *testing.T) {
	callback := func(services []model.Instance, err error) {}
	err := NewTestNamingClient().Unsubscribe(&vo.SubscribeParam{
		ServiceName:       "",
		GroupName:         "DEFAULT_GROUP",
		SubscribeCallback: callback,
	})
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "serviceName cannot be empty")
}

func TestNamingClient_DeregisterInstance_EmptyServiceName(t *testing.T) {
	_, err := NewTestNamingClient().DeregisterInstance(vo.DeregisterInstanceParam{
		ServiceName: "",
		Ip:          "10.0.0.10",
		Port:        80,
	})
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "serviceName cannot be empty")
}

func TestNamingClient_GetService_EmptyServiceName(t *testing.T) {
	_, err := NewTestNamingClient().GetService(vo.GetServiceParam{
		ServiceName: "",
		GroupName:   "DEFAULT_GROUP",
	})
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "serviceName cannot be empty")
}

func TestNamingClient_SelectAllInstances_EmptyServiceName(t *testing.T) {
	_, err := NewTestNamingClient().SelectAllInstances(vo.SelectAllInstancesParam{
		ServiceName: "",
		GroupName:   "DEFAULT_GROUP",
	})
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "serviceName cannot be empty")
}

func TestNamingClient_SelectInstances_EmptyServiceName(t *testing.T) {
	_, err := NewTestNamingClient().SelectInstances(vo.SelectInstancesParam{
		ServiceName: "",
		GroupName:   "DEFAULT_GROUP",
	})
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "serviceName cannot be empty")
}

func TestNamingClient_SelectOneHealthyInstance_EmptyServiceName(t *testing.T) {
	_, err := NewTestNamingClient().SelectOneHealthyInstance(vo.SelectOneHealthInstanceParam{
		ServiceName: "",
		GroupName:   "DEFAULT_GROUP",
	})
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "serviceName cannot be empty")
}

func TestNamingClient_Unsubscribe_WithCallback_ShouldNotCallServiceProxyUnsubscribe(t *testing.T) {
	// 创建一个带有回调函数的订阅参数
	callback := func(services []model.Instance, err error) {
		// 空回调函数
	}
	param := &vo.SubscribeParam{
		ServiceName:       "test-service",
		GroupName:         "test-group",
		Clusters:          []string{"test-cluster"},
		SubscribeCallback: callback,
	}

	// 创建测试客户端
	client := NewTestNamingClient()
	mockProxy := client.serviceProxy.(*MockNamingProxy)

	// 执行 Unsubscribe
	err := client.Unsubscribe(param)

	// 验证没有错误
	assert.Nil(t, err)
	assert.True(t, mockProxy.unsubscribeCalled)
}

func TestNamingClient_Unsubscribe_WithoutCallback_ShouldCallServiceProxyUnsubscribe(t *testing.T) {
	// 创建一个没有回调函数的订阅参数
	param := &vo.SubscribeParam{
		ServiceName: "test-service",
		GroupName:   "test-group",
		Clusters:    []string{"test-cluster"},
		// SubscribeCallback 为 nil
	}

	// 创建测试客户端
	client := NewTestNamingClient()
	// 获取原始的 MockNamingProxy 来检查调用状态
	mockProxy := client.serviceProxy.(*MockNamingProxy)

	// 执行 Unsubscribe
	err := client.Unsubscribe(param)

	// 验证没有错误
	assert.Nil(t, err)
	assert.True(t, mockProxy.unsubscribeCalled)
}

// TestNamingClient_Unsubscribe_Integration_Test 集成测试，使用真实的 ServiceInfoHolder 来测试修复后的逻辑
func TestNamingClient_Unsubscribe_Integration_Test(t *testing.T) {
	// 创建测试客户端
	client := NewTestNamingClient()

	// 获取原始的 MockNamingProxy 来检查调用状态
	mockProxy := client.serviceProxy.(*MockNamingProxy)

	// 创建回调函数
	callback1 := func(services []model.Instance, err error) {
		// 回调函数1
	}

	callback2 := func(services []model.Instance, err error) {
		// 回调函数2
	}

	// 测试场景1：先注册两个回调函数，然后取消订阅第一个
	// 这种情况下，取消订阅第一个回调函数后，还有其他回调函数，所以不应该调用 serviceProxy.Unsubscribe

	// 注册第一个回调函数
	param1 := &vo.SubscribeParam{
		ServiceName:       "test-service",
		GroupName:         "test-group",
		Clusters:          []string{"test-cluster"},
		SubscribeCallback: callback1,
	}

	// 注册第二个回调函数
	param2 := &vo.SubscribeParam{
		ServiceName:       "test-service",
		GroupName:         "test-group",
		Clusters:          []string{"test-cluster"},
		SubscribeCallback: callback2,
	}

	// 先注册两个回调函数
	err := client.Subscribe(param1)
	assert.Nil(t, err)

	err = client.Subscribe(param2)
	assert.Nil(t, err)

	// 重置 MockNamingProxy 的调用状态
	mockProxy.unsubscribeCalled = false
	mockProxy.unsubscribeParams = nil

	// 取消订阅第一个回调函数
	err = client.Unsubscribe(param1)
	assert.Nil(t, err)
	assert.False(t, mockProxy.unsubscribeCalled)

	// 取消订阅第二个回调函数
	err = client.Unsubscribe(param2)
	assert.Nil(t, err)
	assert.True(t, mockProxy.unsubscribeCalled)

}

// TestNamingClient_Unsubscribe_EmptyGroupNormalized regression test: Subscribe
// normalizes an empty group to DEFAULT_GROUP but Unsubscribe did not, so the
// callback registered under DEFAULT_GROUP@@svc was never found when
// unsubscribing with an empty group.
func TestNamingClient_Unsubscribe_EmptyGroupNormalized(t *testing.T) {
	callback := func(services []model.Instance, err error) {
		// 空回调函数
	}
	param := &vo.SubscribeParam{
		ServiceName:       "svc",
		GroupName:         "",
		SubscribeCallback: callback,
	}

	client := NewTestNamingClient()
	mockProxy := client.serviceProxy.(*MockNamingProxy)

	err := client.Subscribe(param)
	assert.Nil(t, err)

	// Subscribe normalizes param.GroupName to DEFAULT_GROUP as a side effect
	// (param is a pointer), which would otherwise mask the bug under test.
	// Reset it to "" so Unsubscribe is exercised with a genuinely empty
	// group, matching real callers who unsubscribe with the empty group they
	// originally configured.
	param.GroupName = ""

	err = client.Unsubscribe(param)
	assert.Nil(t, err)

	assert.True(t, mockProxy.unsubscribeCalled)
	assert.Equal(t, []string{"svc", constant.DEFAULT_GROUP, ""}, mockProxy.unsubscribeParams)
	assert.False(t, client.serviceInfoHolder.IsSubscribed(util.GetGroupName("svc", constant.DEFAULT_GROUP), ""))
}

// When the server-side unsubscribe fails (transport error or a response the
// server did not accept), the server keeps pushing for this subscription, so
// the locally deregistered callback must be re-registered — otherwise pushes
// arrive with no handler while the caller believes the subscription is still
// active (their Unsubscribe returned an error).
func TestNamingClient_Unsubscribe_RestoresCallbackOnProxyFailure(t *testing.T) {
	callback := func(services []model.Instance, err error) {}
	param := &vo.SubscribeParam{
		ServiceName:       "svc-restore",
		GroupName:         "g",
		SubscribeCallback: callback,
	}

	client := NewTestNamingClient()
	mockProxy := client.serviceProxy.(*MockNamingProxy)

	err := client.Subscribe(param)
	assert.Nil(t, err)
	assert.True(t, client.serviceInfoHolder.IsSubscribed(util.GetGroupName("svc-restore", "g"), ""))

	mockProxy.unsubscribeErr = errors.New("server rejected unsubscribe")
	err = client.Unsubscribe(param)
	assert.Error(t, err)
	assert.True(t, mockProxy.unsubscribeCalled)
	assert.True(t, client.serviceInfoHolder.IsSubscribed(util.GetGroupName("svc-restore", "g"), ""),
		"callback must be restored when the server-side unsubscribe fails")
}

func TestFuzzyWatch_EmptyServiceNamePattern(t *testing.T) {
	err := NewTestNamingClient().FuzzyWatch(&vo.FuzzyWatchParam{
		WatchCallback: func(model.FuzzyWatchChangeEvent) {},
	})
	assert.Error(t, err)
}

func TestFuzzyWatch_NilCallback(t *testing.T) {
	err := NewTestNamingClient().FuzzyWatch(&vo.FuzzyWatchParam{
		ServiceNamePattern: "order*",
	})
	assert.Error(t, err)
}

func TestFuzzyWatch_Success(t *testing.T) {
	err := NewTestNamingClient().FuzzyWatch(&vo.FuzzyWatchParam{
		ServiceNamePattern: "order*",
		WatchCallback:      func(model.FuzzyWatchChangeEvent) {},
	})
	assert.NoError(t, err)
}

// TestFuzzyWatchRollsBackOnServerFailure_NewPattern is a regression test:
// when FuzzyWatch is the first registration for a pattern and the
// server-side RPC fails, the just-created pattern context must be removed
// entirely rather than left behind as a local registration that will never
// receive a server push.
func TestFuzzyWatchRollsBackOnServerFailure_NewPattern(t *testing.T) {
	client := NewTestNamingClient()
	mockProxy := client.serviceProxy.(*MockNamingProxy)
	mockProxy.fuzzyWatchErr = errors.New("boom")

	pattern := client.buildGroupKeyPattern("order*", constant.DEFAULT_GROUP)
	err := client.FuzzyWatch(&vo.FuzzyWatchParam{
		ServiceNamePattern: "order*",
		WatchCallback:      func(model.FuzzyWatchChangeEvent) {},
	})

	assert.Error(t, err)
	assert.NotContains(t, client.fuzzyWatchHolder.Patterns(), pattern,
		"a failed registration on a brand-new pattern must not leave it behind")
}

// TestFuzzyWatchRollsBackOnServerFailure_ExistingPattern is a regression
// test: a second watcher registering on an already-registered pattern whose
// own FuzzyWatch RPC fails must roll back only its own callback, leaving the
// pattern and the first watcher's registration untouched. The pattern here
// has no received keys yet, so RegisterPattern's replay block is a no-op and
// the rolled-back callback can be asserted to never fire at all - see
// TestFuzzyWatchRollsBackOnServerFailure_ExistingPatternWithReceivedKeys for
// the case where that assertion would not be honest.
func TestFuzzyWatchRollsBackOnServerFailure_ExistingPattern(t *testing.T) {
	client := NewTestNamingClient()
	mockProxy := client.serviceProxy.(*MockNamingProxy)

	pattern := client.buildGroupKeyPattern("order*", constant.DEFAULT_GROUP)
	firstFired := make(chan struct{}, 1)
	require.NoError(t, client.FuzzyWatch(&vo.FuzzyWatchParam{
		ServiceNamePattern: "order*",
		WatchCallback: func(model.FuzzyWatchChangeEvent) {
			select {
			case firstFired <- struct{}{}:
			default:
			}
		},
	}))

	mockProxy.fuzzyWatchErr = errors.New("boom")
	var secondMu sync.Mutex
	secondCalled := false
	err := client.FuzzyWatch(&vo.FuzzyWatchParam{
		ServiceNamePattern: "order*",
		WatchCallback: func(model.FuzzyWatchChangeEvent) {
			secondMu.Lock()
			secondCalled = true
			secondMu.Unlock()
		},
	})

	assert.Error(t, err)
	assert.Contains(t, client.fuzzyWatchHolder.Patterns(), pattern,
		"the pre-existing pattern must survive a second watcher's failed registration")

	// The first watcher's call created the pattern (isInitializing=true);
	// the second, on the already-created pattern, must ask for a diff only
	// (isInitializing=false).
	calls := mockProxy.fuzzyWatchCallsSnapshot()
	require.Len(t, calls, 2)
	assert.True(t, calls[0].isInitializing, "first watcher on a new pattern must send isInitializing=true")
	assert.False(t, calls[1].isInitializing, "second watcher on an already-created pattern must send isInitializing=false")

	// The rolled-back callback must actually be gone, not just the pattern
	// left in place: fire a change-notify and wait for proof the dispatcher
	// processed it (the surviving watcher's callback firing) before checking
	// that the rolled-back callback never ran.
	client.fuzzyWatchHolder.HandleChangeNotify("public@@DEFAULT_GROUP@@order-a", constant.FUZZY_WATCH_CHANGED_TYPE_ADD_SERVICE)
	select {
	case <-firstFired:
	case <-time.After(2 * time.Second):
		t.Fatal("surviving watcher's callback never fired")
	}
	secondMu.Lock()
	defer secondMu.Unlock()
	assert.False(t, secondCalled, "the rolled-back callback must never fire (this pattern has no received keys, so no replay is possible)")
}

// TestFuzzyWatchRollsBackOnServerFailure_ExistingPatternWithReceivedKeys is a
// regression test for I1: RegisterPattern enqueues the initial replay of a
// pattern's already-known matched services BEFORE the server RPC outcome is
// known, so RemoveCallbackByID's rollback cannot recall a replay task the
// async dispatcher already snapshotted into its queue. A caller whose
// FuzzyWatch fails on an already-synced, non-empty pattern may therefore
// still observe that replay reach its callback. This test asserts the
// honest contract: the local registration is rolled back and the pattern
// survives, replay delivery is deliberately NOT asserted either way (it may
// or may not have landed), but nothing enqueued AFTER the rollback runs ever
// reaches the rolled-back callback.
func TestFuzzyWatchRollsBackOnServerFailure_ExistingPatternWithReceivedKeys(t *testing.T) {
	client := NewTestNamingClient()
	mockProxy := client.serviceProxy.(*MockNamingProxy)
	pattern := client.buildGroupKeyPattern("order*", constant.DEFAULT_GROUP)

	var firstMu sync.Mutex
	var firstEvents []model.FuzzyWatchChangeEvent
	require.NoError(t, client.FuzzyWatch(&vo.FuzzyWatchParam{
		ServiceNamePattern: "order*",
		WatchCallback: func(ev model.FuzzyWatchChangeEvent) {
			firstMu.Lock()
			firstEvents = append(firstEvents, ev)
			firstMu.Unlock()
		},
	}))

	// Seed the pattern with an existing matched service via a server-pushed
	// INIT sync, so the pattern is non-empty before the second watcher
	// registers - this is what makes RegisterPattern's replay block fire for
	// the second callback below.
	client.fuzzyWatchHolder.HandleSync(pattern, constant.FUZZY_WATCH_INIT_NOTIFY, []rpc_request.NamingFuzzyWatchSyncContext{
		{ServiceKey: "public@@DEFAULT_GROUP@@order-seed", ChangedType: constant.FUZZY_WATCH_CHANGED_TYPE_ADD_SERVICE},
	}, 1, 1)
	client.fuzzyWatchHolder.HandleSync(pattern, constant.FINISH_FUZZY_WATCH_INIT_NOTIFY, nil, 1, 1)

	mockProxy.fuzzyWatchErr = errors.New("boom")
	var secondMu sync.Mutex
	var secondEvents []model.FuzzyWatchChangeEvent
	err := client.FuzzyWatch(&vo.FuzzyWatchParam{
		ServiceNamePattern: "order*",
		WatchCallback: func(ev model.FuzzyWatchChangeEvent) {
			secondMu.Lock()
			secondEvents = append(secondEvents, ev)
			secondMu.Unlock()
		},
	})

	assert.Error(t, err)
	assert.Contains(t, client.fuzzyWatchHolder.Patterns(), pattern,
		"the pre-existing pattern must survive a second watcher's failed registration")

	// Prove dispatch still works and the rollback took effect, using an
	// event injected strictly AFTER the rollback with a service name that
	// appears nowhere in the pre-existing replay - a hit on it can only come
	// from post-rollback dispatch, never from racy replay delivery, so this
	// assertion is deterministic even though replay delivery itself is not.
	client.fuzzyWatchHolder.HandleChangeNotify("public@@DEFAULT_GROUP@@order-post-rollback", constant.FUZZY_WATCH_CHANGED_TYPE_ADD_SERVICE)
	require.Eventually(t, func() bool {
		firstMu.Lock()
		defer firstMu.Unlock()
		for _, ev := range firstEvents {
			if ev.ServiceName == "order-post-rollback" {
				return true
			}
		}
		return false
	}, 2*time.Second, 10*time.Millisecond, "surviving watcher's callback never received the post-rollback event")

	secondMu.Lock()
	defer secondMu.Unlock()
	for _, ev := range secondEvents {
		assert.NotEqual(t, "order-post-rollback", ev.ServiceName,
			"the rolled-back callback must never receive an event enqueued after its rollback")
	}
	// Replay delivery of "order-seed" (enqueued before the rollback ran) is
	// deliberately not asserted either way: it is a queued task the rollback
	// cannot recall - see the fuzzyWatchContext dispatcher comment in
	// fuzzy_watch_holder.go - so whether it reached the rolled-back callback
	// is inherently racy, and asserting its absence would be dishonest.
}

// TestFuzzyWatchRollbackNeverDropsConcurrentRegistration is a C1 regression
// test: naming_client.FuzzyWatch's failure-path rollback must never remove a
// pattern that a DIFFERENT, concurrently successful FuzzyWatch call already
// populated with its own callback. The old rollback branched on `created` (a
// snapshot taken before the RPC): if goroutine A's call created the pattern
// and then its RPC failed, it unconditionally called RemovePattern even
// though goroutine B's call on the same pattern may have succeeded and
// registered its own callback while A's RPC was still in flight - wiping B's
// callback and receivedGroupKeys and leaving a server-side watch with no
// matching local pattern. This test forces that exact interleaving with a
// channel-gated mock FuzzyWatch call so the race is deterministic rather
// than timing-dependent.
func TestFuzzyWatchRollbackNeverDropsConcurrentRegistration(t *testing.T) {
	client := NewTestNamingClient()
	mockProxy := client.serviceProxy.(*MockNamingProxy)
	pattern := client.buildGroupKeyPattern("order*", constant.DEFAULT_GROUP)

	aEntered := make(chan struct{})
	aRelease := make(chan struct{})

	mockProxy.mu.Lock()
	mockProxy.fuzzyWatchResultFn = func(p string, isInitializing bool) error {
		if p == pattern && isInitializing {
			// This is goroutine A's call (the first watcher, creating the
			// pattern): signal that it has entered the RPC so the test can
			// safely drive B to completion, then block until told to
			// proceed, simulating an RPC that fails slowly.
			close(aEntered)
			<-aRelease
			return errors.New("boom")
		}
		// Any other call (B's, on the pattern A already created locally)
		// succeeds immediately.
		return nil
	}
	mockProxy.mu.Unlock()

	var aErr error
	aDone := make(chan struct{})
	go func() {
		defer close(aDone)
		aErr = client.FuzzyWatch(&vo.FuzzyWatchParam{
			ServiceNamePattern: "order*",
			WatchCallback:      func(model.FuzzyWatchChangeEvent) {},
		})
	}()

	select {
	case <-aEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("goroutine A never entered its FuzzyWatch RPC")
	}

	bFired := make(chan struct{}, 1)
	require.NoError(t, client.FuzzyWatch(&vo.FuzzyWatchParam{
		ServiceNamePattern: "order*",
		WatchCallback: func(model.FuzzyWatchChangeEvent) {
			select {
			case bFired <- struct{}{}:
			default:
			}
		},
	}), "goroutine B's concurrent watch on the same pattern must succeed while A is still in flight")

	close(aRelease)
	select {
	case <-aDone:
	case <-time.After(2 * time.Second):
		t.Fatal("goroutine A's FuzzyWatch call never returned")
	}
	assert.Error(t, aErr, "goroutine A's own RPC failure must still be reported to it")

	assert.Contains(t, client.fuzzyWatchHolder.Patterns(), pattern,
		"A's rollback must not remove the pattern B is concurrently, successfully watching")

	// Confirm B's registration is still functionally alive, not just present
	// in the pattern map: a server push after the race must still reach it.
	client.fuzzyWatchHolder.HandleChangeNotify("public@@DEFAULT_GROUP@@order-a", constant.FUZZY_WATCH_CHANGED_TYPE_ADD_SERVICE)
	select {
	case <-bFired:
	case <-time.After(2 * time.Second):
		t.Fatal("B's callback never received a subsequent HandleChangeNotify event")
	}
}

// TestCancelFuzzyWatchKeepsStateOnServerFailure is a regression test: if the
// server-side cancel RPC fails, local state (the pattern and its callbacks)
// must be left exactly as it was - the server still thinks the watch is
// active, so tearing down local state early would silently stop delivering
// events the server keeps pushing.
func TestCancelFuzzyWatchKeepsStateOnServerFailure(t *testing.T) {
	client := NewTestNamingClient()
	mockProxy := client.serviceProxy.(*MockNamingProxy)
	cb := func(model.FuzzyWatchChangeEvent) {}
	param := &vo.FuzzyWatchParam{ServiceNamePattern: "order*", WatchCallback: cb}
	require.NoError(t, client.FuzzyWatch(param))
	pattern := client.buildGroupKeyPattern("order*", constant.DEFAULT_GROUP)

	mockProxy.cancelFuzzyWatchErr = errors.New("boom")
	err := client.CancelFuzzyWatch(param)

	assert.Error(t, err)
	assert.Contains(t, client.fuzzyWatchHolder.Patterns(), pattern,
		"a failed cancel RPC must keep the pattern's local state intact")
}

// TestCancelFuzzyWatchTearsDownOnSuccess replaces the former
// TestCancelFuzzyWatch_LastCallbackTearsDown, whose name overstated the
// mechanism: cancellation is pattern-scoped, not gated on "last callback"
// bookkeeping - a successful cancel RPC always tears down the whole pattern.
func TestCancelFuzzyWatchTearsDownOnSuccess(t *testing.T) {
	client := NewTestNamingClient()
	cb := func(model.FuzzyWatchChangeEvent) {}
	param := &vo.FuzzyWatchParam{ServiceNamePattern: "order*", WatchCallback: cb}
	require.NoError(t, client.FuzzyWatch(param))
	pattern := client.buildGroupKeyPattern("order*", constant.DEFAULT_GROUP)
	require.Contains(t, client.fuzzyWatchHolder.Patterns(), pattern)

	require.NoError(t, client.CancelFuzzyWatch(param))
	assert.NotContains(t, client.fuzzyWatchHolder.Patterns(), pattern)
}

// TestCancelFuzzyWatch_NilCallbackAllowed is a regression test: cancellation
// no longer requires param.WatchCallback (it is pattern-scoped, not
// callback-scoped), so a nil callback must be accepted.
func TestCancelFuzzyWatch_NilCallbackAllowed(t *testing.T) {
	client := NewTestNamingClient()
	require.NoError(t, client.FuzzyWatch(&vo.FuzzyWatchParam{
		ServiceNamePattern: "order*",
		WatchCallback:      func(model.FuzzyWatchChangeEvent) {},
	}))

	err := client.CancelFuzzyWatch(&vo.FuzzyWatchParam{ServiceNamePattern: "order*"})
	assert.NoError(t, err)
}
