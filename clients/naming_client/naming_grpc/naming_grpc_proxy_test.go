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
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"github.com/nacos-group/nacos-sdk-go/v3/clients/naming_client/naming_proxy"
	"github.com/nacos-group/nacos-sdk-go/v3/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v3/common/remote/rpc/rpc_request"
	"github.com/nacos-group/nacos-sdk-go/v3/common/remote/rpc/rpc_response"
	"github.com/nacos-group/nacos-sdk-go/v3/model"
	"github.com/nacos-group/nacos-sdk-go/v3/util"
)

// newTestProxy builds a bare NamingGrpcProxy with a stubbed send function so
// tests can assert on the exact request sequence without a live rpc client.
func newTestProxy() (*NamingGrpcProxy, *[]rpc_request.IRequest) {
	var sent []rpc_request.IRequest
	proxy := &NamingGrpcProxy{clientConfig: constant.ClientConfig{NamespaceId: "ns"}}
	proxy.eventListener = NewConnectionEventListener(proxy)
	proxy.send = func(r rpc_request.IRequest) (rpc_response.IResponse, error) {
		sent = append(sent, r)
		return &rpc_response.InstanceResponse{Response: &rpc_response.Response{Success: true, ResultCode: 200}}, nil
	}
	return proxy, &sent
}

func TestRegisterInstanceKeepsSingleShape(t *testing.T) {
	proxy, sent := newTestProxy()
	instanceA := model.Instance{Ip: "1.1.1.1", Port: 8080}
	instanceB := model.Instance{Ip: "2.2.2.2", Port: 9090}

	ok, err := proxy.RegisterInstance("svc", "group", instanceA)
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Len(t, *sent, 1)
	req, isInstanceReq := (*sent)[0].(*rpc_request.InstanceRequest)
	assert.True(t, isInstanceReq)
	assert.Equal(t, "registerInstance", req.Type)
	assert.Equal(t, instanceA, req.Instance)

	// A second RegisterInstance on the same service must NOT be upgraded to a
	// batch request: it still sends a plain InstanceRequest and overwrites the
	// single-shape redo cache.
	ok, err = proxy.RegisterInstance("svc", "group", instanceB)
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Len(t, *sent, 2)
	req2, isInstanceReq := (*sent)[1].(*rpc_request.InstanceRequest)
	assert.True(t, isInstanceReq)
	assert.Equal(t, "registerInstance", req2.Type)
	assert.Equal(t, instanceB, req2.Instance)

	_, isBatch := proxy.eventListener.GetBatchInstancesForRedo("svc", "group")
	assert.False(t, isBatch)
	cached, ok := proxy.eventListener.registeredInstanceCached.Get(util.GetGroupName("svc", "group"))
	assert.True(t, ok)
	assert.Equal(t, instanceB, cached)
}

func TestDeregisterKnownBatchMemberRepublishesRetained(t *testing.T) {
	proxy, sent := newTestProxy()
	a := model.Instance{Ip: "1.1.1.1", Port: 8080}
	b := model.Instance{Ip: "2.2.2.2", Port: 8081}
	c := model.Instance{Ip: "3.3.3.3", Port: 8082}

	_, err := proxy.BatchRegisterInstance("svc", "group", []model.Instance{a, b, c})
	assert.NoError(t, err)
	*sent = (*sent)[:0]

	ok, err := proxy.DeregisterInstance("svc", "group", b)
	assert.NoError(t, err)
	assert.True(t, ok)

	assert.Len(t, *sent, 1)
	req, isBatchReq := (*sent)[0].(*rpc_request.BatchInstanceRequest)
	assert.True(t, isBatchReq)
	assert.Equal(t, "batchRegisterInstance", req.Type)
	assert.Equal(t, []model.Instance{a, c}, req.Instances)

	for _, r := range *sent {
		_, isPlain := r.(*rpc_request.InstanceRequest)
		assert.False(t, isPlain, "no plain deregisterInstance request should be sent for a batch member")
	}

	retained, isBatch := proxy.eventListener.GetBatchInstancesForRedo("svc", "group")
	assert.True(t, isBatch)
	assert.Equal(t, []model.Instance{a, c}, retained)
}

func TestDeregisterUnknownBatchMemberRepublishesSameSet(t *testing.T) {
	proxy, sent := newTestProxy()
	a := model.Instance{Ip: "1.1.1.1", Port: 8080}
	b := model.Instance{Ip: "2.2.2.2", Port: 8081}
	unknown := model.Instance{Ip: "9.9.9.9", Port: 9999}

	_, err := proxy.BatchRegisterInstance("svc", "group", []model.Instance{a, b})
	assert.NoError(t, err)
	*sent = (*sent)[:0]

	ok, err := proxy.DeregisterInstance("svc", "group", unknown)
	assert.NoError(t, err)
	assert.True(t, ok)

	assert.Len(t, *sent, 1)
	req, isBatchReq := (*sent)[0].(*rpc_request.BatchInstanceRequest)
	assert.True(t, isBatchReq)
	assert.Equal(t, []model.Instance{a, b}, req.Instances)

	retained, isBatch := proxy.eventListener.GetBatchInstancesForRedo("svc", "group")
	assert.True(t, isBatch)
	assert.Equal(t, []model.Instance{a, b}, retained)
}

func TestDeregisterLastBatchMemberSendsEmptyNonNilBatch(t *testing.T) {
	proxy, sent := newTestProxy()
	a := model.Instance{Ip: "1.1.1.1", Port: 8080}

	_, err := proxy.BatchRegisterInstance("svc", "group", []model.Instance{a})
	assert.NoError(t, err)
	*sent = (*sent)[:0]

	ok, err := proxy.DeregisterInstance("svc", "group", a)
	assert.NoError(t, err)
	assert.True(t, ok)

	assert.Len(t, *sent, 1)
	req, isBatchReq := (*sent)[0].(*rpc_request.BatchInstanceRequest)
	assert.True(t, isBatchReq)
	assert.NotNil(t, req.Instances)
	assert.Len(t, req.Instances, 0)
	assert.True(t, strings.Contains(req.GetBody(req), `"instances":[]`),
		"expected empty-but-non-nil instances array in wire body, got: %s", req.GetBody(req))

	retained, isBatch := proxy.eventListener.GetBatchInstancesForRedo("svc", "group")
	assert.True(t, isBatch)
	assert.Len(t, retained, 0)
}

func TestDeregisterSingleShapeSendsPlainDeregister(t *testing.T) {
	proxy, sent := newTestProxy()
	a := model.Instance{Ip: "1.1.1.1", Port: 8080}

	_, err := proxy.RegisterInstance("svc", "group", a)
	assert.NoError(t, err)
	*sent = (*sent)[:0]

	ok, err := proxy.DeregisterInstance("svc", "group", a)
	assert.NoError(t, err)
	assert.True(t, ok)

	assert.Len(t, *sent, 1)
	req, isPlain := (*sent)[0].(*rpc_request.InstanceRequest)
	assert.True(t, isPlain)
	assert.Equal(t, "deregisterInstance", req.Type)

	_, cached := proxy.eventListener.registeredInstanceCached.Get(util.GetGroupName("svc", "group"))
	assert.False(t, cached)

	// A service that was never registered also gets a plain deregister
	// (Java parity: there is no redo data to consult, so it falls through
	// the same single-shape path).
	*sent = (*sent)[:0]
	ok, err = proxy.DeregisterInstance("never-registered", "group", a)
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Len(t, *sent, 1)
	req2, isPlain := (*sent)[0].(*rpc_request.InstanceRequest)
	assert.True(t, isPlain)
	assert.Equal(t, "deregisterInstance", req2.Type)
}

func TestRedoReplayPreservesShape(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockProxy := naming_proxy.NewMockINamingProxy(ctrl)
	evListener := NewConnectionEventListener(mockProxy)

	single := model.Instance{Ip: "1.1.1.1", Port: 8080}
	batch := []model.Instance{
		{Ip: "2.2.2.2", Port: 8081},
		{Ip: "3.3.3.3", Port: 8082},
	}

	evListener.CacheInstanceForRedo("svc-single", "group", single)
	evListener.CacheInstancesForRedo("svc-batch", "group", batch)

	mockProxy.EXPECT().RegisterInstance("svc-single", "group", single).Return(true, nil)
	mockProxy.EXPECT().BatchRegisterInstance("svc-batch", "group", batch).Return(true, nil)

	evListener.redoRegisterEachService()
}

// TestDeregisterAfterShapeTransitionTakesBatchPath drives the redo cache
// through single -> batch shape (RegisterInstance(A) then
// BatchRegisterInstance([A,B])) and asserts that deregistering A afterwards
// takes the batch republish path, not the plain-deregister path: DeregisterInstance
// consults the CURRENT redo shape (checked under redoMu), not whichever shape
// happened to be cached when the service was first touched.
func TestDeregisterAfterShapeTransitionTakesBatchPath(t *testing.T) {
	proxy, sent := newTestProxy()
	a := model.Instance{Ip: "1.1.1.1", Port: 8080}
	b := model.Instance{Ip: "2.2.2.2", Port: 8081}

	_, err := proxy.RegisterInstance("svc", "group", a)
	assert.NoError(t, err)
	_, err = proxy.BatchRegisterInstance("svc", "group", []model.Instance{a, b})
	assert.NoError(t, err)
	*sent = (*sent)[:0]

	ok, err := proxy.DeregisterInstance("svc", "group", a)
	assert.NoError(t, err)
	assert.True(t, ok)

	assert.Len(t, *sent, 1)
	req, isBatchReq := (*sent)[0].(*rpc_request.BatchInstanceRequest)
	assert.True(t, isBatchReq, "deregister after a single->batch shape transition must take the batch path")
	assert.Equal(t, []model.Instance{b}, req.Instances)

	for _, r := range *sent {
		_, isPlain := r.(*rpc_request.InstanceRequest)
		assert.False(t, isPlain, "no plain deregisterInstance request should be sent once the service is batch-shaped")
	}
}

// TestDeregisterSingleShapePropagatesSendError verifies error propagation on
// the single-shape path and documents a deliberate Java-parity ordering: the
// redo entry is removed BEFORE the send is attempted (mirrors Java's
// deregisterServiceForEphemeral, which drops the redo data first and lets the
// send fail independently) - so even when send fails, the client no longer
// believes it owns a redo entry for this instance.
func TestDeregisterSingleShapePropagatesSendError(t *testing.T) {
	proxy, _ := newTestProxy()
	sendErr := errors.New("boom")
	proxy.send = func(r rpc_request.IRequest) (rpc_response.IResponse, error) {
		return nil, sendErr
	}

	a := model.Instance{Ip: "1.1.1.1", Port: 8080}
	proxy.eventListener.CacheInstanceForRedo("svc", "group", a)

	ok, err := proxy.DeregisterInstance("svc", "group", a)
	assert.False(t, ok)
	assert.Equal(t, sendErr, err)

	// Deliberate Java-parity ordering: the redo entry is already gone even
	// though the send failed.
	_, cached := proxy.eventListener.registeredInstanceCached.Get(util.GetGroupName("svc", "group"))
	assert.False(t, cached, "redo entry must be removed before the send is attempted, regardless of send outcome")
}

// TestConcurrentRegisterAndDeregisterDoesNotDeadlock hammers RegisterInstance/
// BatchRegisterInstance against DeregisterInstance on the same service from
// two goroutines. It intentionally asserts nothing about which shape wins -
// only that redoMu (now guarding all three entry points, not just the batch
// path) never self-deadlocks and the run finishes promptly. Run with -race to
// let the race detector confirm the redo cache and the recorder are never
// touched unsynchronized.
func TestConcurrentRegisterAndDeregisterDoesNotDeadlock(t *testing.T) {
	proxy := &NamingGrpcProxy{clientConfig: constant.ClientConfig{NamespaceId: "ns"}}
	proxy.eventListener = NewConnectionEventListener(proxy)

	var mu sync.Mutex
	var sent []rpc_request.IRequest
	recordSend := func(r rpc_request.IRequest) (rpc_response.IResponse, error) {
		mu.Lock()
		sent = append(sent, r)
		mu.Unlock()
		return &rpc_response.InstanceResponse{Response: &rpc_response.Response{Success: true, ResultCode: 200}}, nil
	}
	proxy.send = recordSend

	const iterations = 200
	a := model.Instance{Ip: "1.1.1.1", Port: 8080}
	b := model.Instance{Ip: "2.2.2.2", Port: 8081}

	done := make(chan struct{})
	go func() {
		defer close(done)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				if i%2 == 0 {
					_, _ = proxy.RegisterInstance("svc", "group", a)
				} else {
					_, _ = proxy.BatchRegisterInstance("svc", "group", []model.Instance{a, b})
				}
			}
		}()
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				_, _ = proxy.DeregisterInstance("svc", "group", a)
			}
		}()
		wg.Wait()
	}()

	select {
	case <-done:
		// success: no deadlock within the guard timeout. The -race flag does
		// the real synchronization checking; this only guards against a hang.
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for concurrent register/deregister to finish - possible deadlock on redoMu")
	}
}
