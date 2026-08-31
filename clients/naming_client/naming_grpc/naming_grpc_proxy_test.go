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
	"github.com/stretchr/testify/require"

	"github.com/nacos-group/nacos-sdk-go/v3/clients/naming_client/naming_cache"
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

// TestUnsubscribeRestoresRedoOnFailure verifies that when the server-side
// unsubscribe request fails, the redo cache entry is restored so a later
// reconnect keeps re-subscribing instead of silently dropping the
// subscription (the server only stops pushing once an unsubscribe actually
// succeeds).
func TestUnsubscribeRestoresRedoOnFailure(t *testing.T) {
	proxy, _ := newTestProxy()
	proxy.eventListener.CacheSubscriberForRedo(util.GetGroupName("svc", "g"), "")
	proxy.send = func(r rpc_request.IRequest) (rpc_response.IResponse, error) {
		return nil, errors.New("boom")
	}

	err := proxy.Unsubscribe("svc", "g", "")

	assert.Error(t, err)
	assert.True(t, proxy.eventListener.IsSubscriberCached(util.GetServiceCacheKey(util.GetGroupName("svc", "g"), "")))
}

// A response the server answered but did not accept (IsSuccess()==false)
// arrives as (response, nil) from the rpc client, not as a transport error.
// It must be treated exactly like a failure: surface an error and restore
// the redo entry, otherwise the server keeps pushing a subscription the
// client no longer tracks and reconnect never re-subscribes.
func TestUnsubscribeRestoresRedoOnUnsuccessfulResponse(t *testing.T) {
	proxy, _ := newTestProxy()
	proxy.eventListener.CacheSubscriberForRedo(util.GetGroupName("svc", "g"), "")
	proxy.send = func(r rpc_request.IRequest) (rpc_response.IResponse, error) {
		return &rpc_response.SubscribeServiceResponse{
			Response: &rpc_response.Response{Success: false, ResultCode: 500, Message: "server rejected"},
		}, nil
	}

	err := proxy.Unsubscribe("svc", "g", "")

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "server rejected")
	assert.True(t, proxy.eventListener.IsSubscriberCached(util.GetServiceCacheKey(util.GetGroupName("svc", "g"), "")))
}

// The proto wire carries ports as int32 while the public API exposes uint64:
// an unchecked conversion would silently truncate 2^32+80 to 80 and make an
// invalid register/deregister operate on a different, valid endpoint (the
// legacy JSON wire sent the original value and let the server reject it).
// Ports must therefore be rejected client-side before any redo-cache
// mutation or request send.
func TestRegisterInstanceRejectsOutOfRangePort(t *testing.T) {
	proxy, sent := newTestProxy()
	bad := model.Instance{Ip: "1.1.1.1", Port: 1<<32 + 80}

	ok, err := proxy.RegisterInstance("svc", "group", bad)

	assert.Error(t, err)
	assert.False(t, ok)
	assert.Contains(t, err.Error(), "port")
	assert.Empty(t, *sent, "no request may be sent for an invalid port")
	_, cached := proxy.eventListener.registeredInstanceCached.Get(util.GetGroupName("svc", "group"))
	assert.False(t, cached, "redo cache must stay untouched")
}

func TestBatchRegisterInstanceRejectsOutOfRangePort(t *testing.T) {
	proxy, sent := newTestProxy()
	a := model.Instance{Ip: "1.1.1.1", Port: 8080}
	seed := []model.Instance{a}
	_, err := proxy.BatchRegisterInstance("svc", "group", seed)
	assert.NoError(t, err)
	*sent = (*sent)[:0]

	bad := []model.Instance{a, {Ip: "2.2.2.2", Port: 1<<32 + 80}}
	ok, err := proxy.BatchRegisterInstance("svc", "group", bad)

	assert.Error(t, err)
	assert.False(t, ok)
	assert.Empty(t, *sent)
	retained, isBatch := proxy.eventListener.GetBatchInstancesForRedo("svc", "group")
	assert.True(t, isBatch)
	assert.Equal(t, seed, retained, "redo cache must keep the previous valid set")
}

func TestDeregisterInstanceRejectsOutOfRangePort(t *testing.T) {
	proxy, sent := newTestProxy()
	a := model.Instance{Ip: "1.1.1.1", Port: 8080}
	b := model.Instance{Ip: "2.2.2.2", Port: 8081}
	_, err := proxy.BatchRegisterInstance("svc", "group", []model.Instance{a, b})
	assert.NoError(t, err)
	*sent = (*sent)[:0]

	// 2^32+8080 truncates to 8080 as int32 — without validation this would
	// republish the batch minus instance a's neighbor at the truncated port.
	bad := model.Instance{Ip: "1.1.1.1", Port: 1<<32 + 8080}
	ok, err := proxy.DeregisterInstance("svc", "group", bad)

	assert.Error(t, err)
	assert.False(t, ok)
	assert.Empty(t, *sent)
	retained, isBatch := proxy.eventListener.GetBatchInstancesForRedo("svc", "group")
	assert.True(t, isBatch)
	assert.Equal(t, []model.Instance{a, b}, retained)
}

func TestSendFuzzyWatchRequestSendsWatch(t *testing.T) {
	proxy, sent := newTestProxy()
	err := proxy.SendFuzzyWatchRequest("public>>DEFAULT_GROUP>>order*", constant.FUZZY_WATCH_TYPE_WATCH, nil, true)
	require.NoError(t, err)

	require.Len(t, *sent, 1)
	req, ok := (*sent)[0].(*rpc_request.NamingFuzzyWatchRequest)
	require.True(t, ok, "SendFuzzyWatchRequest sends a NamingFuzzyWatchRequest")
	assert.Equal(t, constant.FUZZY_WATCH_TYPE_WATCH, req.WatchType)
	assert.True(t, req.IsInitializing, "first watch is initializing")
	assert.Equal(t, "public>>DEFAULT_GROUP>>order*", req.GroupKeyPattern)
}

func TestSendFuzzyWatchRequestSendsCancel(t *testing.T) {
	proxy, sent := newTestProxy()
	err := proxy.SendFuzzyWatchRequest("public>>DEFAULT_GROUP>>order*", constant.FUZZY_WATCH_TYPE_CANCEL_WATCH, nil, false)
	require.NoError(t, err)
	require.Len(t, *sent, 1)
	req := (*sent)[0].(*rpc_request.NamingFuzzyWatchRequest)
	assert.Equal(t, constant.FUZZY_WATCH_TYPE_CANCEL_WATCH, req.WatchType)
}

// TestSendFuzzyWatchRequestSurfacesServerError is a regression test: a
// non-success reply must come back as *naming_cache.FuzzyWatchServerError
// with the server's errorCode preserved, so the reconcile worker can tell a
// capacity rejection from a transient failure.
func TestSendFuzzyWatchRequestSurfacesServerError(t *testing.T) {
	proxy, _ := newTestProxy()
	proxy.send = func(r rpc_request.IRequest) (rpc_response.IResponse, error) {
		return &rpc_response.InstanceResponse{Response: &rpc_response.Response{
			ResultCode: 500, ErrorCode: constant.ERROR_CODE_FUZZY_WATCH_PATTERN_OVER_LIMIT, Message: "boom",
		}}, nil
	}

	err := proxy.SendFuzzyWatchRequest("public>>DEFAULT_GROUP>>order*", constant.FUZZY_WATCH_TYPE_WATCH, nil, true)

	require.Error(t, err)
	var serverErr *naming_cache.FuzzyWatchServerError
	require.ErrorAs(t, err, &serverErr)
	assert.Equal(t, constant.ERROR_CODE_FUZZY_WATCH_PATTERN_OVER_LIMIT, serverErr.ErrorCode)
}

// TestSendFuzzyWatchRequestNilResponse is a regression test: send() can
// legally return a nil response alongside a nil error, which must not be
// mistaken for success.
func TestSendFuzzyWatchRequestNilResponse(t *testing.T) {
	proxy, _ := newTestProxy()
	proxy.send = func(r rpc_request.IRequest) (rpc_response.IResponse, error) {
		return nil, nil
	}

	err := proxy.SendFuzzyWatchRequest("public>>DEFAULT_GROUP>>order*", constant.FUZZY_WATCH_TYPE_WATCH, nil, true)
	assert.Error(t, err)
}
