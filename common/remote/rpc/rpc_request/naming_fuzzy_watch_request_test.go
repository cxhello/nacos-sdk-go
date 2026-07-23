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

	"github.com/nacos-group/nacos-sdk-go/v3/common/constant"
)

func TestNamingFuzzyWatchRequestProtoMessage(t *testing.T) {
	r := NewNamingFuzzyWatchRequest("ns", "public>>g*>>svc*", constant.FUZZY_WATCH_TYPE_WATCH,
		[]string{"public@@g@@svc1", "public@@g@@svc2"}, true)
	r.RequestId = "1"

	assert.Equal(t, constant.FUZZY_WATCH_REQUEST_NAME, r.GetRequestType())
	assert.Equal(t, "NamingFuzzyWatchRequest", r.GetRequestType())

	msg, ok := r.ProtoMessage().(*naming.NamingFuzzyWatchRequest)
	require.True(t, ok)
	assert.Equal(t, "1", msg.RequestId)
	assert.True(t, msg.IsInitializing)
	assert.Equal(t, "ns", msg.Namespace)
	assert.Equal(t, "public>>g*>>svc*", msg.GroupKeyPattern)
	assert.Equal(t, []string{"public@@g@@svc1", "public@@g@@svc2"}, msg.ReceivedGroupKeys)
	assert.Equal(t, "WATCH", msg.WatchType)
}

func TestNamingFuzzyWatchRequestProtoMessage_CancelWatch(t *testing.T) {
	r := NewNamingFuzzyWatchRequest("ns", "public>>g*>>svc*", constant.FUZZY_WATCH_TYPE_CANCEL_WATCH, nil, false)
	msg, ok := r.ProtoMessage().(*naming.NamingFuzzyWatchRequest)
	require.True(t, ok)
	assert.Equal(t, "CANCEL_WATCH", msg.WatchType)
	assert.False(t, msg.IsInitializing)
}

func TestNamingFuzzyWatchRequestGetStringToSign(t *testing.T) {
	// GetStringToSign mirrors NamingRequest's no-service-name case: only a
	// timestamp, no group@@service to sign.
	r := NewNamingFuzzyWatchRequest("ns", "public>>g*>>svc*", constant.FUZZY_WATCH_TYPE_WATCH, nil, false)
	sign := r.GetStringToSign()
	assert.NotEmpty(t, sign)
	for _, c := range sign {
		assert.True(t, c >= '0' && c <= '9', "GetStringToSign should be a pure timestamp, got %q", sign)
	}
}

func TestNamingFuzzyWatchSyncRequestGetRequestType(t *testing.T) {
	r := &NamingFuzzyWatchSyncRequest{Request: &Request{}}
	assert.Equal(t, constant.FUZZY_WATCH_SYNC_REQUEST_NAME, r.GetRequestType())
	assert.Equal(t, "NamingFuzzyWatchSyncRequest", r.GetRequestType())
}

func TestNamingFuzzyWatchChangeNotifyRequestGetRequestType(t *testing.T) {
	r := &NamingFuzzyWatchChangeNotifyRequest{Request: &Request{}}
	assert.Equal(t, constant.FUZZY_WATCH_CHANGE_NOTIFY_REQUEST_NAME, r.GetRequestType())
	assert.Equal(t, "NamingFuzzyWatchChangeNotifyRequest", r.GetRequestType())
}
