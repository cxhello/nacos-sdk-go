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
	"encoding/json"
	"testing"

	"github.com/nacos-group/nacos-sdk-proto/go/naming"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

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

// TestNamingFuzzyWatchRequestProtoWireName pins the wire name the proto
// encoding path actually puts on the flag, mirroring
// codec.PayloadCodec.Encode (protojson.MarshalOptions{EmitDefaultValues:
// true}.Marshal on ProtoMessage()). nacos-sdk-proto beta.9 added an explicit
// json_name=initializing to the proto field, matching the server's
// Jackson-derived property name - see the struct doc comment on
// NamingFuzzyWatchRequest. Before that, protojson emitted "isInitializing"
// and the server silently dropped the flag.
func TestNamingFuzzyWatchRequestProtoWireName(t *testing.T) {
	r := NewNamingFuzzyWatchRequest("ns", "public>>g*>>svc*", constant.FUZZY_WATCH_TYPE_WATCH,
		[]string{"public@@g@@svc1", "public@@g@@svc2"}, true)
	r.RequestId = "1"

	jsonBytes, err := protojson.MarshalOptions{EmitDefaultValues: true}.Marshal(r.ProtoMessage())
	require.NoError(t, err)

	body := string(jsonBytes)
	assert.Contains(t, body, `"initializing"`, "proto-encoded payload must carry the flag under its Jackson-derived name")
	assert.NotContains(t, body, `"isInitializing"`, "proto-encoded payload must not carry the flag under the discarded default json name")

	var decoded map[string]interface{}
	require.NoError(t, json.Unmarshal(jsonBytes, &decoded))
	assert.Equal(t, true, decoded["initializing"])
}

// TestNamingFuzzyWatchRequestWireBody pins the legacy JSON body's field name
// for the initializing flag: the server's Jackson binding derives the
// property "initializing" from the boolean isInitializing bean (no
// @JsonProperty override), so the body must use that name, never
// "isInitializing" - see the struct doc comment on NamingFuzzyWatchRequest.
// This is the fallback path if proto encoding is ever bypassed, so it is
// pinned independently of TestNamingFuzzyWatchRequestProtoWireName above.
func TestNamingFuzzyWatchRequestWireBody(t *testing.T) {
	r := NewNamingFuzzyWatchRequest("ns", "public>>g*>>svc*", constant.FUZZY_WATCH_TYPE_WATCH,
		[]string{"public@@g@@svc1", "public@@g@@svc2"}, true)
	r.RequestId = "1"

	body := r.GetBody(r)
	var decoded map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(body), &decoded))

	assert.Equal(t, true, decoded["initializing"], "wire body must carry the flag under its Jackson-derived name")
	_, hasLegacyName := decoded["isInitializing"]
	assert.False(t, hasLegacyName, "wire body must not carry the flag under the discarded proto json_name")
	assert.Equal(t, "1", decoded["requestId"])
	assert.Equal(t, "ns", decoded["namespace"])
	assert.Equal(t, "public>>g*>>svc*", decoded["groupKeyPattern"])
	assert.Equal(t, []interface{}{"public@@g@@svc1", "public@@g@@svc2"}, decoded["receivedGroupKeys"])
	assert.Equal(t, "WATCH", decoded["watchType"])
}

func TestNamingFuzzyWatchRequestWireBody_CancelWatch(t *testing.T) {
	r := NewNamingFuzzyWatchRequest("ns", "public>>g*>>svc*", constant.FUZZY_WATCH_TYPE_CANCEL_WATCH, nil, false)

	var decoded map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(r.GetBody(r)), &decoded))

	assert.Equal(t, "CANCEL_WATCH", decoded["watchType"])
	assert.Equal(t, false, decoded["initializing"])
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
