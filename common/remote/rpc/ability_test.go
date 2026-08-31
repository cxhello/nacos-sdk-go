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
	"testing"
	"time"

	"github.com/nacos-group/nacos-sdk-proto/go/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nacos-group/nacos-sdk-go/v3/common/remote/rpc/rpc_request"
)

func TestConnectionAbilityTable(t *testing.T) {
	c := &Connection{}
	_, ok := c.getAbilityTable()
	assert.False(t, ok, "table must be absent before SetupAck arrives")
	c.setAbilityTable(map[string]bool{"fuzzyWatch": true})
	table, ok := c.getAbilityTable()
	require.True(t, ok)
	assert.True(t, table["fuzzyWatch"])
}

func TestIsAbilitySupportedByServer(t *testing.T) {
	r := &RpcClient{}
	// no connection: false after timeout
	assert.False(t, r.IsAbilitySupportedByServer("fuzzyWatch", 150*time.Millisecond))
	// connection present but table lacks the key: false
	conn := &GrpcConnection{Connection: &Connection{}}
	conn.setAbilityTable(map[string]bool{"other": true})
	r.SetCurrentConnection(conn)
	assert.False(t, r.IsAbilitySupportedByServer("fuzzyWatch", 150*time.Millisecond))
	// table arrives late (simulating SetupAck landing asynchronously after
	// setup): becomes true within the bounded wait
	conn2 := &GrpcConnection{Connection: &Connection{}}
	r.SetCurrentConnection(conn2)
	go func() {
		time.Sleep(100 * time.Millisecond)
		conn2.setAbilityTable(map[string]bool{"fuzzyWatch": true})
	}()
	assert.True(t, r.IsAbilitySupportedByServer("fuzzyWatch", 2*time.Second))
}

func TestDecodeProtoSetupAckRequest(t *testing.T) {
	payload, err := payloadCodec.Encode("SetupAckRequest", &common.SetupAckRequest{
		RequestId:    "9",
		AbilityTable: map[string]bool{"fuzzyWatch": true},
	}, nil, "127.0.0.1")
	require.NoError(t, err)

	req, decoded := decodeProtoServerRequest(payload)
	require.True(t, decoded)
	ack, ok := req.(*rpc_request.SetupAckRequest)
	require.True(t, ok)
	assert.Equal(t, "9", ack.RequestId)
	assert.True(t, ack.AbilityTable["fuzzyWatch"])
}
