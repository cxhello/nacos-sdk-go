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
	"sync"

	"github.com/nacos-group/nacos-sdk-go/v3/common/remote/rpc/rpc_request"
	"github.com/nacos-group/nacos-sdk-go/v3/common/remote/rpc/rpc_response"
	"google.golang.org/grpc"
)

type IConnection interface {
	request(request rpc_request.IRequest, timeoutMills int64, client *RpcClient) (rpc_response.IResponse, error)
	close()
	getConnectionId() string
	getServerInfo() ServerInfo
	setAbandon(flag bool)
	getAbandon() bool
	setAbilityTable(table map[string]bool)
	getAbilityTable() (map[string]bool, bool)
}

type Connection struct {
	conn         *grpc.ClientConn
	connectionId string
	abandon      bool
	serverInfo   ServerInfo

	abilityMu    sync.RWMutex
	abilityTable map[string]bool
}

func (c *Connection) getConnectionId() string {
	return c.connectionId
}

func (c *Connection) getServerInfo() ServerInfo {
	return c.serverInfo
}

func (c *Connection) setAbandon(flag bool) {
	c.abandon = flag
}

func (c *Connection) getAbandon() bool {
	return c.abandon
}

func (c *Connection) close() {
	_ = c.conn.Close()
}

// setAbilityTable stores the server-advertised ability table. Called from the
// bi-stream push handler when SetupAckRequest arrives.
func (c *Connection) setAbilityTable(table map[string]bool) {
	c.abilityMu.Lock()
	defer c.abilityMu.Unlock()
	c.abilityTable = table
}

// getAbilityTable returns the negotiated table and whether it has arrived yet.
// Absent (false) is distinct from empty: pre-2.2 servers never send SetupAck.
func (c *Connection) getAbilityTable() (map[string]bool, bool) {
	c.abilityMu.RLock()
	defer c.abilityMu.RUnlock()
	return c.abilityTable, c.abilityTable != nil
}
