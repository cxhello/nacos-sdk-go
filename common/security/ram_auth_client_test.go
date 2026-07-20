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

package security

import (
	"context"
	"testing"

	"github.com/nacos-group/nacos-sdk-go/v3/common/constant"
)

// Compile-time assertion that both auth clients satisfy AuthClient.
var (
	_ AuthClient = (*NacosAuthClient)(nil)
	_ AuthClient = (*RamAuthClient)(nil)
)

func TestRamAuthClient_AutoRefresh_NoOp(t *testing.T) {
	client := NewRamAuthClient(constant.ClientConfig{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Must not panic or block.
	client.AutoRefresh(ctx)
}
