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
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClassifyLoginStatus_WrapsAllNon200(t *testing.T) {
	// Real Nacos servers return an inconsistent mix of 401/403/500 for rejected
	// credentials across versions (2.x: 403 for a wrong password, 500 for an
	// unknown user; 3.x is the reverse), and a 5xx body carries no reliable
	// credential hint. Any non-200 login response means no token was issued, so
	// classifyLoginStatus wraps them all as ErrLoginFailed for FailOnAuthError
	// to act on. Transport errors (server unreachable) never reach here.
	for _, code := range []int{
		http.StatusUnauthorized,
		http.StatusForbidden,
		http.StatusInternalServerError,
		http.StatusBadRequest,
	} {
		err := classifyLoginStatus(code, "rejected")
		assert.Error(t, err)
		assert.True(t, errors.Is(err, ErrLoginFailed), "status %d should wrap ErrLoginFailed", code)
	}
}
