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

func TestClassifyLoginStatus_Credential(t *testing.T) {
	for _, code := range []int{http.StatusUnauthorized, http.StatusForbidden} {
		err := classifyLoginStatus(code, "unknown user!")
		assert.Error(t, err)
		assert.True(t, errors.Is(err, ErrLoginFailed), "status %d should be credential error", code)
	}
}

func TestClassifyLoginStatus_Transient(t *testing.T) {
	err := classifyLoginStatus(http.StatusInternalServerError, "server busy")
	assert.Error(t, err)
	assert.False(t, errors.Is(err, ErrLoginFailed), "5xx should not be credential error")
}
