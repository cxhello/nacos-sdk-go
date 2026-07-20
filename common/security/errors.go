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
	"fmt"
	"net/http"
)

// ErrLoginFailed marks a credential-level auth failure (HTTP 401/403), as
// opposed to a transient network or server error. NewNacosServer only aborts
// client construction on this error when ClientConfig.FailOnAuthError is set;
// transient errors are always retried by the auto-refresh loop.
var ErrLoginFailed = errors.New("nacos auth login failed")

// classifyLoginStatus wraps a non-200 login response. HTTP 401/403 indicate a
// credential problem and wrap ErrLoginFailed; any other status is treated as a
// transient failure that should be retried rather than surfaced to NewClient.
func classifyLoginStatus(statusCode int, body string) error {
	if statusCode == http.StatusUnauthorized || statusCode == http.StatusForbidden {
		return fmt.Errorf("%w: status=%d body=%s", ErrLoginFailed, statusCode, body)
	}
	return fmt.Errorf("nacos auth login unexpected response: status=%d body=%s", statusCode, body)
}
