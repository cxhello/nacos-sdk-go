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
)

// ErrLoginFailed marks an auth login failure: either the login endpoint
// returned a non-200 response, or it returned 200 with a body that carries no
// usable token (missing/empty/non-string accessToken, or missing/non-positive
// tokenTtl) — in both cases no access token was issued. NewNacosServer aborts
// client construction on this error only when ClientConfig.FailOnAuthError is
// set. Transport errors (server unreachable) are not wrapped in this and stay
// transient, always retried by the auto-refresh loop.
var ErrLoginFailed = errors.New("nacos auth login failed")

// classifyLoginStatus wraps any non-200 login response as ErrLoginFailed. Real
// Nacos servers return an inconsistent mix of 401/403/500 for rejected
// credentials across versions (2.x returns 403 for a wrong password but 500 for
// an unknown user; 3.x is the reverse), and a 5xx body carries no reliable
// credential hint. Since any non-200 response means no token was issued, all are
// surfaced as an auth failure so FailOnAuthError can act on them consistently.
// Transport errors (server unreachable) never reach here — the caller keeps
// those transient and retryable.
func classifyLoginStatus(statusCode int, body string) error {
	return fmt.Errorf("%w: status=%d body=%s", ErrLoginFailed, statusCode, body)
}
