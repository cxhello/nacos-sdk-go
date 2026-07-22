package security

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nacos-group/nacos-sdk-go/v3/common/constant"
	"github.com/stretchr/testify/assert"
)

// MockResponseBody creates a mock response body for testing
type MockResponseBody struct {
	*bytes.Buffer
}

func (m *MockResponseBody) Close() error {
	return nil
}

func NewMockResponseBody(data interface{}) io.ReadCloser {
	var buf bytes.Buffer
	if str, ok := data.(string); ok {
		buf.WriteString(str)
	} else {
		enc := json.NewEncoder(&buf)
		enc.SetEscapeHTML(false)
		enc.Encode(data)
	}
	return &MockResponseBody{&buf}
}

// MockHttpAgent implements http_agent.IHttpAgent for testing
type MockHttpAgent struct {
	PostFunc func(url string, header http.Header, timeoutMs uint64, params map[string]string) (response *http.Response, err error)
}

func (m *MockHttpAgent) Request(method string, url string, header http.Header, timeoutMs uint64, params map[string]string) (*http.Response, error) {
	switch method {
	case http.MethodPost:
		return m.Post(url, header, timeoutMs, params)
	default:
		return &http.Response{
			StatusCode: http.StatusMethodNotAllowed,
			Body:       NewMockResponseBody("method not allowed"),
		}, nil
	}
}

func (m *MockHttpAgent) RequestOnlyResult(method string, url string, header http.Header, timeoutMs uint64, params map[string]string) string {
	resp, err := m.Request(method, url, header, timeoutMs, params)
	if err != nil {
		return ""
	}
	if resp.Body == nil {
		return ""
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return ""
	}
	return string(data)
}

func (m *MockHttpAgent) Get(url string, header http.Header, timeoutMs uint64, params map[string]string) (*http.Response, error) {
	return m.Request(http.MethodGet, url, header, timeoutMs, params)
}

func (m *MockHttpAgent) Post(url string, header http.Header, timeoutMs uint64, params map[string]string) (*http.Response, error) {
	if m.PostFunc != nil {
		return m.PostFunc(url, header, timeoutMs, params)
	}
	return &http.Response{
		StatusCode: http.StatusNotImplemented,
		Body:       NewMockResponseBody("not implemented"),
	}, nil
}

func (m *MockHttpAgent) Delete(url string, header http.Header, timeoutMs uint64, params map[string]string) (*http.Response, error) {
	return m.Request(http.MethodDelete, url, header, timeoutMs, params)
}

func (m *MockHttpAgent) Put(url string, header http.Header, timeoutMs uint64, params map[string]string) (*http.Response, error) {
	return m.Request(http.MethodPut, url, header, timeoutMs, params)
}

func TestNacosAuthClient_Login_Success(t *testing.T) {
	// Setup mock response
	mockResp := &http.Response{
		StatusCode: constant.RESPONSE_CODE_SUCCESS,
		Body: NewMockResponseBody(map[string]interface{}{
			constant.KEY_ACCESS_TOKEN: "test-token",
			constant.KEY_TOKEN_TTL:    float64(10),
		}),
	}

	mockAgent := &MockHttpAgent{
		PostFunc: func(url string, header http.Header, timeoutMs uint64, params map[string]string) (*http.Response, error) {
			// Verify request parameters
			assert.Equal(t, "test-user", params["username"])
			assert.Equal(t, "test-pass", params["password"])
			contentType := header["content-type"]
			assert.Equal(t, []string{"application/x-www-form-urlencoded"}, contentType)
			return mockResp, nil
		},
	}

	// Create client config
	clientConfig := constant.ClientConfig{
		Username:  "test-user",
		Password:  "test-pass",
		TimeoutMs: 10000,
	}

	serverConfigs := []constant.ServerConfig{
		{
			IpAddr:      "127.0.0.1",
			Port:        8848,
			ContextPath: "/nacos",
		},
	}

	client := NewNacosAuthClient(clientConfig, serverConfigs, mockAgent)

	// Test login
	success, err := client.Login()
	assert.NoError(t, err)
	assert.True(t, success)

	// Verify token is stored
	assert.Equal(t, "test-token", client.GetAccessToken())
}

func TestNacosAuthClient_Login_NoAuth(t *testing.T) {
	mockAgent := &MockHttpAgent{
		PostFunc: func(url string, header http.Header, timeoutMs uint64, params map[string]string) (*http.Response, error) {
			t.Fatal("Should not make HTTP call when no username is set")
			return nil, nil
		},
	}

	clientConfig := constant.ClientConfig{}
	serverConfigs := []constant.ServerConfig{{}}

	client := NewNacosAuthClient(clientConfig, serverConfigs, mockAgent)

	success, err := client.Login()
	assert.NoError(t, err)
	assert.True(t, success)
	assert.Empty(t, client.GetAccessToken())
}

func TestNacosAuthClient_TokenRefresh(t *testing.T) {
	callCount := 0
	mockAgent := &MockHttpAgent{
		PostFunc: func(url string, header http.Header, timeoutMs uint64, params map[string]string) (*http.Response, error) {
			callCount++
			return &http.Response{
				StatusCode: constant.RESPONSE_CODE_SUCCESS,
				Body: NewMockResponseBody(map[string]interface{}{
					constant.KEY_ACCESS_TOKEN: "token-" + fmt.Sprintf("%d", callCount),
					constant.KEY_TOKEN_TTL:    float64(1), // 1 second TTL for quick testing
				}),
			}, nil
		},
	}

	clientConfig := constant.ClientConfig{
		Username: "test-user",
		Password: "test-pass",
	}

	client := NewNacosAuthClient(clientConfig, []constant.ServerConfig{{IpAddr: "localhost"}}, mockAgent)

	// Initial login
	success, err := client.Login()
	assert.NoError(t, err)
	assert.True(t, success)
	assert.Equal(t, "token-1", client.GetAccessToken())

	// Wait for token to require refresh (1 second TTL)
	time.Sleep(time.Second * 2)

	// Second login should get new token
	success, err = client.Login()
	assert.NoError(t, err)
	assert.True(t, success)
	assert.Equal(t, "token-2", client.GetAccessToken())
}

func TestNacosAuthClient_AutoRefresh(t *testing.T) {
	callCount := 0
	tokenChan := make(chan string, 2)
	mockAgent := &MockHttpAgent{
		PostFunc: func(url string, header http.Header, timeoutMs uint64, params map[string]string) (*http.Response, error) {
			callCount++
			token := fmt.Sprintf("auto-token-%d", callCount)
			tokenChan <- token
			t.Logf("Mock server received request #%d, returning token: %s", callCount, token)
			return &http.Response{
				StatusCode: constant.RESPONSE_CODE_SUCCESS,
				Body: NewMockResponseBody(map[string]interface{}{
					constant.KEY_ACCESS_TOKEN: token,
					constant.KEY_TOKEN_TTL:    float64(10), // 10 seconds TTL, resulting in 1s refresh window
				}),
			}, nil
		},
	}

	clientConfig := constant.ClientConfig{
		Username: "test-user",
		Password: "test-pass",
	}

	client := NewNacosAuthClient(clientConfig, []constant.ServerConfig{{IpAddr: "localhost"}}, mockAgent)

	// First do a manual login
	t.Log("Performing initial manual login")
	success, err := client.Login()
	assert.NoError(t, err)
	assert.True(t, success)
	token1 := <-tokenChan // Get the token from the first login
	t.Logf("Initial login successful, token: %s", token1)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	defer cancel()

	// Start auto refresh
	t.Log("Starting auto refresh")
	client.AutoRefresh(ctx)

	// Wait for token refresh (should happen after TTL-2*refreshWindow seconds = 8 seconds)
	// We'll wait a bit longer to account for any delays
	t.Log("Waiting for token refresh")
	var token2 string
	select {
	case token2 = <-tokenChan:
		t.Logf("Received refreshed token: %s", token2)
	case <-time.After(time.Second * 12):
		t.Fatal("Timeout waiting for token refresh")
	}

	assert.NotEqual(t, token1, token2, "Token should have been refreshed")
	assert.Equal(t, "auto-token-1", token1, "First token should be auto-token-1")
	assert.Equal(t, "auto-token-2", token2, "Second token should be auto-token-2")
}

func TestNacosAuthClient_GetSecurityInfo(t *testing.T) {
	client := NewNacosAuthClient(constant.ClientConfig{}, []constant.ServerConfig{}, nil)

	// When no token
	info := client.GetSecurityInfo(RequestResource{})
	assert.Empty(t, info[constant.KEY_ACCESS_TOKEN])

	// When token exists
	mockToken := "test-security-token"
	client.accessToken.Store(mockToken)

	info = client.GetSecurityInfo(RequestResource{})
	assert.Equal(t, mockToken, info[constant.KEY_ACCESS_TOKEN])
}

func TestNacosAuthClient_LoginFailure(t *testing.T) {
	mockAgent := &MockHttpAgent{
		PostFunc: func(url string, header http.Header, timeoutMs uint64, params map[string]string) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusUnauthorized,
				Body:       NewMockResponseBody("Invalid credentials"),
			}, nil
		},
	}

	client := NewNacosAuthClient(
		constant.ClientConfig{Username: "wrong-user", Password: "wrong-pass"},
		[]constant.ServerConfig{{IpAddr: "localhost"}},
		mockAgent,
	)

	success, err := client.Login()
	assert.Error(t, err)
	assert.True(t, errors.Is(err, ErrLoginFailed), "401 must be classified as credential error")
	assert.False(t, success)
	assert.Empty(t, client.GetAccessToken())
}

func TestNacosAuthClient_ConcurrentLoginAndUpdateServerList(t *testing.T) {
	mockAgent := &MockHttpAgent{
		PostFunc: func(url string, header http.Header, timeoutMs uint64, params map[string]string) (*http.Response, error) {
			return &http.Response{
				StatusCode: constant.RESPONSE_CODE_SUCCESS,
				Body: NewMockResponseBody(map[string]interface{}{
					constant.KEY_ACCESS_TOKEN: "concurrent-token",
					constant.KEY_TOKEN_TTL:    float64(10),
				}),
			}, nil
		},
	}
	client := NewNacosAuthClient(
		constant.ClientConfig{Username: "u", Password: "p"},
		[]constant.ServerConfig{{IpAddr: "localhost"}},
		mockAgent,
	)

	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(3)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				client.UpdateServerList([]constant.ServerConfig{{IpAddr: "127.0.0.1"}, {IpAddr: "localhost"}})
			}
		}
	}()
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_, _ = client.Login()
			}
		}
	}()
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_ = client.GetServerList()
			}
		}
	}()
	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()
}

func TestNacosAuthClient_Login_InvalidSuccessResponses(t *testing.T) {
	cases := []struct {
		name string
		body map[string]interface{}
	}{
		{"no accessToken", map[string]interface{}{constant.KEY_TOKEN_TTL: float64(10)}},
		{"empty accessToken", map[string]interface{}{constant.KEY_ACCESS_TOKEN: "", constant.KEY_TOKEN_TTL: float64(10)}},
		{"non-string accessToken", map[string]interface{}{constant.KEY_ACCESS_TOKEN: float64(123), constant.KEY_TOKEN_TTL: float64(10)}},
		{"missing tokenTtl", map[string]interface{}{constant.KEY_ACCESS_TOKEN: "tok"}},
		{"zero tokenTtl", map[string]interface{}{constant.KEY_ACCESS_TOKEN: "tok", constant.KEY_TOKEN_TTL: float64(0)}},
		{"negative tokenTtl", map[string]interface{}{constant.KEY_ACCESS_TOKEN: "tok", constant.KEY_TOKEN_TTL: float64(-5)}},
		{"sub-second tokenTtl", map[string]interface{}{constant.KEY_ACCESS_TOKEN: "tok", constant.KEY_TOKEN_TTL: float64(0.5)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mockAgent := &MockHttpAgent{
				PostFunc: func(url string, header http.Header, timeoutMs uint64, params map[string]string) (*http.Response, error) {
					return &http.Response{
						StatusCode: constant.RESPONSE_CODE_SUCCESS,
						Body:       NewMockResponseBody(tc.body),
					}, nil
				},
			}
			client := NewNacosAuthClient(
				constant.ClientConfig{Username: "u", Password: "p"},
				[]constant.ServerConfig{{IpAddr: "localhost"}},
				mockAgent,
			)
			success, err := client.Login()
			assert.False(t, success, "invalid success response must not report success")
			assert.Error(t, err)
			assert.True(t, errors.Is(err, ErrLoginFailed), "invalid success response should wrap ErrLoginFailed")
			assert.Empty(t, client.GetAccessToken(), "no token should be stored")

			_, armed := client.refreshDeadlineUnix()
			assert.False(t, armed, "a rejected login response must not arm the refresh state")
		})
	}
}

func TestNacosAuthClient_Login_ValidResponseStillSucceeds(t *testing.T) {
	mockAgent := &MockHttpAgent{
		PostFunc: func(url string, header http.Header, timeoutMs uint64, params map[string]string) (*http.Response, error) {
			return &http.Response{
				StatusCode: constant.RESPONSE_CODE_SUCCESS,
				Body: NewMockResponseBody(map[string]interface{}{
					constant.KEY_ACCESS_TOKEN: "valid-token",
					constant.KEY_TOKEN_TTL:    float64(18000),
				}),
			}, nil
		},
	}
	client := NewNacosAuthClient(
		constant.ClientConfig{Username: "u", Password: "p"},
		[]constant.ServerConfig{{IpAddr: "localhost"}},
		mockAgent,
	)
	success, err := client.Login()
	assert.True(t, success)
	assert.NoError(t, err)
	assert.Equal(t, "valid-token", client.GetAccessToken())
}

func TestNacosAuthClient_RefreshDeadline_Deterministic(t *testing.T) {
	client := NewNacosAuthClient(constant.ClientConfig{Username: "u"}, nil, nil)
	client.mux.Lock()
	client.lastRefreshTime = 1_000_000
	client.tokenTtl = 10
	client.tokenRefreshWindow = 1
	client.mux.Unlock()

	deadline, ok := client.refreshDeadlineUnix()
	assert.True(t, ok)
	// lastRefreshTime + ttl - 2*window = 1000000 + 10 - 2 = 1000008
	assert.Equal(t, int64(1_000_008), deadline, "deadline must be two windows before expiry")
}

func TestNacosAuthClient_RefreshDeadline_NotLoggedIn(t *testing.T) {
	client := NewNacosAuthClient(constant.ClientConfig{Username: "u"}, nil, nil)
	_, ok := client.refreshDeadlineUnix()
	assert.False(t, ok, "no deadline before a successful login")
	assert.Equal(t, retryDelay, client.nextRefreshDelay(), "unauthenticated delay is the retry cadence")
}

func TestNacosAuthClient_NextRefreshDelay_ClampsPastDeadline(t *testing.T) {
	client := NewNacosAuthClient(constant.ClientConfig{Username: "u"}, nil, nil)
	client.mux.Lock()
	// deadline far in the past → raw delay is negative → must clamp to minRefreshDelay
	client.lastRefreshTime = 1
	client.tokenTtl = 10
	client.tokenRefreshWindow = 1
	client.mux.Unlock()
	assert.Equal(t, minRefreshDelay, client.nextRefreshDelay(), "a passed deadline must not spin the timer")
}

func TestNacosAuthClient_NextRefreshDelay_FutureDeadline(t *testing.T) {
	client := NewNacosAuthClient(constant.ClientConfig{Username: "u"}, nil, nil)
	now := time.Now().Unix()
	client.mux.Lock()
	client.lastRefreshTime = now - 3
	client.tokenTtl = 10
	client.tokenRefreshWindow = 1
	client.mux.Unlock()

	// deadline = lastRefreshTime + ttl - 2*window = (now-3) + 8 = now + 5.
	// The old, buggy rule scheduled ttl-window = 9s from now instead.
	delay := client.nextRefreshDelay()
	assert.True(t, delay == 5*time.Second || delay == 4*time.Second,
		"expected ~5s anchored to lastRefreshTime, got %v (the old ttl-window rule would give 9s)", delay)
}

func TestNacosAuthClient_Login_ErrorDoesNotLeakAccessToken(t *testing.T) {
	const secret = "super-secret-access-token"
	mockAgent := &MockHttpAgent{
		PostFunc: func(url string, header http.Header, timeoutMs uint64, params map[string]string) (*http.Response, error) {
			// A usable accessToken but no tokenTtl: rejected on the ttl check, at
			// which point the raw body still carries a valid token.
			return &http.Response{
				StatusCode: constant.RESPONSE_CODE_SUCCESS,
				Body:       NewMockResponseBody(map[string]interface{}{constant.KEY_ACCESS_TOKEN: secret}),
			}, nil
		},
	}
	client := NewNacosAuthClient(
		constant.ClientConfig{Username: "u", Password: "p"},
		[]constant.ServerConfig{{IpAddr: "localhost"}},
		mockAgent,
	)

	success, err := client.Login()
	assert.False(t, success)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, ErrLoginFailed))
	assert.NotContains(t, err.Error(), secret,
		"the login error is logged by SecurityProxy/AutoRefresh and must not leak the access token")
}

func TestNacosAuthClient_Login_PreservesCredentialErrorAcrossServers(t *testing.T) {
	// 10.0.0.1 rejects the credentials (401 -> ErrLoginFailed);
	// 10.0.0.2 is unreachable (transport error, stays transient).
	newClient := func(servers []constant.ServerConfig) *NacosAuthClient {
		agent := &MockHttpAgent{
			PostFunc: func(url string, header http.Header, timeoutMs uint64, params map[string]string) (*http.Response, error) {
				if strings.Contains(url, "10.0.0.1") {
					return &http.Response{
						StatusCode: http.StatusUnauthorized,
						Body:       NewMockResponseBody("unknown user!"),
					}, nil
				}
				return nil, errors.New("dial tcp 10.0.0.2:8848: connect: connection refused")
			},
		}
		return NewNacosAuthClient(constant.ClientConfig{Username: "u", Password: "p"}, servers, agent)
	}

	t.Run("credential first then transport", func(t *testing.T) {
		client := newClient([]constant.ServerConfig{{IpAddr: "10.0.0.1"}, {IpAddr: "10.0.0.2"}})
		success, err := client.Login()
		assert.False(t, success)
		assert.True(t, errors.Is(err, ErrLoginFailed),
			"a credential failure must survive a later transport error, got %v", err)
	})

	t.Run("transport first then credential", func(t *testing.T) {
		client := newClient([]constant.ServerConfig{{IpAddr: "10.0.0.2"}, {IpAddr: "10.0.0.1"}})
		success, err := client.Login()
		assert.False(t, success)
		assert.True(t, errors.Is(err, ErrLoginFailed),
			"a credential failure must be reported regardless of server order, got %v", err)
	})
}
