package security

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nacos-group/nacos-sdk-go/v3/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v3/common/http_agent"
	"github.com/nacos-group/nacos-sdk-go/v3/common/logger"
)

type NacosAuthClient struct {
	username           string
	password           string
	accessToken        *atomic.Value
	tokenTtl           int64
	lastRefreshTime    int64
	tokenRefreshWindow int64
	agent              http_agent.IHttpAgent
	clientCfg          constant.ClientConfig
	serverCfgs         []constant.ServerConfig
	mux                sync.Mutex
}

func NewNacosAuthClient(clientCfg constant.ClientConfig, serverCfgs []constant.ServerConfig, agent http_agent.IHttpAgent) *NacosAuthClient {
	client := &NacosAuthClient{
		username:    clientCfg.Username,
		password:    clientCfg.Password,
		serverCfgs:  serverCfgs,
		clientCfg:   clientCfg,
		agent:       agent,
		accessToken: &atomic.Value{},
	}

	return client
}

func (ac *NacosAuthClient) GetAccessToken() string {
	v := ac.accessToken.Load()
	if v == nil {
		return ""
	}
	return v.(string)
}

func (ac *NacosAuthClient) GetSecurityInfo(resource RequestResource) map[string]string {
	var securityInfo = make(map[string]string, 4)
	v := ac.accessToken.Load()
	if v != nil {
		securityInfo[constant.KEY_ACCESS_TOKEN] = v.(string)
	}
	return securityInfo
}

func (ac *NacosAuthClient) AutoRefresh(ctx context.Context) {
	// If the username is not set, the automatic refresh Token is not enabled
	if ac.username == "" {
		return
	}

	go func() {
		ac.mux.Lock()
		lastRefreshTime := ac.lastRefreshTime
		tokenTtl := ac.tokenTtl
		tokenRefreshWindow := ac.tokenRefreshWindow
		ac.mux.Unlock()

		var timer *time.Timer
		if lastLoginSuccess := lastRefreshTime > 0 && tokenTtl > 0 && tokenRefreshWindow > 0; lastLoginSuccess {
			timer = time.NewTimer(time.Second * time.Duration(tokenTtl-tokenRefreshWindow))
		} else {
			timer = time.NewTimer(time.Second * time.Duration(5))
		}
		defer timer.Stop()
		for {
			select {
			case <-timer.C:
				_, err := ac.Login()
				if err != nil {
					logger.Errorf("login has error %+v", err)
					timer.Reset(time.Second * time.Duration(5))
				} else {
					ac.mux.Lock()
					ttl := ac.tokenTtl
					window := ac.tokenRefreshWindow
					ac.mux.Unlock()
					logger.Infof("login success, tokenTtl: %+v seconds, tokenRefreshWindow: %+v seconds", ttl, window)
					timer.Reset(time.Second * time.Duration(ttl-window))
				}
			case <-ctx.Done():
				return
			}
		}
	}()
}

func (ac *NacosAuthClient) Login() (bool, error) {
	ac.mux.Lock()
	servers := make([]constant.ServerConfig, len(ac.serverCfgs))
	copy(servers, ac.serverCfgs)
	ac.mux.Unlock()

	var throwable error = nil
	for i := 0; i < len(servers); i++ {
		result, err := ac.login(servers[i])
		throwable = err
		if result {
			return true, nil
		}
	}
	return false, throwable
}

func (ac *NacosAuthClient) UpdateServerList(serverList []constant.ServerConfig) {
	ac.mux.Lock()
	ac.serverCfgs = serverList
	ac.mux.Unlock()
}

func (ac *NacosAuthClient) GetServerList() []constant.ServerConfig {
	ac.mux.Lock()
	defer ac.mux.Unlock()
	return ac.serverCfgs
}

func (ac *NacosAuthClient) login(server constant.ServerConfig) (bool, error) {
	ac.mux.Lock()
	lastRefreshTime := ac.lastRefreshTime
	tokenTtl := ac.tokenTtl
	tokenRefreshWindow := ac.tokenRefreshWindow
	ac.mux.Unlock()

	if lastRefreshTime > 0 && tokenTtl > 0 {
		// We refresh 2 windows before expiration to ensure continuous availability
		tokenRefreshTime := lastRefreshTime + tokenTtl - 2*tokenRefreshWindow
		if time.Now().Unix() < tokenRefreshTime {
			return true, nil
		}
	}
	if ac.username == "" {
		ac.mux.Lock()
		ac.lastRefreshTime = time.Now().Unix()
		ac.mux.Unlock()
		return true, nil
	}

	contextPath := server.ContextPath
	if !strings.HasPrefix(contextPath, "/") {
		contextPath = "/" + contextPath
	}
	if strings.HasSuffix(contextPath, "/") {
		contextPath = contextPath[0 : len(contextPath)-1]
	}
	if server.Scheme == "" {
		server.Scheme = "http"
	}

	reqUrl := server.Scheme + "://" + server.IpAddr + ":" + strconv.FormatInt(int64(server.Port), 10) + contextPath + "/v1/auth/users/login"

	header := http.Header{
		"content-type": []string{"application/x-www-form-urlencoded"},
	}
	resp, err := ac.agent.Post(reqUrl, header, ac.clientCfg.TimeoutMs, map[string]string{
		"username": ac.username,
		"password": ac.password,
	})
	if err != nil {
		return false, err
	}

	var bytes []byte
	bytes, err = io.ReadAll(resp.Body)
	defer resp.Body.Close()
	if err != nil {
		return false, err
	}

	if resp.StatusCode != constant.RESPONSE_CODE_SUCCESS {
		return false, classifyLoginStatus(resp.StatusCode, string(bytes))
	}

	var result map[string]interface{}
	err = json.Unmarshal(bytes, &result)
	if err != nil {
		return false, err
	}

	accessToken, ok := result[constant.KEY_ACCESS_TOKEN].(string)
	if !ok || accessToken == "" {
		return false, fmt.Errorf("%w: login response missing a valid accessToken: %s", ErrLoginFailed, string(bytes))
	}
	ttl, ok := result[constant.KEY_TOKEN_TTL].(float64)
	if !ok || ttl <= 0 {
		return false, fmt.Errorf("%w: login response has missing or non-positive tokenTtl: %s", ErrLoginFailed, string(bytes))
	}

	ac.accessToken.Store(accessToken)
	ac.mux.Lock()
	ac.lastRefreshTime = time.Now().Unix()
	ac.tokenTtl = int64(ttl)
	ac.tokenRefreshWindow = ac.tokenTtl / 10
	ac.mux.Unlock()

	return true, nil
}
