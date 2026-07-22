package security

import (
	"context"
	"sync"
	"time"

	"github.com/nacos-group/nacos-sdk-go/v3/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v3/common/logger"
)

type RamContext struct {
	SignatureRegionId    string
	AccessKey            string
	SecretKey            string
	SecurityToken        string
	EphemeralAccessKeyId bool
}

type RamAuthClient struct {
	clientConfig           constant.ClientConfig
	ramCredentialProviders []RamCredentialProvider
	resourceInjector       map[string]ResourceInjector
	// matchedProvider and initialized are read by GetSecurityInfo on every
	// request while AutoRefresh's retry goroutine may still be initializing,
	// so both are guarded by mux.
	matchedProvider RamCredentialProvider
	initialized     bool
	initRetryDelay  time.Duration
	mux             sync.Mutex
}

func NewRamAuthClient(clientCfg constant.ClientConfig) *RamAuthClient {
	var providers = []RamCredentialProvider{
		&RamRoleArnCredentialProvider{
			clientConfig: clientCfg,
		},
		&EcsRamRoleCredentialProvider{
			clientConfig: clientCfg,
		},
		&OIDCRoleArnCredentialProvider{
			clientConfig: clientCfg,
		},
		&CredentialsURICredentialProvider{
			clientConfig: clientCfg,
		},
		&AutoRotateCredentialProvider{
			clientConfig: clientCfg,
		},
		&StsTokenCredentialProvider{
			clientConfig: clientCfg,
		},
		&AccessKeyCredentialProvider{
			clientConfig: clientCfg,
		},
	}
	injectors := map[string]ResourceInjector{
		REQUEST_TYPE_NAMING: &NamingResourceInjector{},
		REQUEST_TYPE_CONFIG: &ConfigResourceInjector{},
	}
	return &RamAuthClient{
		clientConfig:           clientCfg,
		ramCredentialProviders: providers,
		resourceInjector:       injectors,
		initRetryDelay:         retryDelay,
	}
}

func NewRamAuthClientWithProvider(clientCfg constant.ClientConfig, ramCredentialProvider RamCredentialProvider) *RamAuthClient {
	ramAuthClient := NewRamAuthClient(clientCfg)
	if ramCredentialProvider != nil {
		ramAuthClient.ramCredentialProviders = append(ramAuthClient.ramCredentialProviders, ramCredentialProvider)
	}

	return ramAuthClient
}

func (rac *RamAuthClient) Login() (bool, error) {
	rac.mux.Lock()
	provider, initialized := rac.matchedProvider, rac.initialized
	rac.mux.Unlock()

	// Initialization already succeeded; per-request credentials come from
	// GetCredentialsForNacosClient, so there is nothing to redo here.
	if initialized {
		return true, nil
	}

	if provider == nil {
		for _, candidate := range rac.ramCredentialProviders {
			if candidate.MatchProvider() {
				provider = candidate
				break
			}
		}
		if provider == nil {
			return false, nil
		}
		rac.mux.Lock()
		rac.matchedProvider = provider
		rac.mux.Unlock()
	}

	if err := provider.Init(); err != nil {
		return false, err
	}
	rac.mux.Lock()
	rac.initialized = true
	rac.mux.Unlock()
	return true, nil
}

func (rac *RamAuthClient) GetSecurityInfo(resource RequestResource) map[string]string {
	var securityInfo = make(map[string]string, 4)
	rac.mux.Lock()
	provider, initialized := rac.matchedProvider, rac.initialized
	rac.mux.Unlock()
	// Never sign with a provider whose Init has not succeeded: it would produce
	// an empty RamContext and silently unsigned requests.
	if provider == nil || !initialized {
		return securityInfo
	}
	ramContext := provider.GetCredentialsForNacosClient()
	rac.resourceInjector[resource.requestType].doInject(resource, ramContext, securityInfo)
	return securityInfo
}

func (rac *RamAuthClient) UpdateServerList(serverList []constant.ServerConfig) {
	return
}

// AutoRefresh retries a failed provider initialization until it succeeds, then
// stops. There is no background token to renew — RAM/STS credentials are
// resolved per request by GetCredentialsForNacosClient — but Init can fail
// transiently at startup, and without a retry the client would keep signing
// with an uninitialized provider.
func (rac *RamAuthClient) AutoRefresh(ctx context.Context) {
	go func() {
		timer := time.NewTimer(rac.initRetryDelay)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				rac.mux.Lock()
				provider, initialized := rac.matchedProvider, rac.initialized
				rac.mux.Unlock()
				// Nothing to retry: no provider matched, or Init already succeeded.
				if provider == nil || initialized {
					return
				}
				if _, err := rac.Login(); err != nil {
					logger.Warnf("ram credential provider initialization failed, will retry: %v", err)
					timer.Reset(rac.initRetryDelay)
					continue
				}
				return
			}
		}
	}()
}
