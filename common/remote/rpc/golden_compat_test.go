// common/remote/rpc/golden_compat_test.go
package rpc

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nacos-group/nacos-sdk-go/v3/common/remote/codec"
	"github.com/nacos-group/nacos-sdk-go/v3/common/remote/rpc/rpc_request"
)

var _ = []codec.ProtoConvertible{ // 编译期断言：三个迁移请求实现接口
	(*rpc_request.HealthCheckRequest)(nil),
	(*rpc_request.ServerCheckRequest)(nil),
	(*rpc_request.ConnectionSetupRequest)(nil),
}

// goldenCompare 断言迁移消息的 protojson 输出与旧 struct JSON 字段级等价。
// legacyOnly/protoOnly 是实证核对过的已知差异字段（见 Global Constraints）。
func goldenCompare(t *testing.T, req rpc_request.IRequest, legacyOnly, protoOnly []string) {
	t.Helper()
	legacy := map[string]interface{}{}
	require.NoError(t, json.Unmarshal([]byte(req.GetBody(req)), &legacy))

	pc := req.(codec.ProtoConvertible)
	payload, err := codec.NewPayloadCodec().Encode(req.GetRequestType(), pc.ProtoMessage(), req.GetHeaders(), "1.2.3.4")
	require.NoError(t, err)
	protoM := map[string]interface{}{}
	require.NoError(t, json.Unmarshal(payload.GetBody().GetValue(), &protoM))

	for _, k := range legacyOnly {
		_, ok := legacy[k]
		assert.True(t, ok, "expected legacy-only field %q missing from legacy json", k)
		delete(legacy, k)
	}
	for _, k := range protoOnly {
		delete(protoM, k)
	}
	assert.Equal(t, legacy, protoM, "wire body must be field-level equivalent")
}

func TestGoldenHealthCheckRequest(t *testing.T) {
	goldenCompare(t, rpc_request.NewHealthCheckRequest(), []string{"module"}, nil)
}

func TestGoldenServerCheckRequest(t *testing.T) {
	goldenCompare(t, rpc_request.NewServerCheckRequest(), []string{"module"}, nil)
}

func TestGoldenConnectionSetupRequest(t *testing.T) {
	req := rpc_request.NewConnectionSetupRequest()
	req.ClientVersion = "Nacos-Go-Client:v3.0.0"
	req.Tenant = "public"
	req.Labels = map[string]string{"module": "config", "source": "sdk"}
	// module: 服务端派生字段；clientAbilities→abilityTable: Java 3.x 字段更名，
	// 行为等价性由 2.x/3.x 集成测试的连接握手证明
	goldenCompare(t, req, []string{"module", "clientAbilities"}, []string{"abilityTable"})
}
