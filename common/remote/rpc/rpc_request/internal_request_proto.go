// common/remote/rpc/rpc_request/internal_request_proto.go
package rpc_request

import (
	"github.com/nacos-group/nacos-sdk-proto/go/common"
	"google.golang.org/protobuf/proto"
)

// ProtoMessage implementations for the first batch of migrated requests
// (connection-layer internal messages). Field mapping is 1:1 with the
// legacy JSON body; `module` is a server-side derived field and does not
// exist in the proto definitions.

func (r *HealthCheckRequest) ProtoMessage() proto.Message {
	return &common.HealthCheckRequest{RequestId: r.RequestId}
}

func (r *ServerCheckRequest) ProtoMessage() proto.Message {
	return &common.ServerCheckRequest{RequestId: r.RequestId}
}

func (r *ConnectionSetupRequest) ProtoMessage() proto.Message {
	return &common.ConnectionSetupRequest{
		RequestId:     r.RequestId,
		ClientVersion: r.ClientVersion,
		Tenant:        r.Tenant,
		Labels:        r.Labels,
	}
}
