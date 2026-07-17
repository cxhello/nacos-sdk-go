// common/remote/codec/proto_convertible.go
package codec

import "google.golang.org/protobuf/proto"

// ProtoConvertible is implemented by requests that have been migrated to
// nacos-sdk-proto message types. The payload type name still comes from
// IRequest.GetRequestType(); only the body carrier changes.
type ProtoConvertible interface {
	ProtoMessage() proto.Message
}
