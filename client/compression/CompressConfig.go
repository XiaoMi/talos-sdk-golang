package compression

import "github.com/XiaoMi/talos-sdk-golang/thrift/message"

// Defines the configuration that the compression really needs to read. Avoid cyclic dependency issues.
type CompressConfig interface {
	GetCompressionType() message.MessageCompressionType
	GetCompressionLevel() int64
	GetZstdEncoderConcurrency() int64
	IsZstdCompressedWithPureGo() bool
}
