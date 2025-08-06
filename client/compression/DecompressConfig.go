package compression

// Define the decoder configuration interface
type DecompressConfig interface {
	IsZstdCompressedWithPureGo() bool
	GetZstdDecoderConcurrency() int64
}
