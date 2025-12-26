package compression

// Define the decoder configuration interface
type DecompressConfig interface {
	GetZstdDecoderConcurrency() int64
}
