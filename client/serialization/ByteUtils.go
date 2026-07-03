/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 */

package serialization

import (
	"bytes"
	"fmt"
)

// ReadVarint reads a zigzag-encoded variable-length int from the buffer,
// matching Kafka's org.apache.kafka.common.utils.ByteUtils#readVarint.
func ReadVarint(buffer *bytes.Buffer) (int32, error) {
	var value uint32
	var i uint
	for {
		b, err := buffer.ReadByte()
		if err != nil {
			return 0, err
		}
		if b&0x80 == 0 {
			value |= uint32(b) << i
			break
		}
		value |= uint32(b&0x7f) << i
		i += 7
		if i > 28 {
			return 0, fmt.Errorf("read varint failed: value too long")
		}
	}
	// zigzag decode
	return int32(value>>1) ^ -int32(value&1), nil
}

// ReadVarlong reads a zigzag-encoded variable-length long from the buffer,
// matching Kafka's org.apache.kafka.common.utils.ByteUtils#readVarlong.
func ReadVarlong(buffer *bytes.Buffer) (int64, error) {
	var value uint64
	var i uint
	for {
		b, err := buffer.ReadByte()
		if err != nil {
			return 0, err
		}
		if b&0x80 == 0 {
			value |= uint64(b) << i
			break
		}
		value |= uint64(b&0x7f) << i
		i += 7
		if i > 63 {
			return 0, fmt.Errorf("read varlong failed: value too long")
		}
	}
	// zigzag decode
	return int64(value>>1) ^ -int64(value&1), nil
}

// SizeOfVarint returns the number of bytes the zigzag-encoded value occupies,
// matching Kafka's org.apache.kafka.common.utils.ByteUtils#sizeOfVarint.
func SizeOfVarint(value int32) int {
	v := uint32((value << 1) ^ (value >> 31))
	bytesCount := 1
	for v&0xffffff80 != 0 {
		bytesCount++
		v >>= 7
	}
	return bytesCount
}
