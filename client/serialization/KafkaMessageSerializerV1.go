/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 */

package serialization

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/XiaoMi/talos-sdk-golang/thrift/message"
)

// KafkaMessageSerializerV1 deserializes a Kafka RecordBatch (magic v2) that was
// written into a Talos MessageBlock by the kafka-on-talos proxy. It mirrors the
// Java implementation com.xiaomi.infra.galaxy.talos.client.serialization.KafkaMessageSerializerV1.
//
// The MessageBlock payload layout is:
//
//	[ 'K', 0, 0, 0 ]        4-byte version header (KAFKA_IDENTIFIER)
//	[ 4-byte length ]       RecordBatch length
//	[ Kafka RecordBatch ]   magic v2 batch, records are varint-encoded
const (
	kafkaMessageHeaderLength = 8 // 'K' version header (4) + length field (4)

	// Offsets are relative to the RecordBatch start, which begins right after
	// the 8-byte kafka message header.
	kafkaLengthOffset          = kafkaMessageHeaderLength + 8      // after baseOffset(8)
	kafkaAttributesOffset      = kafkaLengthOffset + 4 + 4 + 1 + 4 // length(4)+partitionLeaderEpoch(4)+magic(1)+crc(4)
	kafkaLastOffsetDeltaOffset = kafkaAttributesOffset + 2
	kafkaFirstTimestampOffset  = kafkaLastOffsetDeltaOffset + 4
	kafkaMaxTimestampOffset    = kafkaFirstTimestampOffset + 8
	kafkaProducerIDOffset      = kafkaMaxTimestampOffset + 8
	kafkaProducerEpochOffset   = kafkaProducerIDOffset + 8
	kafkaBaseSequenceOffset    = kafkaProducerEpochOffset + 2
	kafkaRecordsCountOffset    = kafkaBaseSequenceOffset + 4
	kafkaRecordsOffset         = kafkaRecordsCountOffset + 4

	kafkaCompressionCodecMask = 0x07
)

func DeserializeKafkaMessageBlock(messageBlockData []byte,
	startMessageOffset int64) ([]*message.MessageAndOffset, error) {

	if len(messageBlockData) < kafkaRecordsOffset {
		return nil, fmt.Errorf("kafka message block too short: %d bytes", len(messageBlockData))
	}

	firstTimestamp := int64(binary.BigEndian.Uint64(
		messageBlockData[kafkaFirstTimestampOffset : kafkaFirstTimestampOffset+8]))
	attributes := int16(binary.BigEndian.Uint16(
		messageBlockData[kafkaAttributesOffset : kafkaAttributesOffset+2]))
	compressionId := attributes & kafkaCompressionCodecMask
	recordsCount := int32(binary.BigEndian.Uint32(
		messageBlockData[kafkaRecordsCountOffset : kafkaRecordsCountOffset+4]))
	baseSequence := int32(int8(messageBlockData[kafkaBaseSequenceOffset]))

	if compressionId != 0 {
		return nil, fmt.Errorf("unsupported compression type in kafka message block, " +
			"please use NONE compression in producer")
	}

	if recordsCount < 0 {
		return nil, fmt.Errorf("invalid kafka records count: %d", recordsCount)
	}

	messageAndOffsetList := make([]*message.MessageAndOffset, 0, recordsCount)
	messagePos := kafkaRecordsOffset
	for i := int32(0); i < recordsCount; i++ {
		if messagePos >= len(messageBlockData) {
			return nil, fmt.Errorf("kafka message block truncated at record %d", i)
		}
		// each record body is a self-describing varint-prefixed chunk
		recordBuf := bytes.NewBuffer(messageBlockData[messagePos:])

		sizeOfBody, err := ReadVarint(recordBuf)
		if err != nil {
			return nil, err
		}
		if sizeOfBody < 0 {
			return nil, fmt.Errorf("invalid kafka record body size %d at record %d", sizeOfBody, i)
		}
		totalSizeInBytes := SizeOfVarint(sizeOfBody) + int(sizeOfBody)
		// guard against a non-advancing or out-of-range step
		if totalSizeInBytes <= 0 || messagePos+totalSizeInBytes > len(messageBlockData) {
			return nil, fmt.Errorf("kafka record %d overruns block: pos=%d size=%d len=%d",
				i, messagePos, totalSizeInBytes, len(messageBlockData))
		}

		// attribute (1 byte, unused)
		if _, err = recordBuf.ReadByte(); err != nil {
			return nil, err
		}

		timestampDelta, err := ReadVarlong(recordBuf)
		if err != nil {
			return nil, err
		}
		timestamp := firstTimestamp + timestampDelta

		offsetDelta, err := ReadVarint(recordBuf)
		if err != nil {
			return nil, err
		}
		offset := startMessageOffset + int64(offsetDelta)

		keySize, err := ReadVarint(recordBuf)
		if err != nil {
			return nil, err
		}
		// -1 means null key; anything below that is corrupt.
		if keySize < -1 {
			return nil, fmt.Errorf("invalid kafka record %d key size: %d", i, keySize)
		}
		partitionKey := ""
		if keySize >= 0 {
			if int(keySize) > recordBuf.Len() {
				return nil, fmt.Errorf("kafka record %d key size %d exceeds remaining %d",
					i, keySize, recordBuf.Len())
			}
			keyBytes := make([]byte, keySize)
			if _, err = readFull(recordBuf, keyBytes); err != nil {
				return nil, err
			}
			partitionKey = string(keyBytes)
		}

		valueSize, err := ReadVarint(recordBuf)
		if err != nil {
			return nil, err
		}
		// -1 means null value; anything below that is corrupt.
		if valueSize < -1 {
			return nil, fmt.Errorf("invalid kafka record %d value size: %d", i, valueSize)
		}
		var value []byte
		if valueSize >= 0 {
			if int(valueSize) > recordBuf.Len() {
				return nil, fmt.Errorf("kafka record %d value size %d exceeds remaining %d",
					i, valueSize, recordBuf.Len())
			}
			value = make([]byte, valueSize)
			if _, err = readFull(recordBuf, value); err != nil {
				return nil, err
			}
		}

		msg := message.NewMessage()
		msg.Message = value
		msg.PartitionKey = &partitionKey
		msg.CreateTimestamp = &timestamp
		appendTimestamp := timestamp
		msg.AppendTimestamp = &appendTimestamp
		sequenceNumber := fmt.Sprintf("%d", baseSequence+i)
		msg.SequenceNumber = &sequenceNumber

		messageAndOffset := message.NewMessageAndOffset()
		messageAndOffset.Message = msg
		messageAndOffset.MessageOffset = offset
		messageAndOffsetList = append(messageAndOffsetList, messageAndOffset)

		// skip to the next record (past any trailing record headers)
		messagePos += totalSizeInBytes
	}

	return messageAndOffsetList, nil
}

// readFull reads exactly len(p) bytes from the buffer.
func readFull(buffer *bytes.Buffer, p []byte) (int, error) {
	n, err := buffer.Read(p)
	if err != nil {
		return n, err
	}
	if n != len(p) {
		return n, fmt.Errorf("short read: expected %d bytes, got %d", len(p), n)
	}
	return n, nil
}
