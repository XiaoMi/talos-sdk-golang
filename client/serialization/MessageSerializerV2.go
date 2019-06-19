/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package serialization

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/XiaoMi/talos-sdk-golang/thrift/message"
	"github.com/XiaoMi/talos-sdk-golang/utils"

	log "github.com/sirupsen/logrus"
)

const (
	CREATE_TIMESTAMP_BYTES_V2       int = 8
	MESSAGE_TYPE_BYTES_V2           int = 2
	SEQUENCE_NUMBER_LENGTH_BYTES_V2 int = 2
	VERSION_NUMBER_LENGTH_V2        int = 4
	MESSAGE_DATA_LENGTH_BYTES_V2    int = 4
	MESSAGE_HEADER_BYTES_V2             = VERSION_NUMBER_LENGTH_V2 +
		MESSAGE_TYPE_BYTES_V2 + CREATE_TIMESTAMP_BYTES_V2 +
		SEQUENCE_NUMBER_LENGTH_BYTES_V2 + MESSAGE_DATA_LENGTH_BYTES_V2
)

type MessageSerializerV2 struct {
	version MessageVersion
}

func NewMessageSerializerV2() *MessageSerializerV2 {
	return &MessageSerializerV2{version: V2}
}

func (s *MessageSerializerV2) Serialize(msg *message.Message, buf *bytes.Buffer) error {
	// write version number;
	WriteMessageVersion(V2, buf)

	// write createTimeStamp
	timestampBuffer := make([]byte, CREATE_TIMESTAMP_BYTES_V2)
	if msg.IsSetCreateTimestamp() {
		binary.BigEndian.PutUint64(timestampBuffer,
			uint64(msg.GetCreateTimestamp()))
	} else {
		binary.BigEndian.PutUint64(timestampBuffer,
			uint64(utils.CurrentTimeMills()))
	}
	buf.Write(timestampBuffer)

	// write messageType
	if !msg.IsSetMessageType() {
		return fmt.Errorf("message must set messageType")
	}
	messageTypeBuffer := make([]byte, MESSAGE_TYPE_BYTES_V2)
	binary.BigEndian.PutUint16(messageTypeBuffer, uint16(msg.GetMessageType()))
	buf.Write(messageTypeBuffer)

	//write sequenceNumber
	sequenceNumberBuffer := make([]byte, SEQUENCE_NUMBER_LENGTH_BYTES_V2)
	if msg.IsSetSequenceNumber() {
		binary.BigEndian.PutUint16(sequenceNumberBuffer,
			uint16(len(msg.GetSequenceNumber())))
		buf.Write(sequenceNumberBuffer)
		buf.Write([]byte(msg.GetSequenceNumber()))
	} else {
		binary.BigEndian.PutUint16(sequenceNumberBuffer,
			uint16(0))
		buf.Write(sequenceNumberBuffer)
	}

	//write message size
	sizeBuffer := make([]byte, MESSAGE_DATA_LENGTH_BYTES_V2)
	binary.BigEndian.PutUint32(sizeBuffer, uint32(len(msg.GetMessage())))
	buf.Write(sizeBuffer)

	//write message
	buf.Write(msg.GetMessage())
	return nil
}

func (s *MessageSerializerV2) Deserialize(header []byte, buf *bytes.Buffer) (*message.Message, error) {
	msg := message.NewMessage()
	// read createTimestamp;
	timestampBuffer := make([]byte, CREATE_TIMESTAMP_BYTES_V2)
	n, err := buf.Read(timestampBuffer)
	if n != CREATE_TIMESTAMP_BYTES_V2 || err != nil {
		log.Errorf("read create timestamp error: %s", err.Error())
		return nil, err
	}
	timestamp := int64(binary.BigEndian.Uint64(timestampBuffer))

	// read message type
	messageTypeBuffer := make([]byte, MESSAGE_TYPE_BYTES_V2)
	n, err = buf.Read(messageTypeBuffer)
	if n != MESSAGE_TYPE_BYTES_V2 || err != nil {
		log.Errorf("read message type error: %s", err.Error())
		return nil, err
	}
	messageType := message.MessageType(binary.BigEndian.Uint16(messageTypeBuffer))

	// read sequence number
	sequenceNumLenBuffer := make([]byte, SEQUENCE_NUMBER_LENGTH_BYTES_V2)
	n, err = buf.Read(sequenceNumLenBuffer)
	if n != SEQUENCE_NUMBER_LENGTH_BYTES_V2 || err != nil {
		log.Errorf("read sequence number length error: %s", err.Error())
		return nil, err
	}
	sequenceNumLen := binary.BigEndian.Uint16(sequenceNumLenBuffer)
	var sequenceNumber string
	if sequenceNumLen != 0 {
		sequenceNumberBuffer := make([]byte, sequenceNumLen)
		n, err = buf.Read(sequenceNumberBuffer)
		if n != int(sequenceNumLen) || err != nil {
			log.Errorf("read sequence number error: %s", err.Error())
			return nil, err
		}
		sequenceNumber = string(sequenceNumberBuffer)
		msg.SequenceNumber = &sequenceNumber
	}

	//read message size
	messageSizeBuffer := make([]byte, MESSAGE_DATA_LENGTH_BYTES_V2)
	n, err = buf.Read(messageSizeBuffer)
	if n != MESSAGE_DATA_LENGTH_BYTES_V2 || err != nil {
		log.Errorf("read message size error: %s", err.Error())
		return nil, err
	}
	messageSize := binary.BigEndian.Uint32(messageSizeBuffer)

	//read message
	var messageDataBuffer = make([]byte, messageSize)
	n, err = buf.Read(messageDataBuffer)
	if n != int(messageSize) || err != nil {
		log.Errorf("read message data error: %s", err.Error())
		return nil, err
	}

	msg.CreateTimestamp = &timestamp
	msg.MessageType = &messageType
	msg.Message = messageDataBuffer

	return msg, nil
}

func (s *MessageSerializerV2) GetMessageSize(msg *message.Message) (int, error) {
	size := MESSAGE_HEADER_BYTES_V2
	if msg.IsSetSequenceNumber() {
		size += len(msg.GetSequenceNumber())
	}
	size += len(msg.GetMessage())
	return size, nil
}
