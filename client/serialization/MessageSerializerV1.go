/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package serialization

import (
	"bytes"
	"encoding/binary"

	"talos-sdk-golang/thrift/message"

	log "github.com/sirupsen/logrus"
)

const (
	MESSAGE_HEADER_BYTES_V1         int = 8
	SEQUENCE_NUMBER_LENGTH_BYTES_V1 int = 4
	MESSAGE_DATA_LENGTH_BYTES_V1    int = 4
)

type MessageSerializerV1 struct {
	version MessageVersion
}

func NewMessageSerializerV1() *MessageSerializerV1 {
	return &MessageSerializerV1{version: V1}
}

func (s *MessageSerializerV1) Serialize(msg *message.Message, buf *bytes.Buffer) error {
	//write sequenceNumber
	sequenceNumberBuffer := make([]byte, SEQUENCE_NUMBER_LENGTH_BYTES_V1)
	if msg.IsSetSequenceNumber() {
		binary.BigEndian.PutUint32(sequenceNumberBuffer,
			uint32(len(msg.GetSequenceNumber())))
		buf.Write(sequenceNumberBuffer)
		buf.Write([]byte(msg.GetSequenceNumber()))
	} else {
		binary.BigEndian.PutUint32(sequenceNumberBuffer,
			uint32(0))
		buf.Write(sequenceNumberBuffer)
	}

	//write message size
	sizeBuffer := make([]byte, MESSAGE_DATA_LENGTH_BYTES_V1)
	binary.BigEndian.PutUint32(sizeBuffer, uint32(len(msg.GetMessage())))
	buf.Write(sizeBuffer)

	//write message
	buf.Write(msg.GetMessage())
	return nil
}

func (s *MessageSerializerV1) Deserialize(header []byte, buf *bytes.Buffer) (*message.Message, error) {
	msg := message.NewMessage()
	// generate sequence number len
	sequenceNumLen := binary.BigEndian.Uint32(header)
	// read sequence number
	var sequenceNumber string
	if sequenceNumLen != 0 {
		sequenceNumberBuffer := make([]byte, sequenceNumLen)
		n, err := buf.Read(sequenceNumberBuffer)
		if n != int(sequenceNumLen) || err != nil {
			log.Errorf("read sequence number error: %s", err.Error())
			return nil, err
		}
		sequenceNumber = string(sequenceNumberBuffer)
		msg.SequenceNumber = &sequenceNumber
	}

	//read message size
	messageSizeBuffer := make([]byte, MESSAGE_DATA_LENGTH_BYTES_V1)
	n, err := buf.Read(messageSizeBuffer)
	if n != MESSAGE_DATA_LENGTH_BYTES_V1 || err != nil {
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

	msg.Message = messageDataBuffer

	return msg, nil
}

func (s *MessageSerializerV1) GetMessageSize(msg *message.Message) (int, error) {
	size := MESSAGE_HEADER_BYTES_V1
	if msg.IsSetSequenceNumber() {
		size += len(msg.GetSequenceNumber())
	}
	size += len(msg.GetMessage())
	return size, nil
}
