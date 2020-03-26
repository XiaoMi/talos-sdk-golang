/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package serialization

import (
	"bytes"
	"encoding/binary"

	"github.com/XiaoMi/talos-sdk-golang/thrift/message"
	"github.com/XiaoMi/talos-sdk-golang/utils"
)

const (
	MESSAGE_HEADER_BYTES_V3      int = 8
	VERSION_NUMBER_LENGTH_V3     int = 4
	MESSAGE_DATA_LENGTH_BYTES_V3 int = 4
)

type MessageSerializerV3 struct {
	version MessageVersion
}

func NewMessageSerializerV3() *MessageSerializerV3 {
	return &MessageSerializerV3{version: V3}
}

func (s *MessageSerializerV3) Serialize(msg *message.Message, buf *bytes.Buffer) error {
	// write version number;
	WriteMessageVersion(V3, buf)

	// serialize message;
	messageData, err := utils.Serialize(msg)
	if err != nil {
		return err
	}
	//write message size
	sizeBuffer := make([]byte, MESSAGE_DATA_LENGTH_BYTES_V3)
	binary.BigEndian.PutUint32(sizeBuffer, uint32(len(messageData)))
	buf.Write(sizeBuffer)
	//write message
	buf.Write(messageData)
	return nil
}

func (s *MessageSerializerV3) Deserialize(header []byte, buf *bytes.Buffer) (*message.Message, error) {
	//read message size
	messageSizeBuffer := make([]byte, MESSAGE_DATA_LENGTH_BYTES_V3)
	n, err := buf.Read(messageSizeBuffer)
	if n != MESSAGE_DATA_LENGTH_BYTES_V3 || err != nil {
		return nil, err
	}
	messageSize := binary.BigEndian.Uint32(messageSizeBuffer)
	//read message
	var messageData = make([]byte, messageSize)
	buf.Read(messageData)

	msg, e := utils.Deserialize(messageData)
	if e != nil {
		return nil, e
	}
	return msg, nil
}

func (s *MessageSerializerV3) GetMessageSize(msg *message.Message) (int, error) {
	messageData, err := utils.Serialize(msg)
	if err != nil {
		return 0, err
	}
	return MESSAGE_HEADER_BYTES_V3 + len(messageData), nil
}
