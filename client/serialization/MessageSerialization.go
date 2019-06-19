/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package serialization

import (
	"bytes"

	"github.com/XiaoMi/talos-sdk-golang/thrift/message"

	log "github.com/sirupsen/logrus"
)

type MessageSerialization struct {
}

func NewMessageSerialization() *MessageSerialization {
	return &MessageSerialization{}
}

func (s *MessageSerialization) SerializeMessage(message *message.Message,
	buffer *bytes.Buffer, version MessageVersion) error {

	serializer, err := NewMessageSerializationFactory().
		GetMessageSerializer(version)
	if err != nil {
		log.Error(err)
		return err
	}
	err = serializer.Serialize(message, buffer)
	if err != nil {
		log.Errorf("serialize messageData error: %s", err.Error())
		return err
	}
	return nil
}

func (s *MessageSerialization) DeserializeMessage(buffer *bytes.Buffer) (*message.Message, error) {

	header := make([]byte, VERSION_NUMBER_LENGTH_V3)
	buffer.Read(header)
	messageVersion, err := DecodeMessageVersionNumber(header)
	if err != nil {
		log.Errorf("Deserialize message error: %s", err.Error())
		return nil, err
	}
	serializer, err := NewMessageSerializationFactory().
		GetMessageSerializer(messageVersion)
	if err != nil {
		log.Errorf("get messageSerializer error: %s", err.Error())
		return nil, err
	}
	msg, err := serializer.Deserialize(header, buffer)
	if err != nil {
		log.Errorf("Deserialize message error: %s", err.Error())
		return nil, err
	}
	return msg, nil
}

func (s *MessageSerialization) GetMessageSize(message *message.Message,
	version MessageVersion) int {
	serializer, _ := NewMessageSerializationFactory().GetMessageSerializer(version)
	size, _ := serializer.GetMessageSize(message)
	return size
}
