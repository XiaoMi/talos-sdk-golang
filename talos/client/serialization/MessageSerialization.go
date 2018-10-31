/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package serialization

import (
	"bytes"

	"github.com/XiaoMi/talos-sdk-golang/talos/thrift/message"
	log "github.com/alecthomas/log4go"
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
		log.Error("serialize messageData error: %s", err.Error())
		return err
	}
	return nil
}

func (s *MessageSerialization) DeserializeMessage(buffer *bytes.Buffer) (*message.Message, error) {

	header := make([]byte, VERSION_NUMBER_LENGTH)
	buffer.Read(header)
	messageVersion, err := DecodeMessageVersionNumber(header)
	if err != nil {
		log.Error("Deserialize message error: %s", err.Error())
		return nil, err
	}
	serializer, err := NewMessageSerializationFactory().
		GetMessageSerializer(messageVersion)
	if err != nil {
		log.Error("get messageSerializer error: %s", err.Error())
		return nil, err
	}
	message, err := serializer.Deserialize(header, buffer)
	if err != nil {
		log.Error("Deserialize message error: %s", err.Error())
		return nil, err
	}
	return message, nil
}

func (s *MessageSerialization) GetMessageSize(message *message.Message,
	version MessageVersion) int {
	serializer, _ := NewMessageSerializationFactory().GetMessageSerializer(version)
	size, _ := serializer.GetMessageSize(message)
	return size
}

func GetDefaultMessageVersion() MessageVersion {
	return V3
}
