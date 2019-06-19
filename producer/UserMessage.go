/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package producer

import (
	"github.com/XiaoMi/talos-sdk-golang/thrift/message"
	"github.com/XiaoMi/talos-sdk-golang/utils"
)

type UserMessage struct {
	message     *message.Message
	timestamp   int64
	messageSize int64
}

func NewUserMessage(msg *message.Message) *UserMessage {
	messageSize := int64(len(msg.GetMessage()))
	if len(msg.GetSequenceNumber()) > 0 {
		messageSize += int64(len(msg.GetSequenceNumber()))
	}
	return &UserMessage{
		message:     msg,
		timestamp:   utils.CurrentTimeMills(),
		messageSize: messageSize,
	}
}

func (m *UserMessage) GetMessage() *message.Message {
	return m.message
}

func (m *UserMessage) GetTimestamp() int64 {
	return m.timestamp
}

func (m *UserMessage) GetMessageSize() int64 {
	return m.messageSize
}
