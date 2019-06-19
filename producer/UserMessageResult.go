/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package producer

import (
	"github.com/XiaoMi/talos-sdk-golang/thrift/message"
)

type UserMessageResult struct {
	messageList []*message.Message
	partitionId int32
	successful  bool
	cause       error
}

func NewUserMessageResult(messageList []*message.Message, partitionId int32) *UserMessageResult {
	return &UserMessageResult{
		messageList: messageList,
		partitionId: partitionId,
		successful:  false,
		cause:       nil,
	}
}

func (r *UserMessageResult) SetSuccessful(success bool) *UserMessageResult {
	r.successful = success
	return r
}

func (r *UserMessageResult) SetCause(err error) *UserMessageResult {
	r.cause = err
	return r
}

func (r *UserMessageResult) GetPartitionId() int32 {
	return r.partitionId
}

func (r *UserMessageResult) IsSuccessful() bool {
	return r.successful
}

func (r *UserMessageResult) GetCause() error {
	return r.cause
}

func (r *UserMessageResult) GetMessageList() []*message.Message {
	return r.messageList
}
