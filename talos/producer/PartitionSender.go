/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package producer

import (
	"github.com/XiaoMi/talos-sdk-golang/talos/thrift/message"
	"github.com/XiaoMi/talos-sdk-golang/talos/thrift/topic"
	"github.com/alecthomas/log4go"
	"sync"
)

type PartitionSender struct {
	partitionId           int32
	requestId             *int64
	clientId              string
	talosProducerConfig   *TalosProducerConfig
	messageClient         message.MessageService
	userMessageCallback   UserMessageCallback
	topicAndPartition     *topic.TopicAndPartition
	partitionMessageQueue *PartitionMessageQueue
	talosProducer         *TalosProducer
	mutex                 sync.Mutex
	userMessageResult     *UserMessageResult
	simpleProducer        *SimpleProducer
}

func NewPartitionSender(partitionId int32, topicName string,
	topicTalosResourceName *topic.TopicTalosResourceName, requestId *int64,
	clientId string, producerConfig *TalosProducerConfig,
	messageClient message.MessageService, userMessageCallback UserMessageCallback,
	mut sync.Mutex, talosProducer *TalosProducer) *PartitionSender {

	topicAndPartition := &topic.TopicAndPartition{
		TopicName:              topicName,
		TopicTalosResourceName: topicTalosResourceName,
		PartitionId:            partitionId}
	partitionMessageQueue := NewPartitionMessageQueue(producerConfig,
		partitionId, talosProducer)
	go MessageWriterTask()

	return &PartitionSender{
		partitionId:           partitionId,
		requestId:             requestId,
		clientId:              clientId,
		talosProducerConfig:   producerConfig,
		messageClient:         messageClient,
		userMessageCallback:   userMessageCallback,
		mutex:                 mut,
		talosProducer:         talosProducer,
		topicAndPartition:     topicAndPartition,
		partitionMessageQueue: partitionMessageQueue,
	}
}

func (s *PartitionSender) AddMessage(userMessageList []UserMessage) {
	s.partitionMessageQueue.AddMessage(userMessageList)
	log4go.Debug("add %d messages to partition: %d",
		len(userMessageList), s.partitionId)
}

func (s *PartitionSender) Shutdown() {
	// notify PartitionMessageQueue::getMessageList return;
	s.AddMessage(make([]UserMessage, 0))

}

func (s *PartitionSender) MessageCallbackTask(userMessageResult *UserMessageResult) {
  s.userMessageResult = userMessageResult
	if s.userMessageResult.IsSuccessful() {
    s.userMessageCallback.OnSuccess(s.userMessageResult)
	} else {
    s.userMessageCallback.OnError(s.userMessageResult)
  }
}

func (s *PartitionSender) MessageWriterTask() {
  s.simpleProducer = NewSimpleProducer(s.talosProducerConfig,
    s.topicAndPartition, nil, s.messageClient, s.clientId, s.requestId)
  for true {
    messageList := s.partitionMessageQueue
  }
}
