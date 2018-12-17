/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package producer

import (
	"fmt"
	"time"

	"github.com/XiaoMi/talos-sdk-golang/talos/thrift/message"
	"github.com/XiaoMi/talos-sdk-golang/talos/thrift/topic"
	"github.com/alecthomas/log4go"
)

type Sender interface {
	AddMessage(userMessageList []*UserMessage)
	Shutdown()
	MessageCallbackTask(userMessageResult *UserMessageResult)
	MessageWriterTask()
}

type PartitionSender struct {
	partitionId           int32
	requestId             *int64
	clientId              string
	talosProducerConfig   *TalosProducerConfig
	messageClient         message.MessageService
	userMessageCallback   UserMessageCallback
	topicAndPartition     *topic.TopicAndPartition
	partitionMessageQueue *PartitionMessageQueue
	talosProducer         Producer
	globalLock            chan LockState
	userMessageResult     *UserMessageResult
	simpleProducer        *SimpleProducer
	MessageWriterStopSign chan StopSignType
}

func NewPartitionSender(partitionId int32, topicName string,
	topicTalosResourceName *topic.TopicTalosResourceName, requestId *int64,
	clientId string, producerConfig *TalosProducerConfig,
	messageClient message.MessageService, userMessageCallback UserMessageCallback,
	globalLock chan LockState, talosProducer Producer) *PartitionSender {

	topicAndPartition := &topic.TopicAndPartition{
		TopicName:              topicName,
		TopicTalosResourceName: topicTalosResourceName,
		PartitionId:            partitionId}
	partitionMessageQueue := NewPartitionMessageQueue(producerConfig,
		partitionId, talosProducer)

	partitionSender := &PartitionSender{
		partitionId:           partitionId,
		requestId:             requestId,
		clientId:              clientId,
		talosProducerConfig:   producerConfig,
		messageClient:         messageClient,
		userMessageCallback:   userMessageCallback,
		globalLock:            globalLock,
		talosProducer:         talosProducer,
		topicAndPartition:     topicAndPartition,
		partitionMessageQueue: partitionMessageQueue,
		MessageWriterStopSign: make(chan StopSignType, 1),
	}

	go partitionSender.MessageWriterTask()

	return partitionSender
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
		s.topicAndPartition, s.messageClient, s.clientId, s.requestId)
	for true {
		select {
		case <-s.MessageWriterStopSign:
			log4go.Info("MessageWriterTask stop")
			return
		default:
			messageList := s.partitionMessageQueue.GetMessageList()

			// when messageList return no message, this means TalosProducer not
			// alive and there is no more message to send , then we should exit
			// write message right now;
			if len(messageList) == 0 {
				// notify to wake up producer's global lock
				s.globalLock <- NOTIFY
				break
			}
			err := s.putMessage(messageList)
			if err != nil {
				log4go.Error("PutMessageTask for topicAndPartition: %v error: %s",
					s.topicAndPartition, err.Error())
			}
			// s.globalLock <- NOTIFY
		}
	}
}

func (s *PartitionSender) putMessage(messageList []*message.Message) error {
	userMessageResult := NewUserMessageResult(messageList, s.partitionId)

	// when TalosProducer is disabled, we just fail the message and inform user;
	// but when TalosProducer is shutdown, we will send the left message.
	if s.talosProducer.IsDisable() {
		return fmt.Errorf("The Topic: %s with resourceName: %s no longer exist. "+
			"Please check the topic and reconstruct the TalosProducer again. ",
			s.topicAndPartition.GetTopicName(),
			s.topicAndPartition.GetTopicTalosResourceName())
	}

	if err := s.simpleProducer.doPut(messageList); err != nil {
		log4go.Error("Failed to put %d messages for partition: %d",
			len(messageList), s.partitionId)
		for _, msg := range messageList {
			log4go.Error("%d: %s", msg.GetSequenceNumber(), string(msg.GetMessage()))
		}
		// putMessage failed callback
		userMessageResult.SetSuccessful(false).SetCause(err)
		s.MessageCallbackTask(userMessageResult)

		// delay when partitionNotServing
		// TODO: judge utils.IsPartitionNotServing
		//log4go.Warn("Partition: %d is not serving state, sleep a while for waiting it work.", s.partitionId)
		time.Sleep(time.Duration(s.talosProducerConfig.GetWaitPartitionWorkingTime()))
		return err
	}
	// putMessage success callback
	userMessageResult.SetSuccessful(true)
	s.MessageCallbackTask(userMessageResult)
	log4go.Debug("put %d messages for partition: %d", len(messageList), s.partitionId)
	return nil
}

func (s *PartitionSender) AddMessage(userMessageList []*UserMessage) {
	s.partitionMessageQueue.MqWg.Add(1)
	s.partitionMessageQueue.AddMessage(userMessageList)
	log4go.Debug("add %d messages to partition: %d",
		len(userMessageList), s.partitionId)
}

func (s *PartitionSender) Shutdown() {
	// notify PartitionMessageQueue::getMessageList return;
	if s.partitionMessageQueue.userMessageList.Len() > 0 {
		s.AddMessage(make([]*UserMessage, 0))
	}
	s.MessageWriterStopSign <- Shutdown
	log4go.Info("PartitionSender for partition: %d finish stop", s.partitionId)
}
