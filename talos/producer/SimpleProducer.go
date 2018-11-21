/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package producer

import (
	"github.com/XiaoMi/talos-sdk-golang/talos/client"
	"github.com/XiaoMi/talos-sdk-golang/talos/thrift/message"
	"github.com/XiaoMi/talos-sdk-golang/talos/thrift/topic"
	//	"github.com/XiaoMi/talos-sdk-golang/talos/utils"
	"../utils"
	"fmt"
	"github.com/XiaoMi/talos-sdk-golang/talos/client/compression"
	"github.com/alecthomas/log4go"
)

type SimpleProducer struct {
	producerConfig    *TalosProducerConfig
	topicAndPartition *topic.TopicAndPartition
	messageClient     message.MessageService
	requestId         *int64
	clientId          string
	//scheduleInfoCache ScheduleInfoCache
}

func NewSimpleProducer(producerConfig *TalosProducerConfig,
	topicAndPartition *topic.TopicAndPartition, factory *client.TalosClientFactory,
	messageClient message.MessageService, clientId string,
	requestId *int64) *SimpleProducer {

	err := utils.CheckTopicAndPartition(topicAndPartition)
	if err != nil {
		log4go.Error("init simpleProducer error: %s", err.Error())
		return nil
	}
	return &SimpleProducer{
		producerConfig:    producerConfig,
		topicAndPartition: topicAndPartition,
		messageClient:     messageClient,
		clientId:          clientId,
		requestId:         requestId,
	}
}

func (p *SimpleProducer) PutMessage(msgList []*message.Message) bool {
	if msgList == nil || len(msgList) == 0 {
		return true
	}
	err := p.PutMessageList(msgList)
	if err != nil {
		log4go.Error("putMessage error: %s, please try to put again", err.Error())
		return false
	}
	return true
}

func (p *SimpleProducer) PutMessageList(msgList []*message.Message) error {
	if msgList == nil || len(msgList) == 0 {
		return fmt.Errorf("message list is nil")
	}

	//check data validity
	for _, msg := range msgList {
		utils.UpdateMessage(msg, message.MessageType_BINARY)
	}

	//check data validity
	if err := utils.CheckMessagesValidity(msgList); err != nil {
		log4go.Error("message data invalidity: %s", err.Error())
		return err
	}

	p.doPut(msgList)
	return nil
}

func (p *SimpleProducer) doPut(msgList []*message.Message) error {
	messageBlock, err := p.compressMessageList(msgList)
	if err != nil {
		log4go.Error("compress message list error: %s", err.Error())
		return err
	}
	messageBlockList := make([]*message.MessageBlock, 0, 1)
	messageBlockList = append(messageBlockList, messageBlock)

	requestSequenceId, err := utils.GenerateRequestSequenceId(p.clientId, *p.requestId)
	if err != nil {
		log4go.Error("generate RequestSequenceId error: %s", err.Error())
		return err
	}
	timestamp := utils.CurrentTimeMills() + p.producerConfig.ClientTimeout()
	putMessageRequest := &message.PutMessageRequest{
		TopicAndPartition: p.topicAndPartition,
		MessageBlocks:     messageBlockList,
		MessageNumber:     int32(len(msgList)),
		SequenceId:        requestSequenceId,
		TimeoutTimestamp:  &timestamp,
	}
	_, err = p.messageClient.PutMessage(putMessageRequest)
	if err != nil {
		log4go.Error("putMessage error:%s", err.Error())
		return err
	}
	// TODO: add auto location feature
	return nil
}

func (p *SimpleProducer) compressMessageList(
	msgList []*message.Message) (*message.MessageBlock, error) {
	return compression.Compress(msgList, p.producerConfig.GetCompressionType())
}
