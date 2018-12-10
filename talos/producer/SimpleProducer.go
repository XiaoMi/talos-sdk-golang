/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package producer

import (
	"fmt"
	"time"

	"github.com/XiaoMi/talos-sdk-golang/talos/client"
	"github.com/XiaoMi/talos-sdk-golang/talos/client/compression"
	"github.com/XiaoMi/talos-sdk-golang/talos/thrift/auth"
	"github.com/XiaoMi/talos-sdk-golang/talos/thrift/common"
	"github.com/XiaoMi/talos-sdk-golang/talos/thrift/message"
	"github.com/XiaoMi/talos-sdk-golang/talos/thrift/topic"
	"github.com/XiaoMi/talos-sdk-golang/talos/utils"
	"github.com/XiaoMi/talos-sdk-golang/thrift"
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

func NewSimpleProducerDefault(producerConfig *TalosProducerConfig,
	topicAndPartition *topic.TopicAndPartition, credential *auth.Credential) *SimpleProducer {

	socketTimeout := common.GALAXY_TALOS_CLIENT_ADMIN_TIMEOUT_MILLI_SECS_DEFAULT * time.Second
	requestId, _ := utils.CheckAndGenerateClientId("SimpleProducer")
	clientFactory := client.NewTalosClientFactory(producerConfig.TalosClientConfig,
		credential, socketTimeout)
	messageClient := clientFactory.NewMessageClientDefault()
	return NewSimpleProducer(producerConfig, topicAndPartition,
		messageClient, requestId, thrift.Int64Ptr(1))
}

func NewSimpleProducer(producerConfig *TalosProducerConfig,
	topicAndPartition *topic.TopicAndPartition,
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
	if len(msgList) == 0 {
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

	if err := p.doPut(msgList); err != nil {
		log4go.Error("doPut message error: %s", err.Error())
		return err
	}
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

	requestSequenceId, err := utils.GenerateRequestSequenceId(p.clientId, p.requestId)
	if err != nil {
		log4go.Error("generate RequestSequenceId error: %s", err.Error())
		return err
	}
	putMessageRequest := &message.PutMessageRequest{
		TopicAndPartition: p.topicAndPartition,
		MessageBlocks:     messageBlockList,
		MessageNumber:     int32(len(msgList)),
		SequenceId:        requestSequenceId,
	}
	timestamp := utils.CurrentTimeMills() + p.producerConfig.ClientTimeout()
	putMessageRequest.TimeoutTimestamp = &timestamp
	_, err = p.messageClient.PutMessage(putMessageRequest)
	if err != nil {
		log4go.Error("putMessage error: %s", err.Error())
		return err
	}
	// TODO: add auto location feature
	return nil
}

func (p *SimpleProducer) compressMessageList(
	msgList []*message.Message) (*message.MessageBlock, error) {
	return compression.Compress(msgList, p.producerConfig.GetCompressionType())
}
