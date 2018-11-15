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
	"github.com/XiaoMi/talos-sdk-golang/talos/utils"
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
