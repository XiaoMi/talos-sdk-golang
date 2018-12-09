/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package producer

import (
	"strconv"
	"testing"

	"../mock_producer"
	"github.com/XiaoMi/talos-sdk-golang/talos/client"
	"github.com/XiaoMi/talos-sdk-golang/talos/producer"
	"github.com/XiaoMi/talos-sdk-golang/talos/thrift/message"
	"github.com/XiaoMi/talos-sdk-golang/talos/utils"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

var msgStr string
var msg1 *message.Message
var partitionId int32
var maxPutMsgNumber int64
var maxBufferedMillSecs int64
var producerConfig *producer.TalosProducerConfig
var talosProducer *producer.TalosProducer
var partitionMessageQueue *producer.PartitionMessageQueue
var userMessage1, userMessage2, userMessage3 *producer.UserMessage
var userMessageList []*producer.UserMessage
var messageList []*message.Message

func setUpBefore() {
	msgStr = "hello, partitionMessageQueueTest"
	msg1 = &message.Message{
		Message: []byte(msgStr),
	}
	partitionId = 7
	maxPutMsgNumber = 3
	maxBufferedMillSecs = 200

	properties := utils.NewProperties()
	properties.SetProperty(producer.GALAXY_TALOS_PRODUCER_MAX_PUT_MESSAGE_NUMBER,
		strconv.FormatInt(maxPutMsgNumber, 10))
	properties.SetProperty(producer.GALAXY_TALOS_PRODUCER_MAX_BUFFERED_MILLI_SECS,
		strconv.FormatInt(maxBufferedMillSecs, 10))
	properties.SetProperty(client.GALAXY_TALOS_SERVICE_ENDPOINT, "testURI")
	producerConfig = producer.NewTalosProducerConfigByProperties(properties)

	userMessage1 = producer.NewUserMessage(msg1)
	userMessage2 = producer.NewUserMessage(msg1)
	userMessage3 = producer.NewUserMessage(msg1)

}

func TestPartitionMessageQueue(t *testing.T) {
	setUpBefore()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockProducer := mock_producer.NewMockProducer(ctrl)

	partitionMessageQueue = producer.NewPartitionMessageQueue(producerConfig, partitionId, mockProducer)
	userMessageList = make([]*producer.UserMessage, 0)
	userMessageList = append(userMessageList, userMessage1)
	userMessageList = append(userMessageList, userMessage2)
	userMessageList = append(userMessageList, userMessage3)

	messageList = make([]*message.Message, 0)
	messageList = append(messageList, msg1)
	messageList = append(messageList, msg1)
	messageList = append(messageList, msg1)

	mockProducer.EXPECT().IncreaseBufferedCount(3, len(msgStr)*3).Do(nil)
	mockProducer.EXPECT().IsActive().Return(true)
	mockProducer.EXPECT().DecreaseBufferedCount(3, len(msgStr)*3).Do(nil)

	partitionMessageQueue.AddMessage(userMessageList)
	assert.Equal(t, len(messageList), len(partitionMessageQueue.GetMessageList()))

}
