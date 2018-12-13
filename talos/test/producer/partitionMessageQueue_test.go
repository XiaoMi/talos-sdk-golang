/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package producer

import (
	"strconv"
	"testing"

	"../../../talos/producer"
	"../mock_producer"
	"github.com/XiaoMi/talos-sdk-golang/talos/client"
	"github.com/XiaoMi/talos-sdk-golang/talos/thrift/message"
	"github.com/XiaoMi/talos-sdk-golang/talos/utils"
	"github.com/alecthomas/log4go"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

var msgStr string
var msg1 *message.Message
var partitionId int32
var maxPutMsgNumber int64
var maxBufferedMillSecs int64
var producerConfig *producer.TalosProducerConfig
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

func TestAddAndGetMessageWhenMaxPutNumber(t *testing.T) {
	setUpBefore()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockProducer := mock_producer.NewMockProducer(ctrl)
	partitionMessageQueue := producer.NewPartitionMessageQueueForTest(producerConfig, partitionId, mockProducer)

	userMessageList = make([]*producer.UserMessage, 0)
	userMessageList = append(userMessageList, userMessage1)
	userMessageList = append(userMessageList, userMessage2)
	userMessageList = append(userMessageList, userMessage3)

	messageList = make([]*message.Message, 0)
	messageList = append(messageList, msg1)
	messageList = append(messageList, msg1)
	messageList = append(messageList, msg1)

	// Test AddAndGetMessageWhenMaxPutNumber
	gomock.InOrder(
		mockProducer.EXPECT().IncreaseBufferedCount(int64(3), int64(len(msgStr)*3)).Times(1),
		mockProducer.EXPECT().IsActive().Return(true).Times(1),
		mockProducer.EXPECT().DecreaseBufferedCount(int64(3), int64(len(msgStr)*3)).Times(1),
	)

	partitionMessageQueue.MqWg.Add(1)
	partitionMessageQueue.AddMessage(userMessageList)
	assert.Equal(t, len(messageList), len(partitionMessageQueue.GetMessageList()))
}

func TestAddAndGetWaitMessageWhenNumberNotEnough(t *testing.T) {
	defer log4go.Close()
	setUpBefore()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockProducer := mock_producer.NewMockProducer(ctrl)
	partitionMessageQueue := producer.NewPartitionMessageQueueForTest(producerConfig, partitionId, mockProducer)

	userMessageList = make([]*producer.UserMessage, 0)
	userMessageList = append(userMessageList, userMessage1)
	userMessageList = append(userMessageList, userMessage2)

	messageList = make([]*message.Message, 0)
	messageList = append(messageList, msg1)
	messageList = append(messageList, msg1)

	gomock.InOrder(
		mockProducer.EXPECT().IncreaseBufferedCount(int64(2), int64(len(msgStr)*2)).Times(1),
		mockProducer.EXPECT().IsActive().Return(true).Times(2),
		mockProducer.EXPECT().DecreaseBufferedCount(int64(2), int64(len(msgStr)*2)).Times(1),
	)

	partitionMessageQueue.MqWg.Add(1)
	partitionMessageQueue.AddMessage(userMessageList)
	// check log has waiting time or not
	assert.Equal(t, len(messageList), len(partitionMessageQueue.GetMessageList()))
}

func TestAddAndGetMessageWhenNotAlive(t *testing.T) {
	setUpBefore()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockProducer := mock_producer.NewMockProducer(ctrl)
	partitionMessageQueue := producer.NewPartitionMessageQueueForTest(producerConfig, partitionId, mockProducer)

	userMessageList = make([]*producer.UserMessage, 0)
	userMessageList = append(userMessageList, userMessage1)
	userMessageList = append(userMessageList, userMessage2)
	userMessageList = append(userMessageList, userMessage3)

	messageList = make([]*message.Message, 0)
	messageList = append(messageList, msg1)
	messageList = append(messageList, msg1)
	messageList = append(messageList, msg1)

	gomock.InOrder(
		mockProducer.EXPECT().IncreaseBufferedCount(int64(3), int64(len(msgStr)*3)).Times(1),
		mockProducer.EXPECT().IsActive().Return(true).Times(1),
		mockProducer.EXPECT().DecreaseBufferedCount(int64(3), int64(len(msgStr)*3)).Times(1),
		mockProducer.EXPECT().IsActive().Return(false).Times(1),
		mockProducer.EXPECT().DecreaseBufferedCount(int64(0), int64(len(msgStr)*0)).Times(1),
	)

	partitionMessageQueue.MqWg.Add(1)
	partitionMessageQueue.AddMessage(userMessageList)
	// check log has waiting time or not
	assert.Equal(t, len(messageList), len(partitionMessageQueue.GetMessageList()))
	assert.Equal(t, 0, len(partitionMessageQueue.GetMessageList()))
}

func TestAddAndGetWaitMessageWhenNotAlive(t *testing.T) {
	defer log4go.Close()
	setUpBefore()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockProducer := mock_producer.NewMockProducer(ctrl)
	partitionMessageQueue := producer.NewPartitionMessageQueueForTest(producerConfig, partitionId, mockProducer)

	userMessageList = make([]*producer.UserMessage, 0)
	userMessageList = append(userMessageList, userMessage1)
	userMessageList = append(userMessageList, userMessage2)

	messageList = make([]*message.Message, 0)
	messageList = append(messageList, msg1)
	messageList = append(messageList, msg1)

	gomock.InOrder(
		mockProducer.EXPECT().IncreaseBufferedCount(int64(2), int64(len(msgStr)*2)).Times(1),
		mockProducer.EXPECT().IsActive().Return(true).Times(2),
		mockProducer.EXPECT().DecreaseBufferedCount(int64(2), int64(len(msgStr)*2)).Times(1),
		mockProducer.EXPECT().IsActive().Return(false).Times(1),
		mockProducer.EXPECT().DecreaseBufferedCount(int64(0), int64(len(msgStr)*0)).Times(1),
	)

	partitionMessageQueue.MqWg.Add(1)
	partitionMessageQueue.AddMessage(userMessageList)
	// check log has waiting time or not
	assert.Equal(t, len(messageList), len(partitionMessageQueue.GetMessageList()))
	assert.Equal(t, 0, len(partitionMessageQueue.GetMessageList()))
}
