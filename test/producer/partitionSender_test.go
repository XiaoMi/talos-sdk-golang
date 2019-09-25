/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package producer

import (
	"strconv"
	"sync/atomic"
	"testing"

	"talos-sdk-golang/thrift/message"
	"talos-sdk-golang/thrift/topic"

	"talos-sdk-golang/utils"

	"talos-sdk-golang/client"
	"talos-sdk-golang/producer"
	"talos-sdk-golang/testos-sdk-golang/test/mock_message"
	"talos-sdk-golang/testos-sdk-golang/test/mock_producer"

	"github.com/XiaoMi/talos-sdk-golang/thrift/thrift"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

const (
	resourceName0                = "12345#TopicName#july7777770009990"
	resourceName1                = "12345#TopicName#july7777770009991"
	resourceName2                = "12345#TopicName#july7777770009992"
	resourceName3                = "12345#TopicName#july7777770009993"
	topicName3                   = "TopicName"
	producerMaxBufferedMillSecs1 = 10
	producerMaxPutMsgNumber1     = 5
	producerMaxPutMsgBytes1      = 50
	producerMaxBufferedMsgNum1   = 50
	partitionId1                 = 0
)

var talosResourceName0 *topic.TopicTalosResourceName
var talosResourceName1 *topic.TopicTalosResourceName
var talosResourceName2 *topic.TopicTalosResourceName
var talosResourceName3 *topic.TopicTalosResourceName
var talosProducerConfig1 *producer.TalosProducerConfig
var partitionSender1 *producer.PartitionSender
var messageClientMock1 *mock_message.MockMessageService
var talosProducer1 *mock_producer.MockProducer
var userMsg1 *producer.UserMessage
var userMsg2 *producer.UserMessage
var userMsg3 *producer.UserMessage
var userMsg4 *producer.UserMessage
var globalLock chan producer.LockState
var requestId1 *int64
var userMsgList1 []*producer.UserMessage
var topic3 *topic.Topic
var msgPutSuccessCount1 *int32
var msgPutFailureCount1 *int32
var messageCallback *MessageCallback

type MessageCallback struct {
}

func (m *MessageCallback) OnSuccess(result *producer.UserMessageResult) {
	atomic.StoreInt32(msgPutSuccessCount1, atomic.LoadInt32(msgPutSuccessCount1)+int32(len(result.GetMessageList())))
}

func (m *MessageCallback) OnError(result *producer.UserMessageResult) {
	atomic.StoreInt32(msgPutFailureCount1, atomic.LoadInt32(msgPutFailureCount1)+int32(len(result.GetMessageList())))
}

func clearCounter() {
	atomic.StoreInt32(msgPutSuccessCount1, 0)
	atomic.StoreInt32(msgPutFailureCount1, 0)
}

func setup(t *testing.T) {
	properties := utils.NewProperties()
	properties.SetProperty(producer.GALAXY_TALOS_PRODUCER_MAX_BUFFERED_MILLI_SECS,
		strconv.FormatInt(producerMaxBufferedMillSecs1, 10))
	properties.SetProperty(producer.GALAXY_TALOS_PRODUCER_MAX_PUT_MESSAGE_NUMBER,
		strconv.FormatInt(producerMaxPutMsgNumber1, 10))
	properties.SetProperty(producer.GALAXY_TALOS_PRODUCER_MAX_PUT_MESSAGE_BYTES,
		strconv.FormatInt(producerMaxPutMsgBytes1, 10))
	properties.SetProperty(producer.GALAXY_TALOS_PRODUCER_MAX_BUFFERED_MESSAGE_NUMBER,
		strconv.FormatInt(producerMaxBufferedMsgNum1, 10))
	properties.SetProperty(client.GALAXY_TALOS_SERVICE_ENDPOINT, "testURL")
	talosProducerConfig1 = producer.NewTalosProducerConfigForTest(properties, false)

	talosResourceName0 = &topic.TopicTalosResourceName{resourceName0}
	talosResourceName1 = &topic.TopicTalosResourceName{resourceName1}
	talosResourceName2 = &topic.TopicTalosResourceName{resourceName2}
	talosResourceName3 = &topic.TopicTalosResourceName{resourceName3}

	userMsgList1 = make([]*producer.UserMessage, 0)
	userMsg1 = producer.NewUserMessage(&message.Message{Message: []byte("hello")})
	userMsg2 = producer.NewUserMessage(&message.Message{Message: []byte("world")})
	userMsg3 = producer.NewUserMessage(&message.Message{Message: []byte("nice day")})
	userMsg4 = producer.NewUserMessage(&message.Message{Message: []byte("good guy")})

	requestId1 = thrift.Int64Ptr(1)
	messageCallback = &MessageCallback{}

	msgPutFailureCount1 = new(int32)
	msgPutSuccessCount1 = new(int32)
	atomic.StoreInt32(msgPutFailureCount1, 0)
	atomic.StoreInt32(msgPutSuccessCount1, 0)
}

func TestPartitionSenderSuccessPut(t *testing.T) {
	// mock parameter
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	messageClientMock1 = mock_message.NewMockMessageService(ctrl)
	talosProducer1 = mock_producer.NewMockProducer(ctrl)

	setup(t)

	userMsgList1 = append(userMsgList1, userMsg1)
	userMsgList1 = append(userMsgList1, userMsg2)
	userMsgList1 = append(userMsgList1, userMsg3)
	userMsgList1 = append(userMsgList1, userMsg4)

	messageClientMock1.EXPECT().PutMessage(gomock.Any()).Return(message.NewPutMessageResponse(), nil).AnyTimes()
	talosProducer1.EXPECT().IsActive().Return(true).AnyTimes()
	talosProducer1.EXPECT().IsActive().Return(false).AnyTimes()
	talosProducer1.EXPECT().IsDisable().Return(true).AnyTimes()
	talosProducer1.EXPECT().IsDisable().Return(true).AnyTimes()
	talosProducer1.EXPECT().IncreaseBufferedCount(int64(4), int64(26)).AnyTimes()
	talosProducer1.EXPECT().IncreaseBufferedCount(int64(0), int64(0)).AnyTimes()
	talosProducer1.EXPECT().DecreaseBufferedCount(int64(5), int64(31)).AnyTimes()
	talosProducer1.EXPECT().DecreaseBufferedCount(int64(5), int64(34)).AnyTimes()
	partitionSender1 = producer.NewPartitionSender(partitionId1, topicName3,
		talosResourceName1, requestId1, utils.GenerateClientId(), talosProducerConfig1,
		messageClientMock1, messageCallback, globalLock, talosProducer1)

	addCount := 50
	for addCount > 0 {
		partitionSender1.AddMessage(userMsgList1)
		addCount -= 1
	}

	partitionSender1.Shutdown()
	assert.Equal(t, int32(200), atomic.LoadInt32(msgPutSuccessCount1))
}
