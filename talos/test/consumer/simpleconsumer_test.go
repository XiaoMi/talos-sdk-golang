/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package consumer

import (
  "testing"

  "github.com/XiaoMi/talos-sdk-golang/talos/client/compression"
  "github.com/XiaoMi/talos-sdk-golang/talos/consumer"
  "github.com/XiaoMi/talos-sdk-golang/talos/thrift/message"
  "github.com/XiaoMi/talos-sdk-golang/talos/thrift/topic"
  "github.com/XiaoMi/talos-sdk-golang/talos/utils"
  "github.com/XiaoMi/talos-sdk-golang/talos/test/mock_message"
  "github.com/golang/mock/gomock"
  "github.com/stretchr/testify/assert"
  log "github.com/alecthomas/log4go"
)

const (
	startOffset  = int64(0)
	topicName    = "TestTopic"
	resourceName = "CL8888#TestTopic#1238e616adfa00431fa98245694"
	partitionId  = int32(7)

	message1 = "message1"
	message2 = "message2"
	message3 = "message3"
)

func setUpConsumer() *consumer.SimpleConsumer {
	properties := utils.NewProperties()
	properties.SetProperty("galaxy.talos.service.endpoint", "testUrl")
	consumerConfig, _ := consumer.NewTalosConsumerConfigByProperties(properties)
	topicAndPartition := &topic.TopicAndPartition{
		TopicName:              topicName,
		TopicTalosResourceName: &topic.TopicTalosResourceName{TopicTalosResourceName: resourceName},
		PartitionId:            partitionId,
	}

	var messageClient message.MessageService
	messageClient = &message.MessageServiceClient{}
	//scheduleInfoCache := &client.ScheduleInfoCache{}
	simpleConsumer := consumer.NewSimpleConsumerForTest(consumerConfig,
	  topicAndPartition, messageClient)
	return simpleConsumer
}

func setUpResponse(t *testing.T) (*message.GetMessageResponse, []*message.MessageAndOffset) {
	messageList := make([]*message.Message, 0, 3)
	currentTime := utils.CurrentTimeMills()
	messageType := message.MessageType_BINARY
	msg1 := message.Message{Message: []byte(message1)}
	msg2 := message.Message{Message: []byte(message2)}
	msg3 := message.Message{Message: []byte(message3)}
	msg1.CreateTimestamp = &currentTime
	msg2.CreateTimestamp = &currentTime
	msg3.CreateTimestamp = &currentTime
	msg1.MessageType = &messageType
	msg2.MessageType = &messageType
	msg3.MessageType = &messageType

	messageList = append(messageList, &msg1)
	messageList = append(messageList, &msg2)
	messageList = append(messageList, &msg3)

	messageBlock, _ := compression.Compress(messageList, message.MessageCompressionType_SNAPPY)
	messageBlock.StartMessageOffset = startOffset

	messageBlockList := make([]*message.MessageBlock, 0, 1)
	messageBlockList = append(messageBlockList, messageBlock)

	messageAndOffset1 := message.MessageAndOffset{Message: &msg1, MessageOffset: startOffset}
	messageAndOffset2 := message.MessageAndOffset{Message: &msg2, MessageOffset: startOffset + 1}
	messageAndOffset3 := message.MessageAndOffset{Message: &msg3, MessageOffset: startOffset + 2}

	messageAndOffsetList := make([]*message.MessageAndOffset, 0, 3)
	messageAndOffsetList = append(messageAndOffsetList, &messageAndOffset1)
	messageAndOffsetList = append(messageAndOffsetList, &messageAndOffset2)
	messageAndOffsetList = append(messageAndOffsetList, &messageAndOffset3)

	response := &message.GetMessageResponse{
		MessageBlocks: messageBlockList,
		MessageNumber: 3,
	}
	return response, messageAndOffsetList
}

func TestClientPrefixIllegal(t *testing.T) {
  defer log.Close()
	properties := utils.NewProperties()
	properties.SetProperty("galaxy.talos.service.endpoint", "testUrl")
	consumerConfig, _ := consumer.NewTalosConsumerConfigByProperties(properties)
	topicAndPartition := &topic.TopicAndPartition{
		TopicName:              topicName,
		TopicTalosResourceName: &topic.TopicTalosResourceName{TopicTalosResourceName: resourceName},
		PartitionId:            partitionId,
	}

	var messageClient message.MessageService
	messageClient = &message.MessageServiceClient{}
	simpleConsumer1 := consumer.NewSimpleConsumer(consumerConfig, topicAndPartition,
		messageClient, "prefix#illegal")
	assert.NotEqual(t, simpleConsumer1, nil)
}

func TestGetInfo(t *testing.T) {
	defer log.Close()
	simpleConsumer2 := setUpConsumer()
	assert.Equal(t, simpleConsumer2.TopicTalosResourceName(),
		&topic.TopicTalosResourceName{TopicTalosResourceName: resourceName})

	assert.Equal(t, simpleConsumer2.PartitionId(), partitionId)

	tap := &topic.TopicAndPartition{
		TopicName:              topicName,
		TopicTalosResourceName: &topic.TopicTalosResourceName{TopicTalosResourceName: resourceName},
		PartitionId:            partitionId,
	}
	assert.Equal(t, simpleConsumer2.TopicAndPartition(), tap)
}

func TestCheckStartOffset(t *testing.T) {
	response, _ := setUpResponse(t)
	response.SequenceId = "testCheckStartOffset"

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockMessageClient := mock_message.NewMockMessageService(ctrl)
	mockMessageClient.EXPECT().GetMessage(gomock.Any()).Return(response, nil).Times(6)

	//mockScheduleInfoCache := mock_client.NewMockScheduleInfo(ctrl)
	//mockScheduleInfoCache.EXPECT().GetOrCreateMessageClient(gomock.Any()).Return(mockMessageClient).Times(6)

	properties := utils.NewProperties()
	properties.SetProperty("galaxy.talos.service.endpoint", "testUrl")
	consumerConfig, _ := consumer.NewTalosConsumerConfigByProperties(properties)
	topicAndPartition := &topic.TopicAndPartition{
		TopicName:              topicName,
		TopicTalosResourceName: &topic.TopicTalosResourceName{TopicTalosResourceName: resourceName},
		PartitionId:            partitionId,
	}

	simpleConsumer3 := consumer.NewSimpleConsumerForTest(consumerConfig, topicAndPartition,
		mockMessageClient)

	msgAndOffset1, e := simpleConsumer3.FetchMessage(int64(message.MessageOffset_START_OFFSET), 100)
	assert.Equal(t, 3, len(msgAndOffset1))
	assert.Equal(t, nil, e)

	msgAndOffset2, e := simpleConsumer3.FetchMessage(int64(message.MessageOffset_LATEST_OFFSET), 100)
	assert.Equal(t, 3, len(msgAndOffset2))
	assert.Equal(t, nil, e)

	msgAndOffset3, e := simpleConsumer3.FetchMessage(0, 100)
	assert.Equal(t, 3, len(msgAndOffset3))
	assert.Equal(t, nil, e)

	msgAndOffset4, e := simpleConsumer3.FetchMessage(2, 100)
	assert.Equal(t, e, nil)
	assert.Equal(t, 1, len(msgAndOffset4))
	assert.Equal(t, "message3", string(msgAndOffset4[0].GetMessage().GetMessage()))

	msgAndOffset5, e := simpleConsumer3.FetchMessage(-1, 100)
	assert.Equal(t, 3, len(msgAndOffset5))
	assert.Equal(t, nil, e)

	msgAndOffset6, e := simpleConsumer3.FetchMessage(-2, 100)
	assert.Equal(t, 3, len(msgAndOffset6))
	assert.Equal(t, nil, e)

	//check startOffset validity error
	msgAndOffset7, e := simpleConsumer3.FetchMessage(-3, 100)
	assert.Equal(t, 0, len(msgAndOffset7))
	assert.NotEqual(t, nil, e)
	assert.Equal(t, msgAndOffset7, []*message.MessageAndOffset(nil))

}

func TestFetchMessage(t *testing.T) {
	defer log.Close()
	unHandledNumber := int64(117)
	response, messageAndOffsetList := setUpResponse(t)
	response.SequenceId = "testFetchMessage"
	response.UnHandledMessageNumber = &unHandledNumber

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockMessageClient := mock_message.NewMockMessageService(ctrl)
	mockMessageClient.EXPECT().GetMessage(gomock.Any()).Return(response, nil).Times(2)

	//mockScheduleInfoCache := mock_client.NewMockScheduleInfo(ctrl)
	//mockScheduleInfoCache.EXPECT().GetOrCreateMessageClient(gomock.Any()).Return(mockMessageClient).Times(2)

	properties := utils.NewProperties()
	properties.SetProperty("galaxy.talos.service.endpoint", "testUrl")
	consumerConfig, _ := consumer.NewTalosConsumerConfigByProperties(properties)
	topicAndPartition := &topic.TopicAndPartition{
		TopicName:              topicName,
		TopicTalosResourceName: &topic.TopicTalosResourceName{TopicTalosResourceName: resourceName},
		PartitionId:            partitionId,
	}

	simpleConsumer4 := consumer.NewSimpleConsumerForTest(consumerConfig, topicAndPartition,
		mockMessageClient)

	//verify fetched message
	msgList , e := simpleConsumer4.FetchMessage(startOffset, consumerConfig.GetMaxFetchRecords())
	assert.Equal(t, nil, e)
	for i := 0; i < len(msgList); i++ {
		assert.Equal(t, messageAndOffsetList[i].GetMessage(), msgList[i].GetMessage())
		assert.Equal(t, messageAndOffsetList[i].GetMessageOffset(), msgList[i].GetMessageOffset())
		assert.Equal(t, msgList[i].GetUnHandledMessageNumber(), unHandledNumber + int64(len(msgList)) - 1 - int64(i))
	}

	//fetch too many
	msgList1, e := simpleConsumer4.FetchMessage(startOffset, 3000)
	assert.Equal(t, []*message.MessageAndOffset(nil), msgList1)
	assert.NotEqual(t, nil, e)


	//fetch message in middle offset
	messageAndOffsetList = messageAndOffsetList[1:]

	msgList2, e := simpleConsumer4.FetchMessage(startOffset + 1, 100)
	assert.Nil(t, e)
	for i := 0; i < len(msgList2); i++ {
		assert.Equal(t, messageAndOffsetList[i].GetMessage(), msgList2[i].GetMessage())
		assert.Equal(t, messageAndOffsetList[i].GetMessageOffset(), msgList2[i].GetMessageOffset())
		assert.Equal(t, msgList[i].GetUnHandledMessageNumber(), unHandledNumber + int64(len(msgList)) - 1 - int64(i))
	}

}
