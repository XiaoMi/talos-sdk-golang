/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package client

import (
	"testing"

	"talos-sdk-golang/thrift/message"
	"talos-sdk-golang/thrift/topic"

	"talos-sdk-golang/utils"

	"talos-sdk-golang/client"
	"talos-sdk-golang/testos-sdk-golang/test/mock_client"
	"talos-sdk-golang/testos-sdk-golang/test/mock_message"

	"github.com/alecthomas/log4go"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

const (
	topicName0    = "TestTopicName0"
	topicName1    = "TestTopicName1"
	resourceName0 = "CL8888#TestTopicName0#1238e616adfa00431fa98245694"
	resourceName1 = "CL8888#TestTopicName1#1238e616adfa0dfa0431fa98245694"
	host0         = "host0:cnbj1-talos.api.xiaomi.net"
	host1         = "host1:cnbj4-talos.api.xiaomi.net"
	host2         = "host2:awsusor1-talos.api.xiaomi.net"
	partitionId   = int32(7)
)

func TestNewAndGetScheduleInfoCache(t *testing.T) {
	log4go.Close()
	properties := utils.NewProperties()
	properties.SetProperty("galaxy.talos.service.endpoint", "https://talos.api.xiaomi.com")
	clientConfig := client.NewTalosClientConfigByProperties(properties)
	topicTalosResourceName0 := &topic.TopicTalosResourceName{TopicTalosResourceName: resourceName0}
	topicTalosResourceName1 := &topic.TopicTalosResourceName{TopicTalosResourceName: resourceName1}
	topicAndPartition0 := &topic.TopicAndPartition{
		TopicName:              topicName0,
		TopicTalosResourceName: topicTalosResourceName0,
		PartitionId:            partitionId,
	}
	ScheduleInfoMap := make(map[*topic.TopicAndPartition]string, 1)
	ScheduleInfoMap[topicAndPartition0] = host0
	getScheduleInfoResponse := &message.GetScheduleInfoResponse{ScheduleInfo: ScheduleInfoMap}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockMessageClient := mock_message.NewMockMessageService(ctrl)
	mockMessageClient.EXPECT().GetScheduleInfo(gomock.Any()).Return(getScheduleInfoResponse, nil).Times(1)
	mockClientFactory := mock_client.NewMockTalosClient(ctrl)

	//test new scheduleinfocache
	scheduleInfoCache0 := client.NewScheduleInfoCache(topicTalosResourceName0,
		clientConfig, mockMessageClient, mockClientFactory)

	scheduleInfoCache1 := scheduleInfoCache0.GetScheduleInfoCache(topicTalosResourceName0)
	assert.NotNil(t, scheduleInfoCache1)
	assert.Equal(t, scheduleInfoCache0, scheduleInfoCache1)

	scheduleInfoCache2 := scheduleInfoCache0.GetScheduleInfoCache(topicTalosResourceName1)
	assert.Nil(t, scheduleInfoCache2)
	assert.NotEqual(t, scheduleInfoCache0, scheduleInfoCache2)

}

func TestGetOrCreatMessageClient(t *testing.T) {
	//log4go.LoadConfiguration("log4go.xml")
	defer log4go.Close()
	properties := utils.NewProperties()
	properties.SetProperty("galaxy.talos.service.endpoint", "https://talos.api.xiaomi.com")
	clientConfig := client.NewTalosClientConfigByProperties(properties)

	topicTalosResourceName0 := &topic.TopicTalosResourceName{TopicTalosResourceName: resourceName0}
	topicTalosResourceName1 := &topic.TopicTalosResourceName{TopicTalosResourceName: resourceName1}

	topicAndPartition0 := &topic.TopicAndPartition{TopicName: topicName0,
		TopicTalosResourceName: topicTalosResourceName0, PartitionId: partitionId}

	topicAndPartition1 := &topic.TopicAndPartition{TopicName: topicName1,
		TopicTalosResourceName: topicTalosResourceName1, PartitionId: 1}

	topicAndPartition2 := &topic.TopicAndPartition{TopicName: topicName1,
		TopicTalosResourceName: topicTalosResourceName1, PartitionId: 2}

	topicAndPartition3 := &topic.TopicAndPartition{TopicName: topicName1,
		TopicTalosResourceName: topicTalosResourceName1, PartitionId: 3}

	ScheduleInfoMap := make(map[*topic.TopicAndPartition]string, 1)
	ScheduleInfoMap[topicAndPartition2] = host0
	ScheduleInfoMap1 := make(map[*topic.TopicAndPartition]string, 3)
	ScheduleInfoMap1[topicAndPartition1] = host0
	ScheduleInfoMap1[topicAndPartition2] = host1
	ScheduleInfoMap1[topicAndPartition3] = host2
	getScheduleInfoResponse := &message.GetScheduleInfoResponse{ScheduleInfo: ScheduleInfoMap}
	getScheduleInfoResponse1 := &message.GetScheduleInfoResponse{ScheduleInfo: ScheduleInfoMap1}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockMessageClient := mock_message.NewMockMessageService(ctrl)
	mockMessageClient.EXPECT().GetScheduleInfo(gomock.Any()).Return(getScheduleInfoResponse1, nil).Times(2)

	mockMessageClient1 := mock_message.NewMockMessageService(ctrl)
	mockMessageClient2 := mock_message.NewMockMessageService(ctrl)
	mockMessageClient2.EXPECT().GetScheduleInfo(gomock.Any()).Return(getScheduleInfoResponse, nil).Times(2)
	//mockMessageClient3 := mock_message.NewMockMessageService(ctrl)

	mockClientFactory := mock_client.NewMockTalosClient(ctrl)
	mockClientFactory.EXPECT().NewMessageClient("http://" + host0).Return(mockMessageClient1)
	//mockClientFactory.EXPECT().NewMessageClient("http://" + host1).Return(mockMessageClient2)
	//mockClientFactory.EXPECT().NewMessageClient("http://" + host2).Return(mockMessageClient3)

	scheduleInfoCache := client.NewScheduleInfoCache(topicTalosResourceName1,
		clientConfig, mockMessageClient, mockClientFactory)

	assert.Equal(t, mockMessageClient, scheduleInfoCache.GetOrCreateMessageClient(topicAndPartition0))
	assert.Equal(t, mockMessageClient1, scheduleInfoCache.GetOrCreateMessageClient(topicAndPartition1))

	scheduleInfoCache2 := client.NewScheduleInfoCache(topicTalosResourceName1,
		clientConfig, mockMessageClient2, mockClientFactory)
	scheduleInfoCache2.UpdateScheduleInfoCache()
}
