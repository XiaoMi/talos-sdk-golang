/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package producer

import (
	"bytes"
	"fmt"
	"testing"

	"talos-sdk-golang/thrift/message"
	"talos-sdk-golang/thrift/topic"

	"talos-sdk-golang/utils"

	"talos-sdk-golang/producer"
	"talos-sdk-golang/testos-sdk-golang/test/mock_message"

	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/golang/mock/gomock"
	log4go "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

const (
	GALAXY_TALOS_DEFAULT_SERVICE_ENDPOINT                  = "http://talos.api.xiaomi.com"
	GALAXY_TALOS_PRODUCER_CHECK_PARTITION_INTERVAL_DEFAULT = 1000 * 60 * 3
	GALAXY_TALOS_PRODUCER_MAX_PUT_MESSAGE_NUMBER_DEFAULT   = 2000
)

func Test_ConfigWithoutInfo(t *testing.T) {
	properties := utils.NewProperties()
	producerConfig := producer.NewTalosProducerConfigByProperties(properties)
	assert.Equal(t, GALAXY_TALOS_DEFAULT_SERVICE_ENDPOINT, producerConfig.ServiceEndpoint())
	assert.Equal(t, int64(GALAXY_TALOS_PRODUCER_CHECK_PARTITION_INTERVAL_DEFAULT), producerConfig.GetCheckPartitionInterval())
	assert.Equal(t, int64(GALAXY_TALOS_PRODUCER_MAX_PUT_MESSAGE_NUMBER_DEFAULT), producerConfig.GetMaxPutMsgNumber())
	assert.Equal(t, message.MessageCompressionType_SNAPPY, producerConfig.GetCompressionType())
}

func Test_SimpleProducer(t *testing.T) {
	defer log4go.Close()
	// setUp
	topicName := "MyTopic"
	resourceName := "12345#MyTopic#dfi34598dfj4"
	partitionId := int32(7)
	properties := utils.NewProperties()
	properties.SetProperty("galaxy.talos.service.endpoint", "testURL")
	producerConfig := producer.NewTalosProducerConfigByProperties(properties)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockMessageClient := mock_message.NewMockMessageService(ctrl)

	topicAndParition := &topic.TopicAndPartition{
		topicName,
		&topic.TopicTalosResourceName{resourceName},
		partitionId,
	}
	msg := &message.Message{Message: []byte("hello world")}
	msgList := make([]*message.Message, 0)
	msgList = append(msgList, msg)
	clientId, _ := utils.CheckAndGenerateClientId("simpleProduce")
	simpleProducer := producer.NewSimpleProducer(producerConfig, topicAndParition,
		mockMessageClient, clientId, thrift.Int64Ptr(1))

	// Test PutMessage
	mockMessageClient.EXPECT().PutMessage(gomock.Any()).Return(message.NewPutMessageResponse(), nil).Times(1)
	assert.True(t, simpleProducer.PutMessage(msgList))
	assert.True(t, simpleProducer.PutMessage(make([]*message.Message, 0)))
	assert.True(t, simpleProducer.PutMessage(nil))

	// Test TopicAndPartition Error
	topicAndPartititon1 := &topic.TopicAndPartition{
		"Cl6737/" + topicName,
		&topic.TopicTalosResourceName{resourceName},
		partitionId,
	}
	assert.Nil(t, producer.NewSimpleProducer(producerConfig, topicAndPartititon1,
		mockMessageClient, clientId, new(int64)))

	// Test PutMessage Error
	mockMessageClient.EXPECT().PutMessage(gomock.Any()).
		Return(message.NewPutMessageResponse(), fmt.Errorf("test error")).Times(1)
	assert.False(t, simpleProducer.PutMessage(msgList))

	// Test MessageBlock size limit
	var buffer bytes.Buffer
	for i := 0; i <= 20*1024*1024; i++ {
		buffer.WriteString("a")
	}
	msg1 := &message.Message{
		Message: buffer.Bytes(),
	}
	msgList1 := make([]*message.Message, 0)
	msgList1 = append(msgList1, msg1)
	assert.False(t, simpleProducer.PutMessage(msgList1))
}
