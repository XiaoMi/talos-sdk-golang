/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package producer

import (
	"bytes"
	"math/rand"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"errors"

	"talos-sdk-golang/thrift/message"
	"talos-sdk-golang/thrift/topic"

	"talos-sdk-golang/utils"

	"talos-sdk-golang/client"
	"talos-sdk-golang/producer"
	"talos-sdk-golang/testos-sdk-golang/test/mock_admin"
	"talos-sdk-golang/testos-sdk-golang/test/mock_client"
	"talos-sdk-golang/testos-sdk-golang/test/mock_message"
	"talos-sdk-golang/testos-sdk-golang/test/mock_producer"

	"talos-sdk-golang/thrift/auth"

	"github.com/XiaoMi/talos-sdk-golang/thrift/thrift"
	"github.com/golang/mock/gomock"
	log4go "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

const (
	base                        string = "abcdefgh ijklmnopqr stuvwxyz 0123456789"
	resourceName                string = "12345#TopicName#july777777000999"
	anotherResourceName         string = "12345#TopicName#july777777000629"
	topicName                   string = "TopicName"
	ownerId                     string = "12345"
	messageRetentionMs          int32  = 1000
	partitionNumber             int32  = 8
	partitionNumber2            int32  = 16
	randomStrLen                int    = 15
	producerMaxBufferedMillSecs int64  = 10
	producerMaxPutMsgNumber     int64  = 10
	producerMaxPutMsgBytes      int64  = 100
	checkPartitionInterval      int64  = 200
)

var talosProducerConfig *producer.TalosProducerConfig
var talosProducer *producer.TalosProducer
var msgList []*message.Message
var topic1 *topic.Topic
var msgPutSuccessCount *int32
var msgPutFailureCount *int32
var credential *auth.Credential
var talosAdminMock *mock_admin.MockAdmin
var talosClientFactoryMock *mock_client.MockTalosClient
var messageClientMock *mock_message.MockMessageService
var partitionSenderMock *mock_producer.MockSender

type TestCallback struct {
}

func (t *TestCallback) OnSuccess(result *producer.UserMessageResult) {
	addSuccessCounter(len(result.GetMessageList()))
}

func (t *TestCallback) OnError(result *producer.UserMessageResult) {
	addFailureCounter(len(result.GetMessageList()))
}

func getRandomString(randomStrLen int) string {
	rand.Seed(time.Now().UnixNano())
	buf := make([]byte, 0, 1024)
	stringBuffer := bytes.NewBuffer(buf)
	for i := 0; i < randomStrLen; i++ {
		randomNumber := rand.Intn(len(base))
		stringBuffer.WriteByte(base[randomNumber])
	}
	return stringBuffer.String()
}

func addSuccessCounter(counter int) {
	atomic.StoreInt32(msgPutSuccessCount, atomic.LoadInt32(msgPutSuccessCount)+int32(counter))
}

func addFailureCounter(counter int) {
	atomic.StoreInt32(msgPutFailureCount, atomic.LoadInt32(msgPutFailureCount)+int32(counter))
}

func SetUp(t *testing.T) {
	// set properties
	properties := utils.NewProperties()
	properties.SetProperty(producer.GALAXY_TALOS_PRODUCER_MAX_BUFFERED_MILLI_SECS,
		strconv.FormatInt(producerMaxBufferedMillSecs, 10))
	properties.SetProperty(producer.GALAXY_TALOS_PRODUCER_MAX_PUT_MESSAGE_NUMBER,
		strconv.FormatInt(producerMaxPutMsgNumber, 10))
	properties.SetProperty(producer.GALAXY_TALOS_PRODUCER_MAX_PUT_MESSAGE_BYTES,
		strconv.FormatInt(producerMaxPutMsgBytes, 10))
	properties.SetProperty(producer.GALAXY_TALOS_PRODUCER_CHECK_PARTITION_INTERVAL,
		strconv.FormatInt(checkPartitionInterval, 10))
	properties.SetProperty(client.GALAXY_TALOS_SERVICE_ENDPOINT, "testURL")
	talosProducerConfig = producer.NewTalosProducerConfigForTest(properties, false)

	// construct topic1
	topicInfo := &topic.TopicInfo{
		TopicName:              topicName,
		TopicTalosResourceName: &topic.TopicTalosResourceName{resourceName},
		OwnerId:                ownerId,
	}
	topicAttribute := &topic.TopicAttribute{
		PartitionNumber:      thrift.Int32Ptr(partitionNumber),
		MessageRetentionSecs: thrift.Int32Ptr(messageRetentionMs),
	}
	topicState := &topic.TopicState{
		TopicStatus:     topic.TopicStatus_ACTIVE,
		CreateTimestamp: utils.CurrentTimeMills(),
	}
	topic1 = &topic.Topic{
		TopicInfo:      topicInfo,
		TopicAttribute: topicAttribute,
		TopicState:     topicState,
	}

	// mock parameter
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	talosAdminMock = mock_admin.NewMockAdmin(ctrl)
	talosClientFactoryMock = mock_client.NewMockTalosClient(ctrl)
	messageClientMock = mock_message.NewMockMessageService(ctrl)
	partitionSenderMock = mock_producer.NewMockSender(ctrl)

	talosClientFactoryMock.EXPECT().NewMessageClientDefault().Return(messageClientMock).AnyTimes()
	messageClientMock.EXPECT().PutMessage(gomock.Any()).Return(message.NewPutMessageResponse(), nil).AnyTimes()

	// generate 100 random messages
	msgList = make([]*message.Message, 0, 100)
	for i := 0; i < 100; i++ {
		msgList = append(msgList, &message.Message{Message: []byte(getRandomString(randomStrLen))})
	}
	msgPutFailureCount = new(int32)
	msgPutSuccessCount = new(int32)
	atomic.StoreInt32(msgPutFailureCount, 0)
	atomic.StoreInt32(msgPutSuccessCount, 0)
}

func TestAsynchronouslyAddUserMessage(t *testing.T) {
	SetUp(t)
	// mock parameter

	talosClientFactoryMock.EXPECT().NewMessageClientDefault().Return(messageClientMock).AnyTimes()
	messageClientMock.EXPECT().PutMessage(gomock.Any()).Return(message.NewPutMessageResponse(), nil).AnyTimes()
	talosAdminMock.EXPECT().DescribeTopic(&topic.DescribeTopicRequest{topicName}).Return(topic1, nil)

	talosProducer = producer.NewTalosProducerForTest(talosProducerConfig,
		talosClientFactoryMock, talosAdminMock,
		&topic.TopicTalosResourceName{resourceName},
		&client.SimpleTopicAbnormalCallback{}, &TestCallback{})
	talosProducer.AddUserMessage(msgList)

	time.Sleep(time.Duration(10*producerMaxBufferedMillSecs) * time.Millisecond)
}

//check partition change when producer running
func TestPartitionChangeDuringProducerRunning(t *testing.T) {
	SetUp(t)

	topicInfo2 := &topic.TopicInfo{
		TopicName:              topicName,
		TopicTalosResourceName: &topic.TopicTalosResourceName{resourceName},
		OwnerId:                ownerId,
	}
	topicAttribute2 := &topic.TopicAttribute{
		PartitionNumber:      thrift.Int32Ptr(partitionNumber2),
		MessageRetentionSecs: thrift.Int32Ptr(messageRetentionMs),
	}
	topicState2 := &topic.TopicState{
		TopicStatus:     topic.TopicStatus_ACTIVE,
		CreateTimestamp: utils.CurrentTimeMills(),
	}
	another := &topic.Topic{
		TopicInfo:      topicInfo2,
		TopicAttribute: topicAttribute2,
		TopicState:     topicState2,
	}

	gomock.InOrder(
		talosAdminMock.EXPECT().DescribeTopic(&topic.DescribeTopicRequest{topicName}).Return(topic1, nil).Times(2),
		talosAdminMock.EXPECT().DescribeTopic(&topic.DescribeTopicRequest{topicName}).Return(another, nil).Times(1),
	)

	talosProducer = producer.NewTalosProducerForTest(talosProducerConfig,
		talosClientFactoryMock, talosAdminMock,
		&topic.TopicTalosResourceName{resourceName},
		&client.SimpleTopicAbnormalCallback{}, &TestCallback{})

	// wait check partition interval
	time.Sleep(time.Duration(checkPartitionInterval*2) * time.Millisecond)
	// check the partition number and outgoingMessageMap changing by log info
}

// check topic be deleted when producer running
func TestTopicBeDeleteDuringProducerRunning(t *testing.T) {
	SetUp(t)

	topicInfo2 := &topic.TopicInfo{
		TopicName:              topicName,
		TopicTalosResourceName: &topic.TopicTalosResourceName{anotherResourceName},
		OwnerId:                ownerId,
	}
	topicAttribute2 := &topic.TopicAttribute{
		PartitionNumber:      thrift.Int32Ptr(partitionNumber),
		MessageRetentionSecs: thrift.Int32Ptr(messageRetentionMs),
	}
	topicState2 := &topic.TopicState{
		TopicStatus:     topic.TopicStatus_ACTIVE,
		CreateTimestamp: utils.CurrentTimeMills(),
	}
	another := &topic.Topic{
		TopicInfo:      topicInfo2,
		TopicAttribute: topicAttribute2,
		TopicState:     topicState2,
	}

	gomock.InOrder(
		talosAdminMock.EXPECT().DescribeTopic(&topic.DescribeTopicRequest{topicName}).Return(topic1, nil).Times(1),
		talosAdminMock.EXPECT().DescribeTopic(&topic.DescribeTopicRequest{topicName}).Return(another, nil).Times(1),
	)

	talosProducer = producer.NewTalosProducerForTest(talosProducerConfig,
		talosClientFactoryMock, talosAdminMock,
		&topic.TopicTalosResourceName{resourceName},
		&client.SimpleTopicAbnormalCallback{}, &TestCallback{})

	// wait check partition interval
	time.Sleep(time.Duration(checkPartitionInterval*2) * time.Millisecond)
}

func TestAddUserMessage(t *testing.T) {
	defer log4go.Close()
	SetUp(t)
	topic1.GetTopicAttribute().PartitionNumber = thrift.Int32Ptr(1)
	talosAdminMock.EXPECT().DescribeTopic(&topic.DescribeTopicRequest{topicName}).Return(topic1, nil).AnyTimes()
	messageClientMock.EXPECT().PutMessage(gomock.Any()).Return(message.NewPutMessageResponse(), nil).AnyTimes()

	talosProducer = producer.NewTalosProducerForTest(talosProducerConfig,
		talosClientFactoryMock, talosAdminMock,
		&topic.TopicTalosResourceName{resourceName},
		&client.SimpleTopicAbnormalCallback{}, &TestCallback{})

	for i := 0; i < 100; i++ {
		talosProducer.AddUserMessage(msgList)
	}
	time.Sleep(time.Duration(checkPartitionInterval*2) * time.Millisecond)
	talosProducer.Shutdown()
	assert.Equal(t, int32(100*len(msgList)), atomic.LoadInt32(msgPutSuccessCount))
}

//TODO: shutdonw failed , holding block
func TestAddUserMessageTimeout(t *testing.T) {
	defer log4go.Close()
	SetUp(t)
	talosProducerConfig.SetMaxBufferedMsgBytes(int64(0))
	talosProducerConfig.SetMaxBufferedMsgNumber(int64(0))
	topic1.GetTopicAttribute().PartitionNumber = thrift.Int32Ptr(1)
	talosAdminMock.EXPECT().DescribeTopic(&topic.DescribeTopicRequest{topicName}).Return(topic1, nil).Times(5)

	talosClientFactoryMock.EXPECT().NewMessageClientDefault().Return(messageClientMock)
	messageClientMock.EXPECT().PutMessage(gomock.Any()).Return(message.NewPutMessageResponse(), nil)

	talosProducer = producer.NewTalosProducerForTest(talosProducerConfig,
		talosClientFactoryMock, talosAdminMock,
		&topic.TopicTalosResourceName{resourceName},
		&client.SimpleTopicAbnormalCallback{}, &TestCallback{})

	for i := 0; i < 100; i++ {
		if err := talosProducer.AddUserMessage(msgList); err != nil {
			assert.NotNil(t, err)
			break
		}
	}
	time.Sleep(time.Duration(checkPartitionInterval*2) * time.Millisecond)
	talosProducer.Shutdown()
}

func TestProducerNotActiveError(t *testing.T) {
	SetUp(t)

	topicInfo2 := &topic.TopicInfo{
		TopicName:              topicName,
		TopicTalosResourceName: &topic.TopicTalosResourceName{anotherResourceName},
		OwnerId:                ownerId,
	}
	topicAttribute2 := &topic.TopicAttribute{
		PartitionNumber:      thrift.Int32Ptr(partitionNumber),
		MessageRetentionSecs: thrift.Int32Ptr(messageRetentionMs),
	}
	topicState2 := &topic.TopicState{
		TopicStatus:     topic.TopicStatus_ACTIVE,
		CreateTimestamp: utils.CurrentTimeMills(),
	}
	another := &topic.Topic{
		TopicInfo:      topicInfo2,
		TopicAttribute: topicAttribute2,
		TopicState:     topicState2,
	}

	talosAdminMock.EXPECT().DescribeTopic(&topic.DescribeTopicRequest{topicName}).Return(topic1, nil)
	talosAdminMock.EXPECT().DescribeTopic(&topic.DescribeTopicRequest{topicName}).Return(another, nil)
	partitionSenderMock.EXPECT().Shutdown().Do(nil)

	talosProducer = producer.NewTalosProducerForTest(talosProducerConfig,
		talosClientFactoryMock, talosAdminMock,
		&topic.TopicTalosResourceName{resourceName},
		&client.SimpleTopicAbnormalCallback{}, &TestCallback{})

	time.Sleep(time.Duration(checkPartitionInterval*2) * time.Millisecond)
	partitionSenderMock.EXPECT().AddMessage(gomock.Any()).Do(nil)

	err := talosProducer.AddUserMessage(msgList)
	assert.NotNil(t, err)
}

// addUserMessage check message validity
func TestAddUserMessageValidity(t *testing.T) {
	defer log4go.Close()
	SetUp(t)
	talosAdminMock.EXPECT().DescribeTopic(&topic.DescribeTopicRequest{topicName}).Return(topic1, nil).Times(2)

	talosProducer = producer.NewTalosProducerForTest(talosProducerConfig,
		talosClientFactoryMock, talosAdminMock,
		&topic.TopicTalosResourceName{resourceName},
		&client.SimpleTopicAbnormalCallback{}, &TestCallback{})

	partitionKey1 := getRandomString(utils.TALOS_PARTITION_KEY_LENGTH_MAXIMAL + 1)
	list := make([]*message.Message, 0)
	msg2 := &message.Message{Message: []byte("hello")}
	msg2.PartitionKey = &partitionKey1
	list = append(list, msg2)
	err := talosProducer.AddUserMessage(list)
	assert.NotNil(t, err)
	time.Sleep(200 * time.Millisecond)
}

// addUserMessage check nil pointer error
func TestAddUserMessageValidity2(t *testing.T) {
	defer log4go.Close()
	SetUp(t)
	talosAdminMock.EXPECT().DescribeTopic(&topic.DescribeTopicRequest{topicName}).Return(topic1, nil).Times(4)

	talosProducer = producer.NewTalosProducerForTest(talosProducerConfig,
		talosClientFactoryMock, talosAdminMock,
		&topic.TopicTalosResourceName{resourceName},
		&client.SimpleTopicAbnormalCallback{}, &TestCallback{})

	list := make([]*message.Message, 0)
	list = append(list, nil)
	err := talosProducer.AddUserMessage(list)
	assert.NotNil(t, err)
}

func TestAddUserMessageValidity3(t *testing.T) {
	defer log4go.Close()
	SetUp(t)
	talosAdminMock.EXPECT().DescribeTopic(&topic.DescribeTopicRequest{topicName}).Return(topic1, nil).Times(4)

	talosProducer = producer.NewTalosProducerForTest(talosProducerConfig,
		talosClientFactoryMock, talosAdminMock,
		&topic.TopicTalosResourceName{resourceName},
		&client.SimpleTopicAbnormalCallback{}, &TestCallback{})

	bigStr := getRandomString(utils.TALOS_SINGLE_MESSAGE_BYTES_MAXIMAL + 1)
	list := make([]*message.Message, 0)
	msg2 := &message.Message{Message: []byte(bigStr)}
	list = append(list, msg2)
	err := talosProducer.AddUserMessage(list)
	assert.NotNil(t, err)
}

// TODO: add judge topic not exist logical
func TestTopicNotExist(t *testing.T) {
	defer log4go.Close()
	SetUp(t)
	talosAdminMock.EXPECT().DescribeTopic(&topic.DescribeTopicRequest{topicName}).Return(nil, errors.New("topic not exist")).Times(4)

	talosProducer = producer.NewTalosProducerForTest(talosProducerConfig,
		talosClientFactoryMock, talosAdminMock,
		&topic.TopicTalosResourceName{resourceName},
		&client.SimpleTopicAbnormalCallback{}, &TestCallback{})

	bigStr := getRandomString(utils.TALOS_SINGLE_MESSAGE_BYTES_MAXIMAL + 1)
	list := make([]*message.Message, 0)
	msg2 := &message.Message{Message: []byte(bigStr)}
	list = append(list, msg2)
	err := talosProducer.AddUserMessage(list)
	assert.NotNil(t, err)
}

func TestTopicNotExistForDifferentResourceName(t *testing.T) {
	defer log4go.Close()
	SetUp(t)
	talosAdminMock.EXPECT().DescribeTopic(&topic.DescribeTopicRequest{topicName}).Return(topic1, nil).Times(1)

	talosProducer = producer.NewTalosProducerForTest(talosProducerConfig,
		talosClientFactoryMock, talosAdminMock,
		&topic.TopicTalosResourceName{anotherResourceName},
		&client.SimpleTopicAbnormalCallback{}, &TestCallback{})
	assert.Nil(t, talosProducer)
}
