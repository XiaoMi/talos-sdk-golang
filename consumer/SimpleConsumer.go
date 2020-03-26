/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package consumer

import (
	"strconv"
	"sync/atomic"

	"github.com/XiaoMi/talos-sdk-golang/client"
	"github.com/XiaoMi/talos-sdk-golang/client/compression"
	"github.com/XiaoMi/talos-sdk-golang/thrift/auth"
	"github.com/XiaoMi/talos-sdk-golang/thrift/message"
	"github.com/XiaoMi/talos-sdk-golang/thrift/topic"
	"github.com/XiaoMi/talos-sdk-golang/utils"
	"github.com/sirupsen/logrus"
)

type SimpleConsumer struct {
	topicAndPartition *topic.TopicAndPartition
	messageClient     message.MessageService
	consumerConfig    *TalosConsumerConfig
	scheduleInfoCache *client.ScheduleInfoCache
	requestId         atomic.Value
	simpleConsumerId  string
	log               *logrus.Logger
}

func NewSimpleConsumerByFilename(propertyFilename string) (*SimpleConsumer, error) {
	return NewSimpleConsumerByProperties(utils.LoadProperties(propertyFilename))
}

func NewSimpleConsumerByProperties(props *utils.Properties) (*SimpleConsumer, error) {
	topicName := props.Get("galaxy.talos.topic.name")
	secretKeyId := props.Get("galaxy.talos.access.key")
	secretKey := props.Get("galaxy.talos.access.secret")
	partitionId, _ := strconv.Atoi(props.Get("galaxy.talos.partition.id"))

	userType := auth.UserType_DEV_XIAOMI
	credential := &auth.Credential{
		TypeA1:      &userType,
		SecretKeyId: &secretKeyId,
		SecretKey:   &secretKey,
	}

	consumerConfig := NewTalosConsumerConfigByProperties(props)
	return NewSimpleConsumer(consumerConfig, topicName, int32(partitionId),
		credential, utils.InitLogger())
}

func NewSimpleConsumerWithLogger(propertyFilename string, logger *logrus.Logger) (*SimpleConsumer, error) {
	props := utils.LoadProperties(propertyFilename)
	topicName := props.Get("galaxy.talos.topic.name")
	secretKeyId := props.Get("galaxy.talos.access.key")
	secretKey := props.Get("galaxy.talos.access.secret")
	partitionId, _ := strconv.Atoi(props.Get("galaxy.talos.partition.id"))

	userType := auth.UserType_DEV_XIAOMI
	credential := &auth.Credential{
		TypeA1:      &userType,
		SecretKeyId: &secretKeyId,
		SecretKey:   &secretKey,
	}

	consumerConfig := NewTalosConsumerConfigByProperties(props)
	return NewSimpleConsumer(consumerConfig, topicName, int32(partitionId),
		credential, logger)
}

func NewSimpleConsumer(consumerConfig *TalosConsumerConfig, topicName string,
	partitionId int32, credential *auth.Credential, logger *logrus.Logger) (*SimpleConsumer, error) {
	talosClientFactory := client.NewTalosClientFactory(
		consumerConfig.TalosClientConfig, credential)
	return initForSimpleConsumer(consumerConfig, topicName, partitionId,
		talosClientFactory, "", logger)
}

func initForSimpleConsumer(consumerConfig *TalosConsumerConfig, topicName string,
	partitionId int32, talosClientFactory client.TalosClientFactoryInterface,
	consumerIdPrefix string, logger *logrus.Logger) (*SimpleConsumer, error) {

	var requestId atomic.Value
	requestId.Store(int64(1))

	err := utils.CheckTopicName(topicName)
	if err != nil {
		return nil, err
	}

	topicAndPartition, err := getTopicInfo(talosClientFactory.NewTopicClientDefault(), topicName, partitionId)
	if err != nil {
		return nil, err
	}

	consumerId, err := utils.CheckAndGenerateClientId(consumerIdPrefix)
	if err != nil {
		return nil, err
	}

	messageClient := talosClientFactory.NewMessageClientDefault()
	scheduleInfoCache := client.GetScheduleInfoCache(topicAndPartition.TopicTalosResourceName,
		consumerConfig.TalosClientConfig, messageClient, talosClientFactory, logger)

	return &SimpleConsumer{
		consumerConfig:    consumerConfig,
		topicAndPartition: topicAndPartition,
		messageClient:     messageClient,
		simpleConsumerId:  consumerId,
		requestId:         requestId,
		scheduleInfoCache: scheduleInfoCache,
		log:               logger,
	}, nil
}

func NewSimpleConsumerForHighLvl(consumerConfig *TalosConsumerConfig,
	topicAndPartition *topic.TopicAndPartition,
	messageClient message.MessageService,
	cache *client.ScheduleInfoCache, logger *logrus.Logger) (*SimpleConsumer, error) {
	return initForTalosConsumer(consumerConfig, topicAndPartition,
		nil, messageClient, "", cache, logger)
}

func initForTalosConsumer(consumerConfig *TalosConsumerConfig,
	topicAndPartition *topic.TopicAndPartition,
	talosClientFactory client.TalosClientFactoryInterface,
	messageClient message.MessageService, consumerIdPrefix string,
	cache *client.ScheduleInfoCache, logger *logrus.Logger) (*SimpleConsumer, error) {

	var requestId atomic.Value
	requestId.Store(int64(1))

	err := utils.CheckTopicAndPartition(topicAndPartition)
	if err != nil {
		return nil, err
	}

	consumerId, err := utils.CheckAndGenerateClientId(consumerIdPrefix)
	if err != nil {
		return nil, err
	}

	return &SimpleConsumer{
		consumerConfig:    consumerConfig,
		topicAndPartition: topicAndPartition,
		messageClient:     messageClient,
		simpleConsumerId:  consumerId,
		requestId:         requestId,
		scheduleInfoCache: cache,
		log:               logger,
	}, nil
}

func NewSimpleConsumerForTest(consumerConfig *TalosConsumerConfig,
	topicAndPartition *topic.TopicAndPartition,
	messageClient message.MessageService) (*SimpleConsumer, error) {

	if err := utils.CheckTopicAndPartition(topicAndPartition); err != nil {
		return nil, err
	}
	clientId, _ := utils.CheckAndGenerateClientId("")
	return &SimpleConsumer{
		consumerConfig:    consumerConfig,
		topicAndPartition: topicAndPartition,
		messageClient:     messageClient,
		simpleConsumerId:  clientId,
		//scheduleInfoCache: scheduleInfoCache,
	}, nil
}

func (c *SimpleConsumer) TopicAndPartition() *topic.TopicAndPartition {
	return c.topicAndPartition
}

func (c *SimpleConsumer) TopicTalosResourceName() *topic.TopicTalosResourceName {
	return c.topicAndPartition.GetTopicTalosResourceName()
}

func (c *SimpleConsumer) SetSimpleConsumerId(id string) {
	c.simpleConsumerId = id
}

func (c *SimpleConsumer) SimpleConsumerId() string {
	return c.simpleConsumerId
}

func (c *SimpleConsumer) PartitionId() int32 {
	return c.topicAndPartition.GetPartitionId()
}

func (c *SimpleConsumer) FetchMessage(startOffset, maxFetchedNumber int64) (
	[]*message.MessageAndOffset, error) {

	err := utils.CheckStartOffsetValidity(startOffset)
	if err != nil {
		return nil, err
	}

	err = utils.CheckParameterRange(
		GALAXY_TALOS_CONSUMER_MAX_FETCH_RECORDS,
		maxFetchedNumber,
		GALAXY_TALOS_CONSUMER_MAX_FETCH_RECORDS_MINIMUM,
		GALAXY_TALOS_CONSUMER_MAX_FETCH_RECORDS_MAXIMUM)
	if err != nil {
		return nil, err
	}

	requestSequenceId, err := utils.GenerateRequestSequenceId(
		c.simpleConsumerId, c.requestId)
	if err != nil {
		return nil, err
	}

	timestamp := utils.CurrentTimeMills() + c.consumerConfig.ClientTimeout()

	// limit the default max fetch bytes 2M
	getMessageRequest := &message.GetMessageRequest{
		TopicAndPartition:   c.topicAndPartition,
		MessageOffset:       startOffset,
		SequenceId:          requestSequenceId,
		MaxGetMessageNumber: int32(maxFetchedNumber),
		MaxGetMessageBytes:  GALAXY_TALOS_CONSUMER_MAX_FETCH_BYTES_DEFAULT,
		TimeoutTimestamp:    &timestamp,
	}

	var getMessageResponse *message.GetMessageResponse

	getMessageResponse, err = c.scheduleInfoCache.GetOrCreateMessageClient(
		c.topicAndPartition).GetMessage(getMessageRequest)
	if err != nil {
		if c.scheduleInfoCache != nil && c.scheduleInfoCache.IsAutoLocation() {
			c.log.Warnf("can't connect to the host directly, refresh "+
				"scheduleInfo and retry using url. The exception is: %s."+
				" Ignore this if not frequently.", err.Error())
			c.scheduleInfoCache.UpdateScheduleInfoCache()
			timestamp := utils.CurrentTimeMills() + c.consumerConfig.ClientTimeout()
			getMessageRequest.TimeoutTimestamp = &timestamp
			getMessageResponse, err = c.messageClient.GetMessage(getMessageRequest)
			if err != nil {
				c.log.Errorf("getMessage error: %s", err.Error())
				return nil, err
			}
		} else {
			return nil, err
		}
	}

	messageAndOffsetList := make([]*message.MessageAndOffset, 0)
	messageAndOffsetList, err = compression.Decompress(
		getMessageResponse.GetMessageBlocks(),
		getMessageResponse.GetUnHandledMessageNumber())
	if err != nil {
		return nil, err
	}

	if len(messageAndOffsetList) <= 0 {
		return messageAndOffsetList, nil
	}

	actualStartOffset := messageAndOffsetList[0].GetMessageOffset()
	if messageAndOffsetList[0].GetMessageOffset() == startOffset ||
		startOffset == int64(message.MessageOffset_START_OFFSET) ||
		startOffset == int64(message.MessageOffset_LATEST_OFFSET) {
		return messageAndOffsetList, nil
	} else {
		start := startOffset - actualStartOffset
		err := utils.CheckArgumentWithErrorMsg(start > 0, "Unexpected subList start index: ")
		if err != nil {
			return nil, err
		}
		return messageAndOffsetList[start:], nil
	}
}

func (c *SimpleConsumer) Shutdown() {
	//onec called, all request of this topic in the process cannot auto location
	c.scheduleInfoCache.Shutdown(c.TopicTalosResourceName())
}

func getTopicInfo(topicClient topic.TopicService, topicName string, partitionId int32) (
	*topic.TopicAndPartition, error) {
	request := &topic.GetDescribeInfoRequest{TopicName: topicName}
	response, err := topicClient.GetDescribeInfo(request)
	if err != nil {
		return nil, err
	}
	topicAndPartition := &topic.TopicAndPartition{
		TopicName:              topicName,
		TopicTalosResourceName: response.GetTopicTalosResourceName(),
		PartitionId:            partitionId,
	}
	return topicAndPartition, nil
}
