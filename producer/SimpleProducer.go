/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package producer

import (
	"fmt"
	"sync/atomic"

	"strconv"
	"talos-sdk-golang/client"
	"talos-sdk-golang/client/compression"
	"talos-sdk-golang/thrift/auth"
	"talos-sdk-golang/thrift/message"
	"talos-sdk-golang/thrift/topic"
	"talos-sdk-golang/utils"

	log "github.com/sirupsen/logrus"
)

type SimpleProducer struct {
	producerConfig    *TalosProducerConfig
	topicAndPartition *topic.TopicAndPartition
	messageClient     message.MessageService
	requestId         atomic.Value
	clientId          string
	scheduleInfoCache *client.ScheduleInfoCache
}

func NewSimpleProducerByProperties(props *utils.Properties) (*SimpleProducer, error) {
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

	producerConfig := NewTalosProducerConfigByProperties(props)
	return NewSimpleProducer(producerConfig, topicName, int32(partitionId),
		credential)
}

func NewSimpleProducerByFilename(propertyFilename string) (*SimpleProducer, error) {
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

	producerConfig := NewTalosProducerConfigByProperties(props)
	return NewSimpleProducer(producerConfig, topicName, int32(partitionId),
		credential)
}

func NewSimpleProducer(producerConfig *TalosProducerConfig, topicName string,
	partitionId int32, credential *auth.Credential) (*SimpleProducer, error) {
	clientId, err := utils.CheckAndGenerateClientId("SimpleProducer")
	if err != nil {
		return nil, err
	}
	return initForSimpleProducer(producerConfig, topicName, partitionId,
		client.NewTalosClientFactory(producerConfig.TalosClientConfig, credential),
		clientId, 1)
}

func initForSimpleProducer(producerConfig *TalosProducerConfig,
	topicName string, partitionId int32, talosClientFactory *client.TalosClientFactory,
	clientId string, requestId int64) (*SimpleProducer, error) {

	var reqId atomic.Value
	reqId.Store(requestId)

	err := utils.CheckTopicName(topicName)
	if err != nil {
		return nil, err
	}

	topicAndPartition, err := getTopicInfo(talosClientFactory.NewTopicClientDefault(), topicName, partitionId)
	if err != nil {
		return nil, err
	}

	messageClient := talosClientFactory.NewMessageClientDefault()
	scheduleInfoCache := client.GetScheduleInfoCache(topicAndPartition.TopicTalosResourceName,
		producerConfig.TalosClientConfig, messageClient, talosClientFactory)

	return &SimpleProducer{
		producerConfig:    producerConfig,
		topicAndPartition: topicAndPartition,
		messageClient:     messageClient,
		clientId:          clientId,
		requestId:         reqId,
		scheduleInfoCache: scheduleInfoCache,
	}, nil
}

func NewSimpleProducerForHighLvl(producerConfig *TalosProducerConfig,
	topicAndPartition *topic.TopicAndPartition,
	messageClient message.MessageService, clientId string,
	requestId atomic.Value, cache *client.ScheduleInfoCache) (*SimpleProducer, error) {
	return initForTalosProducer(producerConfig, topicAndPartition, nil,
		messageClient, clientId, requestId, cache)
}

func initForTalosProducer(producerConfig *TalosProducerConfig,
	topicAndPartition *topic.TopicAndPartition, talosClientFactory *client.TalosClientFactory,
	messageClient message.MessageService, clientId string,
	requestId atomic.Value, cache *client.ScheduleInfoCache) (*SimpleProducer, error) {

	err := utils.CheckTopicAndPartition(topicAndPartition)
	if err != nil {
		return nil, err
	}

	return &SimpleProducer{
		producerConfig:    producerConfig,
		topicAndPartition: topicAndPartition,
		messageClient:     messageClient,
		clientId:          clientId,
		requestId:         requestId,
		scheduleInfoCache: cache,
	}, nil
}

func (p *SimpleProducer) PutMessage(msgList []*message.Message) bool {
	if msgList == nil || len(msgList) == 0 {
		return true
	}
	err := p.PutMessageList(msgList)
	if err != nil {
		log.Errorf("putMessage error: %s, please try to put again", err.Error())
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
		log.Errorf("message data invalidity: %s", err.Error())
		return err
	}

	if err := p.doPut(msgList); err != nil {
		log.Errorf("doPut message error: %s", err.Error())
		return err
	}
	return nil
}

func (p *SimpleProducer) doPut(msgList []*message.Message) error {
	messageBlock, err := p.compressMessageList(msgList)
	if err != nil {
		log.Errorf("compress message list error: %s", err.Error())
		return err
	}
	messageBlockList := make([]*message.MessageBlock, 0, 1)
	messageBlockList = append(messageBlockList, messageBlock)

	requestSequenceId, err := utils.GenerateRequestSequenceId(p.clientId, p.requestId)
	if err != nil {
		log.Errorf("generate RequestSequenceId error: %s", err.Error())
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

	_, err = p.scheduleInfoCache.GetOrCreateMessageClient(p.topicAndPartition).
		PutMessage(putMessageRequest)
	if err != nil {
		if p.scheduleInfoCache != nil && p.scheduleInfoCache.IsAutoLocation() {
			log.Warnf("can't connect to the host directly, refresh "+
				"scheduleInfo and retry using url. The exception is: %s."+
				" Ignore this if not frequently.", err.Error())
			p.scheduleInfoCache.UpdateScheduleInfoCache()
			timestamp := utils.CurrentTimeMills() + p.producerConfig.ClientTimeout()
			putMessageRequest.TimeoutTimestamp = &timestamp
			_, err = p.messageClient.PutMessage(putMessageRequest)
			if err != nil {
				log.Errorf("putMessage error: %s", err.Error())
				return err
			}
		} else {
			return err
		}
	}

	return nil
}

func (p *SimpleProducer) compressMessageList(
	msgList []*message.Message) (*message.MessageBlock, error) {
	return compression.Compress(msgList, p.producerConfig.GetCompressionType())
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
