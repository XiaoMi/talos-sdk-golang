/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package producer

import (
	"../admin"
	"github.com/XiaoMi/talos-sdk-golang/talos/client"
	"github.com/XiaoMi/talos-sdk-golang/talos/thrift/auth"
	"github.com/XiaoMi/talos-sdk-golang/talos/thrift/common"
	"github.com/XiaoMi/talos-sdk-golang/talos/thrift/topic"
	"github.com/XiaoMi/talos-sdk-golang/talos/utils"
	"time"
)

type ProducerState int32

const (
	ACTIVE ProducerState = iota
	DISABLED
	SHUTDOWN
)

type TalosProducer struct {
	producerState              ProducerState
	partitioner                Partitioner
	topicAbnormalCallback      client.TopicAbnormalCallback
	userMessageCallback        UserMessageCallback
	producerConfig             *TalosProducerConfig
	updatePartitionIdInterval  int64
	lastUpdatePartitionIdTime  int64
	updatePartitionIdMsgNumber int64
	lastAddMsgNumber           int64
	maxBufferedMsgNumber       int64
	maxBufferedMsgBytes        int64
	bufferedCount              *BufferedMessageCount
	clientId                   string
	talosClientFactory         *client.TalosClientFactory
	talosAdmin                 *admin.TalosAdmin
	topicName                  string
	partitionNumber            int
	curPartitionId             int32
	topicTalosResourceName     *topic.TopicTalosResourceName
	partitionSenderMap         map[int]*PartitionSender
}

func NewTalosProducer(producerConfig *TalosProducerConfig, credential *auth.Credential,
	topicTalosResourceName *topic.TopicTalosResourceName,
	topicAbnormalCallback client.TopicAbnormalCallback,
	userMessageCallback UserMessageCallback) (*TalosProducer, error) {

	bufMsgNum := producerConfig.GetMaxBufferedMsgNumber()
	bufMsgBytes := producerConfig.GetMaxBufferedMsgBytes()
	socketTimeout := int64(common.GALAXY_TALOS_CLIENT_ADMIN_TIMEOUT_MILLI_SECS_DEFAULT)
	clientFactory := client.NewTalosClientFactory(producerConfig.TalosClientConfig,
		credential, time.Duration(socketTimeout*int64(time.Millisecond)))

	return &TalosProducer{
		producerState:              ACTIVE,
		partitioner:                NewSimplePartitioner(),
		topicAbnormalCallback:      topicAbnormalCallback,
		userMessageCallback:        userMessageCallback,
		producerConfig:             producerConfig,
		updatePartitionIdInterval:  producerConfig.GetUpdatePartitionIdInterval(),
		lastUpdatePartitionIdTime:  utils.CurrentTimeMills(),
		updatePartitionIdMsgNumber: producerConfig.GetUpdatePartitionMsgNum(),
		lastAddMsgNumber:           0,
		maxBufferedMsgNumber:       bufMsgNum,
		maxBufferedMsgBytes:        bufMsgBytes,
		bufferedCount:              NewBufferedMessageCount(bufMsgNum, bufMsgBytes),
		topicTalosResourceName:     topicTalosResourceName,
		clientId:                   utils.GenerateClientId(),
		talosClientFactory:         &clientFactory,
		talosAdmin:                 NewTalosAdmin(clientFactory),
	}, nil
}
