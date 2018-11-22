/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package producer

import (
	"../admin"
	"../utils"
	"fmt"
	"github.com/XiaoMi/talos-sdk-golang/talos/client"
	"github.com/XiaoMi/talos-sdk-golang/talos/thrift/auth"
	"github.com/XiaoMi/talos-sdk-golang/talos/thrift/common"
	"github.com/XiaoMi/talos-sdk-golang/talos/thrift/topic"
	"github.com/XiaoMi/talos-sdk-golang/thrift"
	"github.com/alecthomas/log4go"
	"github.com/nu7hatch/gouuid"
	"math/rand"
	"sync"
	"time"
)

type ProducerState int32

const (
	ACTIVE ProducerState = iota
	DISABLED
	SHUTDOWN
)

const (
	partitionKeyMinLen = common.TALOS_PARTITION_KEY_LENGTH_MINIMAL
	partitionKeyMaxLen = common.TALOS_PARTITION_KEY_LENGTH_MAXIMAL
)

type TalosProducer struct {
	requestId                  *int64
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
	partitionNumber            int32
	curPartitionId             int32
	topicTalosResourceName     *topic.TopicTalosResourceName
	partitionSenderMap         map[int32]*PartitionSender
	mutex                      sync.Mutex
}

func NewTalosProducer(producerConfig *TalosProducerConfig, credential *auth.Credential,
	topicTalosResourceName *topic.TopicTalosResourceName,
	topicAbnormalCallback client.TopicAbnormalCallback,
	userMessageCallback UserMessageCallback) *TalosProducer {

	bufMsgNum := producerConfig.GetMaxBufferedMsgNumber()
	bufMsgBytes := producerConfig.GetMaxBufferedMsgBytes()
	socketTimeout := int64(common.GALAXY_TALOS_CLIENT_ADMIN_TIMEOUT_MILLI_SECS_DEFAULT)
	clientFactory := client.NewTalosClientFactory(producerConfig.TalosClientConfig,
		credential, time.Duration(socketTimeout*int64(time.Millisecond)))

	talosProducer := &TalosProducer{
		requestId:                  thrift.Int64Ptr(1),
		producerState:              ACTIVE,
		partitioner:                new(SimplePartitioner),
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
		talosAdmin:                 admin.NewTalosAdmin(&clientFactory),
	}

	talosProducer.checkAndGetTopicInfo(topicTalosResourceName)

	talosProducer.initPartitionSender()
	talosProducer.initCheckPartitionTask()
	log4go.Info("Init a producer for topic: %s, partitions: %d ",
		topicTalosResourceName.GetTopicTalosResourceName(),
		talosProducer.partitionNumber)

	return talosProducer
}

// cancel the putMessage threads and checkPartitionTask
// when topic not exist during producer running
func (p *TalosProducer) disableProducer(err error) {
	p.mutex.Lock()
	if !p.IsActive() {
		return
	}
	p.producerState = DISABLED
	p.stopAndwait()
	p.topicAbnormalCallback.AbnormalHandler(p.topicTalosResourceName, err)
	p.mutex.Unlock()
}

func (p *TalosProducer) shutdown() {
	p.mutex.Lock()
	if !p.IsActive() {
		return
	}

	p.producerState = SHUTDOWN
	p.stopAndwait()
	p.mutex.Unlock()
}

func (p *TalosProducer) stopAndwait() {
	//TODO stopAll
}

func (p *TalosProducer) IsActive() bool {
	return p.producerState == ACTIVE
}

func (p *TalosProducer) IsDisable() bool {
	return p.producerState == DISABLED
}

func (p *TalosProducer) IsShutdown() bool {
	return p.producerState == SHUTDOWN
}

func (p *TalosProducer) shouldUpdatePartition() bool {
	return utils.CurrentTimeMills()-p.lastUpdatePartitionIdTime >=
		p.updatePartitionIdInterval || p.lastAddMsgNumber >=
		p.updatePartitionIdMsgNumber
}

func (p *TalosProducer) checkAndGetTopicInfo(
	topicTalosResourceName *topic.TopicTalosResourceName) error {
	var err error
	rand.Seed(time.Now().UnixNano())
	p.mutex.Lock()
	p.topicName, err = utils.GetTopicNameByResourceName(
		topicTalosResourceName.GetTopicTalosResourceName())
	if err != nil {
		log4go.Error("Check and get TopicInfo error: %s", err.Error())
		return err
	}
	getTopic, err := p.talosAdmin.DescribeTopic(&topic.
		DescribeTopicRequest{TopicName: p.topicName})
	if err != nil {
		log4go.Error("Describe topic error: %s", err.Error())
		return err
	}
	if topicTalosResourceName != getTopic.GetTopicInfo().
		GetTopicTalosResourceName() {
		return fmt.Errorf("The topic: %s not found ",
			topicTalosResourceName.GetTopicTalosResourceName())
	}
	p.partitionNumber = getTopic.GetTopicAttribute().GetPartitionNumber()
	p.curPartitionId = rand.Int31n(p.partitionNumber)
	p.mutex.Unlock()
	return nil
}

func (p *TalosProducer) initPartitionSender() {
	p.mutex.Lock()
	for partitionId := int32(0); partitionId < p.partitionNumber; partitionId++ {
		p.createPartitionSender(partitionId)
	}
	p.mutex.Unlock()
}

func (p *TalosProducer) adjustPartitionSender(newPartitionNumber int32) {
	// Note: we do not allow and process 'newPartitionNum < partitionNumber'
	p.mutex.Lock()
	for partitionId := p.partitionNumber; partitionId < newPartitionNumber; partitionId++ {
		p.createPartitionSender(partitionId)
	}
	p.mutex.Unlock()
	log4go.Info("Adjust partitionSender and partitionNumber from: %d to %d.",
		p.partitionNumber, newPartitionNumber)
}

func (p *TalosProducer) createPartitionSender(partitionId int32) {
	p.mutex.Lock()
	partitionSender := NewPartitionSender(partitionId, p.topicName,
		p.topicTalosResourceName, p.requestId, p.clientId, p.producerConfig,
		p.talosClientFactory.NewMessageClientDefault(), p.userMessageCallback,
		p.mutex, p)
	p.partitionSenderMap[partitionId] = partitionSender
	p.mutex.Unlock()
}

func (p *TalosProducer) initCheckPartitionTask() {
	p.mutex.Lock()
	// check and update partition number every 3 minutes by default
	// TODO
	p.mutex.Unlock()
}

func (p *TalosProducer) getPartitionId(partitionKey string) int32 {
	return p.partitioner.partition(partitionKey, p.partitionNumber)
}

func (p *TalosProducer) generatePartitionKey() string {
	uid, _ := uuid.NewV4()
	return uid.String()
}

func (p *TalosProducer) setPartitionNumber(partitionNumber int32) {
	p.mutex.Lock()
	p.partitionNumber = partitionNumber
	p.mutex.Unlock()
}

func (p *TalosProducer) checkMessagePartitionKeyValidity(partitionKey string) error {
	if err := utils.CheckNotNull(partitionKey); err != nil {
		return err
	}
	if len(partitionKey) < partitionKeyMinLen ||
		len(partitionKey) > partitionKeyMaxLen {
		return fmt.Errorf("Invalid partition key which length must be"+
			" at least %d and at most %d, but got: %d ",
			partitionKeyMinLen, partitionKeyMaxLen, len(partitionKey))
	}
	return nil
}

func (p *TalosProducer) increaseBufferedCount(incrementNumber int64,
	incrementBytes int64) {
	p.bufferedCount.Increase(incrementNumber, incrementBytes)
}

func (p *TalosProducer) decreaseBufferedCount(decrementNumber int64,
	decrementBytes int64) {
	p.bufferedCount.Decrease(decrementNumber, decrementNumber)
}
