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
	"github.com/XiaoMi/talos-sdk-golang/talos/thrift/message"
	"github.com/XiaoMi/talos-sdk-golang/talos/thrift/topic"
	"github.com/XiaoMi/talos-sdk-golang/thrift"
	"github.com/alecthomas/log4go"
	"github.com/nu7hatch/gouuid"
	"math/rand"
	"sync"
	"time"
)

type ProducerState int32
type StopSignType int
type LockState int

var mutex sync.Mutex

const (
	ACTIVE ProducerState = iota
	DISABLED
	SHUTDOWN
)

const (
	partitionKeyMinLen = common.TALOS_PARTITION_KEY_LENGTH_MINIMAL
	partitionKeyMaxLen = common.TALOS_PARTITION_KEY_LENGTH_MAXIMAL
)

const (
	WAIT   LockState = 0
	NOTIFY LockState = 1
)
const (
	shutdown StopSignType = iota // 0
	running
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
	globalLock                 chan LockState
	StopSign                   chan StopSignType
	checkPartTaskSign          chan StopSignType
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
		globalLock:                 make(chan LockState),
	}

	talosProducer.checkAndGetTopicInfo(topicTalosResourceName)

	talosProducer.initPartitionSender()
	go talosProducer.initCheckPartitionTask()
	log4go.Info("Init a producer for topic: %s, partitions: %d ",
		topicTalosResourceName.GetTopicTalosResourceName(),
		talosProducer.partitionNumber)

	return talosProducer
}

func (p *TalosProducer) AddUserMessage(msgList []*message.Message) error {
	mutex.Lock()
	// check producer state
	if !p.IsActive() {
		return fmt.Errorf("Producer is not active, current state: %d ", p.producerState)
	}

	// check total buffered message number
	for p.bufferedCount.IsFull() {
		log4go.Info("too many buffered messages, globalLock is active."+
			" message number: %d, message bytes: %d",
			p.bufferedCount.GetBufferedMsgNumber(),
			p.bufferedCount.GetBufferedMsgBytes())

		if <-p.globalLock != NOTIFY {
			err := fmt.Errorf("addUserMessage global lock wait is interrupt. ")
			log4go.Error(err)
			return err
		}
	}

	p.DoAddUserMessage(msgList)
	mutex.Unlock()
	return nil
}

func (p *TalosProducer) DoAddUserMessage(msgList []*message.Message) error {
	var currentPartitionId int32
	mutex.Lock()
	partitionBufferMap := make(map[int32][]*UserMessage)
	if p.shouldUpdatePartition() {
		p.curPartitionId = (p.curPartitionId + 1) % p.partitionNumber
		p.lastUpdatePartitionIdTime = utils.CurrentTimeMills()
		p.lastAddMsgNumber = 0
	}
	currentPartitionId = p.curPartitionId
	p.lastAddMsgNumber += int64(len(msgList))

	partitionBufferMap[currentPartitionId] = make([]*UserMessage, 0)

	for _, msg := range msgList {
		// set timestamp and messageType if not set;
		utils.UpdateMessage(msg, message.MessageType_BINARY)
		utils.UpdateMessage(msg, message.MessageType_BINARY)
		// check data validity
		utils.CheckMessageValidity(msg)

		// check partitionKey setting and validity
		if !msg.IsSetPartitionKey() {
			// straightforward put to cur partitionId queue
			partitionBufferMap[currentPartitionId] = append(
				partitionBufferMap[currentPartitionId], NewUserMessage(msg))
		} else {
			p.checkMessagePartitionKeyValidity(msg.GetPartitionKey())
			// construct UserMessage and dispatch to buffer by partitionId
			partitionId := p.getPartitionId(msg.GetPartitionKey())
			if _, ok := partitionBufferMap[partitionId]; !ok {
				partitionBufferMap[partitionId] = make([]*UserMessage, 0)
			}
			partitionBufferMap[partitionId] = append(
				partitionBufferMap[partitionId], NewUserMessage(msg))
		}
	}

	// add to partitionSender
	for partitionId, usrMsg := range partitionBufferMap {
		if _, ok := p.partitionSenderMap[partitionId]; !ok {
			return fmt.Errorf("Illegal Argument Error! ")
		}
		p.partitionSenderMap[partitionId].AddMessage(usrMsg)
	}
	mutex.Unlock()
	return nil
}

// cancel the putMessage threads and checkPartitionTask
// when topic not exist during producer running
func (p *TalosProducer) disableProducer(err error) {
	mutex.Lock()
	if !p.IsActive() {
		return
	}
	p.producerState = DISABLED
	p.stopAndwait()
	p.topicAbnormalCallback.AbnormalHandler(p.topicTalosResourceName, err)
	mutex.Unlock()
}

func (p *TalosProducer) shutdown() {
	mutex.Lock()
	if !p.IsActive() {
		return
	}

	p.producerState = SHUTDOWN
	p.stopAndwait()
	mutex.Unlock()
}

func (p *TalosProducer) stopAndwait() {
	for _, partitionSender := range p.partitionSenderMap {
		partitionSender.Shutdown()
	}
	// TODO partitioncheck and messageCallBack executor shutdown
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
	mutex.Lock()
	p.topicName, err = utils.GetTopicNameByResourceName(
		topicTalosResourceName.GetTopicTalosResourceName())
	if err != nil {
		log4go.Error("Check and get TopicInfo error: %s", err.Error())
		return err
	}
	getTopic, err := p.talosAdmin.DescribeTopic(
		&topic.DescribeTopicRequest{TopicName: p.topicName})
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
	mutex.Unlock()
	return nil
}

func (p *TalosProducer) initPartitionSender() {
	mutex.Lock()
	for partitionId := int32(0); partitionId < p.partitionNumber; partitionId++ {
		p.createPartitionSender(partitionId)
	}
	mutex.Unlock()
}

func (p *TalosProducer) adjustPartitionSender(newPartitionNumber int32) {
	// Note: we do not allow and process 'newPartitionNum < partitionNumber'
	mutex.Lock()
	for partitionId := p.partitionNumber; partitionId < newPartitionNumber; partitionId++ {
		p.createPartitionSender(partitionId)
	}
	mutex.Unlock()
	log4go.Info("Adjust partitionSender and partitionNumber from: %d to %d.",
		p.partitionNumber, newPartitionNumber)
}

func (p *TalosProducer) createPartitionSender(partitionId int32) {
	mutex.Lock()
	partitionSender := NewPartitionSender(partitionId, p.topicName,
		p.topicTalosResourceName, p.requestId, p.clientId, p.producerConfig,
		p.talosClientFactory.NewMessageClientDefault(), p.userMessageCallback,
		p.globalLock, p)
	p.partitionSenderMap[partitionId] = partitionSender
	mutex.Unlock()
}

func (p *TalosProducer) initCheckPartitionTask() {
	mutex.Lock()
	// check and update partition number every 3 minutes by default
	duration := time.Duration(p.producerConfig.GetCheckPartitionInterval()) *
		time.Millisecond
	ticker := time.NewTicker(duration)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			p.CheckPartitionTask()
		case <-p.checkPartTaskSign:
			p.StopSign <- shutdown
			return
		}
	}
	mutex.Unlock()
}

func (p *TalosProducer) getPartitionId(partitionKey string) int32 {
	return p.partitioner.partition(partitionKey, p.partitionNumber)
}

func (p *TalosProducer) generatePartitionKey() string {
	uid, _ := uuid.NewV4()
	return uid.String()
}

func (p *TalosProducer) setPartitionNumber(partitionNumber int32) {
	mutex.Lock()
	p.partitionNumber = partitionNumber
	mutex.Unlock()
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

/**
 * Check Partition Task
 *
 * if partition number change, invoke ReBalanceTask
 */
func (p *TalosProducer) CheckPartitionTask() {
	getTopic, err := p.talosAdmin.DescribeTopic(
		&topic.DescribeTopicRequest{TopicName: p.topicName})
	if err != nil {
		log4go.Error("Exception in CheckPartitionTask: %s", err.Error())
		// if error is TopicNotExist, cancel all reading task
		// TODO:add errorCode check , choose operation for different error
		//if utils.IsTopicNotExist(err) {
		//  p.disableProducer(err)
		//}
		return
	}
	if p.topicTalosResourceName !=
		getTopic.GetTopicInfo().GetTopicTalosResourceName() {
		err := fmt.Errorf("The topic: %s not exist. It might have been deleted. "+
			"The putMessage threads will be cancel. ", p.topicTalosResourceName.
			GetTopicTalosResourceName())
		log4go.Error(err)
		p.disableProducer(err)
		return
	}

	topicPartitionNum := getTopic.GetTopicAttribute().GetPartitionNumber()
	if p.partitionNumber < topicPartitionNum {
		// increase partitionSender (not allow decreasing)
		p.adjustPartitionSender(topicPartitionNum)
		// update partitionNumber
		p.setPartitionNumber(topicPartitionNum)
	}
}
