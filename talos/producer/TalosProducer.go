/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package producer

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/XiaoMi/talos-sdk-golang/talos/admin"
	"github.com/XiaoMi/talos-sdk-golang/talos/client"
	"github.com/XiaoMi/talos-sdk-golang/talos/thrift/auth"
	"github.com/XiaoMi/talos-sdk-golang/talos/thrift/common"
	"github.com/XiaoMi/talos-sdk-golang/talos/thrift/message"
	"github.com/XiaoMi/talos-sdk-golang/talos/thrift/topic"
	"github.com/XiaoMi/talos-sdk-golang/talos/utils"
	"github.com/XiaoMi/talos-sdk-golang/thrift"
	"github.com/alecthomas/log4go"
	"github.com/nu7hatch/gouuid"
)

type ProducerState int32
type StopSignType int
type LockState int

var mutex sync.Mutex

const (
	ACTIVE ProducerState = iota
	DISABLED
	SHUTDOWN

	partitionKeyMinLen = common.TALOS_PARTITION_KEY_LENGTH_MINIMAL
	partitionKeyMaxLen = common.TALOS_PARTITION_KEY_LENGTH_MAXIMAL

	WAIT   LockState = 0
	NOTIFY LockState = 1

	Shutdown StopSignType = 0
	Running               = 1
)

type Producer interface {
	AddUserMessage(msgList []*message.Message) error
	DoAddUserMessage(msgList []*message.Message) error
	IsActive() bool
	IsDisable() bool
	IsShutdown() bool
	IncreaseBufferedCount(incrementNumber int64, incrementBytes int64)
	DecreaseBufferedCount(decrementNumber int64, decrementBytes int64)
	CheckPartitionTask()
}

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
	talosClientFactory         client.TalosClient
	talosAdmin                 admin.Admin
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
		StopSign:                   make(chan StopSignType, 1),
		checkPartTaskSign:          make(chan StopSignType),
	}

	talosProducer.checkAndGetTopicInfo(topicTalosResourceName)

	talosProducer.initPartitionSender()
	go talosProducer.initCheckPartitionTask()
	log4go.Info("Init a producer for topic: %s, partitions: %d ",
		topicTalosResourceName.GetTopicTalosResourceName(),
		talosProducer.partitionNumber)

	return talosProducer
}

// construct function for test
func NewTalosProducerForTest(producerConfig *TalosProducerConfig,
	talosClientFactory client.TalosClient, talosAdmin admin.Admin,
	topicTalosResourceName *topic.TopicTalosResourceName,
	topicAbnormalCallback client.TopicAbnormalCallback,
	userMessageCallback UserMessageCallback) *TalosProducer {

	bufMsgNum := producerConfig.GetMaxBufferedMsgNumber()
	bufMsgBytes := producerConfig.GetMaxBufferedMsgBytes()

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
		talosClientFactory:         talosClientFactory,
		talosAdmin:                 talosAdmin,
		globalLock:                 make(chan LockState),
		StopSign:                   make(chan StopSignType, 1),
		checkPartTaskSign:          make(chan StopSignType),
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
	// check producer state
	if !p.IsActive() {
		return fmt.Errorf("Producer is not active, current state: %d ", p.producerState)
	}

	// check total buffered message number
	timeout := time.Duration(60 * time.Second)
	for p.bufferedCount.IsFull() {
		log4go.Info("too many buffered messages, globalLock is active."+
			" message number: %d, message bytes: %d",
			p.bufferedCount.GetBufferedMsgNumber(),
			p.bufferedCount.GetBufferedMsgBytes())

		select {
		case signal := <-p.globalLock:
			if signal != NOTIFY {
				err := fmt.Errorf("addUserMessage global lock return wrong. ")
				log4go.Error(err)
				return err
			}
		case <-time.After(timeout):
			err := fmt.Errorf("addUserMessage global lock is waitting. ")
			log4go.Warn(err)
		}
	}
	p.DoAddUserMessage(msgList)
	return nil
}

func (p *TalosProducer) DoAddUserMessage(msgList []*message.Message) error {
	// user can optionally set 'partitionKey' and 'sequenceNumber' when construct Message
	var currentPartitionId int32
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
		// check data validity

		if err := utils.CheckMessageValidity(msg); err != nil {
			log4go.Error("CheckMessageValidity error: %s", err.Error())
			return err
		}

		// check partitionKey setting and validity
		if !msg.IsSetPartitionKey() {
			// straightforward put to cur partitionId queue
			partitionBufferMap[currentPartitionId] = append(partitionBufferMap[currentPartitionId], NewUserMessage(msg))
		} else {
			if err := p.checkMessagePartitionKeyValidity(msg.GetPartitionKey()); err != nil {
				log4go.Error("checkMessagePartitionKeyValidity error: %s", err.Error())
				return err
			}
			log4go.Debug("checkMessagePartitionKeyValidity:%s", msg.GetPartitionKey())
			// construct UserMessage and dispatch to buffer by partitionId
			partitionId := p.getPartitionId(msg.GetPartitionKey())
			log4go.Debug("partitionID: %d", partitionId)
			if _, ok := partitionBufferMap[partitionId]; !ok {
				partitionBufferMap[partitionId] = make([]*UserMessage, 0)
			}
			partitionBufferMap[partitionId] = append(partitionBufferMap[partitionId], NewUserMessage(msg))
		}
	}

	// add to partitionSender
	for partitionId, usrMsgList := range partitionBufferMap {
		if _, ok := p.partitionSenderMap[partitionId]; !ok {
			err := fmt.Errorf("Illegal Argument Error: partitionSenderMap not contain partition: %d ", partitionId)
			log4go.Error(err)
			return err
		}
		log4go.Debug("talos producer addmessage")
		p.partitionSenderMap[partitionId].AddMessage(usrMsgList)
	}
	return nil
}

// cancel the putMessage threads and checkPartitionTask
// when topic not exist during producer running
func (p *TalosProducer) disableProducer(err error) {
	if !p.IsActive() {
		return
	}
	p.producerState = DISABLED
	p.stopAndwait()
	p.topicAbnormalCallback.AbnormalHandler(p.topicTalosResourceName, err)
}

func (p *TalosProducer) Shutdown() {
	if !p.IsActive() {
		return
	}

	p.producerState = SHUTDOWN
	p.stopAndwait()
}

func (p *TalosProducer) stopAndwait() {
	for _, partitionSender := range p.partitionSenderMap {
		partitionSender.Shutdown()
	}

	p.checkPartTaskSign <- Shutdown
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
	if topicTalosResourceName.GetTopicTalosResourceName() != getTopic.GetTopicInfo().
		GetTopicTalosResourceName().GetTopicTalosResourceName() {
		err = fmt.Errorf("The topic: %s not found ",
			topicTalosResourceName.GetTopicTalosResourceName())
		log4go.Error(err)
		return err
	}
	p.partitionNumber = getTopic.GetTopicAttribute().GetPartitionNumber()
	p.curPartitionId = rand.Int31n(p.partitionNumber)
	log4go.Debug("Check and get topic info success")
	return nil
}

func (p *TalosProducer) initPartitionSender() {
	p.partitionSenderMap = make(map[int32]*PartitionSender)
	for partitionId := int32(0); partitionId < p.partitionNumber; partitionId++ {
		p.createPartitionSender(partitionId)
	}
	log4go.Info("init partition sender finished")
}

func (p *TalosProducer) adjustPartitionSender(newPartitionNumber int32) {
	// Note: we do not allow and process 'newPartitionNum < partitionNumber'
	for partitionId := p.partitionNumber; partitionId < newPartitionNumber; partitionId++ {
		p.createPartitionSender(partitionId)
	}
	log4go.Info("Adjust partitionSender and partitionNumber from: %d to %d.",
		p.partitionNumber, newPartitionNumber)
}

func (p *TalosProducer) createPartitionSender(partitionId int32) {
	partitionSender := NewPartitionSender(partitionId, p.topicName,
		p.topicTalosResourceName, p.requestId, p.clientId, p.producerConfig,
		p.talosClientFactory.NewMessageClientDefault(), p.userMessageCallback,
		p.globalLock, p)
	p.partitionSenderMap[partitionId] = partitionSender
}

func (p *TalosProducer) initCheckPartitionTask() {
	// check and update partition number every 3 minutes by default
	duration := time.Duration(p.producerConfig.GetCheckPartitionInterval()) *
		time.Millisecond
	ticker := time.NewTicker(duration)
	defer ticker.Stop()
	log4go.Debug("start check partition Task")
	for {
		select {
		case <-ticker.C:
			p.CheckPartitionTask()
		case <-p.checkPartTaskSign:
			p.StopSign <- Shutdown
			return
		}
	}
}

/**
 * Check Partition Task
 *
 * if partition number change, invoke ReBalanceTask
 */
func (p *TalosProducer) CheckPartitionTask() {
	getTopic, err := p.talosAdmin.DescribeTopic(&topic.DescribeTopicRequest{TopicName: p.topicName})
	if err != nil {
		log4go.Error("Exception in CheckPartitionTask: %s", err.Error())
		// if error is TopicNotExist, cancel all reading task
		// TODO:add errorCode check , choose operation for different error
		//if utils.IsTopicNotExist(err) {
		//  p.disableProducer(err)
		//}
		return
	}
	log4go.Debug("describe topic %s success", getTopic.TopicInfo.TopicName)

	if p.topicTalosResourceName.GetTopicTalosResourceName() !=
		getTopic.GetTopicInfo().GetTopicTalosResourceName().GetTopicTalosResourceName() {
		err := fmt.Errorf("The topic: %s not exist. It might have been deleted. "+
			"The putMessage goroutine will be cancel. ", p.topicTalosResourceName.
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
	log4go.Debug("Check partition Task finished")
}

func (p *TalosProducer) getPartitionId(partitionKey string) int32 {
	return p.partitioner.Partition(partitionKey, p.partitionNumber)
}

func (p *TalosProducer) generatePartitionKey() string {
	uid, _ := uuid.NewV4()
	return uid.String()
}

func (p *TalosProducer) setPartitionNumber(partitionNumber int32) {
	mutex.Lock()
	defer mutex.Unlock()
	p.partitionNumber = partitionNumber
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

func (p *TalosProducer) IncreaseBufferedCount(incrementNumber int64,
	incrementBytes int64) {
	p.bufferedCount.Increase(incrementNumber, incrementBytes)
}

func (p *TalosProducer) DecreaseBufferedCount(decrementNumber int64,
	decrementBytes int64) {
	p.bufferedCount.Decrease(decrementNumber, decrementNumber)
}
