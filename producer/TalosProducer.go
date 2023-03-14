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

	"sync/atomic"

	"github.com/XiaoMi/talos-sdk-golang/admin"
	"github.com/XiaoMi/talos-sdk-golang/client"
	"github.com/XiaoMi/talos-sdk-golang/thrift/auth"
	"github.com/XiaoMi/talos-sdk-golang/thrift/message"
	"github.com/XiaoMi/talos-sdk-golang/thrift/topic"
	"github.com/XiaoMi/talos-sdk-golang/utils"
	"github.com/sirupsen/logrus"
)

var mutex sync.Mutex

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
	requestId                  atomic.Value
	producerState              atomic.Value
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
	talosClientFactory         client.TalosClientFactoryInterface
	talosAdmin                 admin.Admin
	falconWriter               *utils.FalconWriter
	topicName                  string
	partitionNumber            int32
	curPartitionId             int32
	topicTalosResourceName     *topic.TopicTalosResourceName
	scheduleInfoCache          *client.ScheduleInfoCache
	partitionSenderMap         map[int32]*PartitionSender
	BufferFullChan             chan utils.LockState
	checkPartTaskSign          chan utils.StopSign
	monitorTaskSign            chan utils.StopSign
	WaitGroup                  *sync.WaitGroup
	producerLock               sync.Mutex
	log                        *logrus.Logger
}

func NewTalosProducerByProperties(props *utils.Properties,
	topicAbnormalCallback client.TopicAbnormalCallback,
	userMessageCallback UserMessageCallback) (*TalosProducer, error) {

	topicName := props.Get("galaxy.talos.topic.name")
	secretKeyId := props.Get("galaxy.talos.access.key")
	secretKey := props.Get("galaxy.talos.access.secret")
	userType := auth.UserType_DEV_XIAOMI
	// credential
	credential := &auth.Credential{
		TypeA1:      &userType,
		SecretKeyId: &secretKeyId,
		SecretKey:   &secretKey,
	}

	producerConfig := NewTalosProducerConfigByProperties(props)
	return NewDefaultTalosProducer(producerConfig, credential, topicName,
		NewSimplePartitioner(), topicAbnormalCallback, userMessageCallback, utils.InitLogger())
}

func NewTalosProducerWithLogger(filename string, topicAbnormalCallback client.TopicAbnormalCallback,
	userMessageCallback UserMessageCallback, logger *logrus.Logger) (*TalosProducer, error) {
	props := utils.LoadProperties(filename)
	topicName := props.Get("galaxy.talos.topic.name")
	secretKeyId := props.Get("galaxy.talos.access.key")
	secretKey := props.Get("galaxy.talos.access.secret")
	userType := auth.UserType_DEV_XIAOMI
	// credential
	credential := &auth.Credential{
		TypeA1:      &userType,
		SecretKeyId: &secretKeyId,
		SecretKey:   &secretKey,
	}

	producerConfig := NewTalosProducerConfigByProperties(props)
	return NewDefaultTalosProducer(producerConfig, credential, topicName,
		NewSimplePartitioner(), topicAbnormalCallback, userMessageCallback, logger)
}

func NewTalosProducerByFilename(filename string,
	topicAbnormalCallback client.TopicAbnormalCallback,
	userMessageCallback UserMessageCallback) (*TalosProducer, error) {

	props := utils.LoadProperties(filename)
	topicName := props.Get("galaxy.talos.topic.name")
	secretKeyId := props.Get("galaxy.talos.access.key")
	secretKey := props.Get("galaxy.talos.access.secret")
	userType := auth.UserType_DEV_XIAOMI
	// credential
	credential := &auth.Credential{
		TypeA1:      &userType,
		SecretKeyId: &secretKeyId,
		SecretKey:   &secretKey,
	}

	producerConfig := NewTalosProducerConfigByProperties(props)
	return NewDefaultTalosProducer(producerConfig, credential, topicName,
		NewSimplePartitioner(), topicAbnormalCallback, userMessageCallback, utils.InitLogger())
}

func NewDefaultTalosProducer(producerConfig *TalosProducerConfig, credential *auth.Credential,
	topicName string, partitioner Partitioner,
	topicAbnormalCallback client.TopicAbnormalCallback,
	userMessageCallback UserMessageCallback, logger *logrus.Logger) (*TalosProducer, error) {

	var producerState atomic.Value
	var reqId atomic.Value
	producerState.Store(utils.ACTIVE)
	reqId.Store(int64(1))

	bufMsgNum := producerConfig.GetMaxBufferedMsgNumber()
	bufMsgBytes := producerConfig.GetMaxBufferedMsgBytes()
	clientFactory := client.NewTalosClientFactory(producerConfig.TalosClientConfig, credential)
	talosAdmin := admin.NewTalosAdmin(clientFactory)
	describeInfoResponse, err := talosAdmin.GetDescribeInfo(&topic.
		GetDescribeInfoRequest{TopicName: topicName})
	if err != nil {
		return nil, err
	}

	scheduleInfoCache := client.GetScheduleInfoCache(
		describeInfoResponse.GetTopicTalosResourceName(),
		producerConfig.TalosClientConfig,
		clientFactory.NewMessageClientDefault(), clientFactory, logger)

	falconWriter := utils.NewFalconWriter(producerConfig.TalosClientConfig.FalconUrl(), logger)

	p := &TalosProducer{
		requestId:                  reqId,
		producerState:              producerState,
		partitioner:                partitioner,
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
		clientId:                   utils.GenerateClientId(),
		talosClientFactory:         clientFactory,
		falconWriter:               falconWriter,
		talosAdmin:                 talosAdmin,
		topicName:                  topicName,
		topicTalosResourceName:     describeInfoResponse.GetTopicTalosResourceName(),
		scheduleInfoCache:          scheduleInfoCache,
		partitionSenderMap:         make(map[int32]*PartitionSender),
		BufferFullChan:             make(chan utils.LockState, 1),
		checkPartTaskSign:          make(chan utils.StopSign, 1),
		monitorTaskSign:            make(chan utils.StopSign, 1),
		WaitGroup:                  new(sync.WaitGroup),
		log:                        logger,
	}

	p.checkAndGetTopicInfo()

	p.initPartitionSender()

	p.WaitGroup.Add(2)
	go p.initCheckPartitionTask()
	go p.initProducerMonitorTask()
	p.log.Infof("Init a producer for topic: %s, partitions: %d ",
		describeInfoResponse.GetTopicTalosResourceName(),
		p.partitionNumber)

	return p, nil
}

func (p *TalosProducer) AddUserMessageWithTimeout(msgList []*message.Message, timeoutMillis int64) error {
	p.producerLock.Lock()
	defer p.producerLock.Unlock()

	// check producer state
	if !p.IsActive() {
		return fmt.Errorf("Producer is not active, "+
			"current state: %d ", p.producerState)
	}

	// check total buffered message number
	startWaitTime := utils.CurrentTimeMills()
	for p.bufferedCount.IsFull() {
		p.log.Infof("too many buffered messages, globalLock is active."+
			" message number: %d, message bytes: %d",
			p.bufferedCount.GetBufferedMsgNumber(),
			p.bufferedCount.GetBufferedMsgBytes())
		p.producerLock.Unlock()

		// judging wait exit by 'timeout' or 'notify'
		select {
		case p.BufferFullChan <- utils.NOTIFY:
			// if receive notify, just break wait and judge if should addMessage
		case <-time.After(time.Duration(timeoutMillis) * time.Millisecond):
			if utils.CurrentTimeMills()-startWaitTime >= timeoutMillis {
				p.producerLock.Lock()
				return fmt.Errorf("Producer buffer is full and AddUserMessage"+
					" timeout by: %d millis. ", timeoutMillis)
			}
		}

		p.producerLock.Lock()
	}

	if err := p.DoAddUserMessage(msgList); err != nil {
		return err
	}
	return nil
}

func (p *TalosProducer) AddUserMessage(msgList []*message.Message) error {
	p.producerLock.Lock()
	defer p.producerLock.Unlock()

	// check producer state
	if !p.IsActive() {
		return fmt.Errorf("Producer is not active, "+
			"current state: %d ", p.producerState)
	}

	// check total buffered message number
	for p.bufferedCount.IsFull() {
		p.log.Infof("too many buffered messages, globalLock is active."+
			" message number: %d, message bytes: %d",
			p.bufferedCount.GetBufferedMsgNumber(),
			p.bufferedCount.GetBufferedMsgBytes())
		p.producerLock.Unlock()
		p.BufferFullChan <- utils.NOTIFY
		// if receive notify, just break wait and judge if should addMessage
		p.producerLock.Lock()
	}

	if err := p.DoAddUserMessage(msgList); err != nil {
		return err
	}
	return nil
}

func (p *TalosProducer) DoAddUserMessage(msgList []*message.Message) error {
	// user can optionally set 'partitionKey' and 'sequenceNumber' when construct Message
	partitionBufferMap := make(map[int32][]*UserMessage)
	if p.shouldUpdatePartition() {
		p.curPartitionId = (p.curPartitionId + 1) % p.partitionNumber
		p.lastUpdatePartitionIdTime = utils.CurrentTimeMills()
		p.lastAddMsgNumber = 0
	}
	currentPartitionId := p.curPartitionId
	p.lastAddMsgNumber += int64(len(msgList))

	partitionBufferMap[currentPartitionId] = make([]*UserMessage, 0)
	for _, msg := range msgList {
		// set timestamp and messageType if not set;
		utils.UpdateMessage(msg, message.MessageType_BINARY)
		// check data validity
		if err := utils.CheckMessageValidity(msg); err != nil {
			return err
		}

		// check partitionKey setting and validity
		if !msg.IsSetPartitionKey() {
			// straightforward put to cur partitionId queue
			partitionBufferMap[currentPartitionId] = append(partitionBufferMap[currentPartitionId], NewUserMessage(msg))
		} else {
			if err := p.checkMessagePartitionKeyValidity(msg.GetPartitionKey()); err != nil {
				return err
			}
			// construct UserMessage and dispatch to buffer by partitionId
			partitionId := p.getPartitionId(msg.GetPartitionKey())
			if _, ok := partitionBufferMap[partitionId]; !ok {
				partitionBufferMap[partitionId] = make([]*UserMessage, 0)
			}
			partitionBufferMap[partitionId] = append(partitionBufferMap[partitionId], NewUserMessage(msg))
		}
	}

	// add to partitionSender
	for partitionId, usrMsgList := range partitionBufferMap {
		if _, ok := p.partitionSenderMap[partitionId]; !ok {
			return fmt.Errorf("partitionSenderMap not contain partition: %d ", partitionId)
		}
		p.partitionSenderMap[partitionId].AddMessage(usrMsgList)
	}
	return nil
}

// cancel the putMessage threads and checkPartitionTask
// when topic not exist during producer running
func (p *TalosProducer) disableProducer(err error) {
	p.producerLock.Lock()
	defer p.producerLock.Unlock()

	if !p.IsActive() {
		return
	}
	p.producerState.Store(utils.DISABLED)
	p.stopAndwait()
	p.topicAbnormalCallback.AbnormalHandler(p.topicTalosResourceName, err)
}

func (p *TalosProducer) Shutdown() {
	p.producerLock.Lock()
	defer p.producerLock.Unlock()

	if !p.IsActive() {
		return
	}

	p.producerState.Store(utils.SHUTDOWN)
	p.stopAndwait()

	close(p.BufferFullChan)
	close(p.checkPartTaskSign)
	close(p.monitorTaskSign)
	p.WaitGroup.Wait()
}

func (p *TalosProducer) stopAndwait() {
	for _, partitionSender := range p.partitionSenderMap {
		partitionSender.Shutdown()
	}

	p.checkPartTaskSign <- utils.Shutdown
	p.monitorTaskSign <- utils.Shutdown
	p.scheduleInfoCache.Shutdown(p.topicTalosResourceName)
}

func (p *TalosProducer) IsActive() bool {
	return p.producerState.Load() == utils.ACTIVE
}

func (p *TalosProducer) IsDisable() bool {
	return p.producerState.Load() == utils.DISABLED
}

func (p *TalosProducer) IsShutdown() bool {
	return p.producerState.Load() == utils.SHUTDOWN
}

func (p *TalosProducer) shouldUpdatePartition() bool {
	return utils.CurrentTimeMills()-p.lastUpdatePartitionIdTime >=
		p.updatePartitionIdInterval || p.lastAddMsgNumber >=
		p.updatePartitionIdMsgNumber
}

func (p *TalosProducer) checkAndGetTopicInfo() error {
	p.producerLock.Lock()
	defer p.producerLock.Unlock()

	rand.Seed(time.Now().UnixNano())
	resourceNameFromServer, partitionNumber, err := p.DescribeTopicInfo()
	if err != nil {
		return err
	}
	if p.topicTalosResourceName.GetTopicTalosResourceName() !=
		resourceNameFromServer.GetTopicTalosResourceName() {
		return fmt.Errorf("The topic: %s not found ", p.topicTalosResourceName)
	}

	p.setPartitionNumber(partitionNumber)
	p.topicTalosResourceName = resourceNameFromServer
	p.curPartitionId = rand.Int31n(p.partitionNumber)

	p.log.Infof("The client: %s check and get topic info success", p.clientId)
	return nil
}

func (p *TalosProducer) initPartitionSender() {
	p.producerLock.Lock()
	defer p.producerLock.Unlock()

	for partitionId := int32(0); partitionId < p.partitionNumber; partitionId++ {
		p.createPartitionSender(partitionId)
	}
}

func (p *TalosProducer) adjustPartitionSender(newPartitionNumber int32) {
	p.producerLock.Lock()
	defer p.producerLock.Unlock()

	// Note: we do not allow and process 'newPartitionNum < partitionNumber'
	for partitionId := p.partitionNumber; partitionId < newPartitionNumber; partitionId++ {
		p.createPartitionSender(partitionId)
	}
	p.log.Infof("Adjust partitionSender and partitionNumber from: %d to %d.",
		p.partitionNumber, newPartitionNumber)
}

func (p *TalosProducer) createPartitionSender(partitionId int32) {
	partitionSender := NewPartitionSender(partitionId, p.topicName,
		p.topicTalosResourceName, p.requestId, p.clientId, p.producerConfig,
		p.talosClientFactory.NewMessageClientDefault(), p.userMessageCallback, p)
	p.partitionSenderMap[partitionId] = partitionSender
}

func (p *TalosProducer) initCheckPartitionTask() {
	defer p.WaitGroup.Done()
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
			return
		}
	}
}

func (c *TalosProducer) initProducerMonitorTask() {
	defer c.WaitGroup.Done()
	if !c.producerConfig.TalosClientConfig.ClientMonitorSwitch() {
		return
	}
	// push to falcon every 60 seconds delay by default
	duration := time.Duration(c.producerConfig.TalosClientConfig.
		ReportMetricInterval()) * time.Second
	ticker := time.NewTicker(duration)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.ProducerMonitorTask()
		case <-c.monitorTaskSign:
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
	response, err := p.talosAdmin.GetDescribeInfo(
		&topic.GetDescribeInfoRequest{TopicName: p.topicName})
	if err != nil {
		p.log.Errorf("Exception in CheckPartitionTask: %s", err.Error())
		if utils.IsTopicNotExist(err) {
			p.disableProducer(err)
		}
		return
	}

	if p.topicTalosResourceName.GetTopicTalosResourceName() !=
		response.GetTopicTalosResourceName().GetTopicTalosResourceName() {
		err := fmt.Errorf("The topic: %s not exist. It might have been deleted. "+
			"The putMessage goroutine will be cancel. ", p.topicTalosResourceName.
			GetTopicTalosResourceName())
		p.log.Errorf(err.Error())
		p.disableProducer(err)
		return
	}

	topicPartitionNum := response.GetPartitionNumber()
	if p.partitionNumber < topicPartitionNum {
		// increase partitionSender (not allow decreasing)
		p.adjustPartitionSender(topicPartitionNum)
		// update partitionNumber
		p.setPartitionNumber(topicPartitionNum)
	}
	p.log.Infof("CheckPartitionTask done")
}

func (p *TalosProducer) getPartitionId(partitionKey string) int32 {
	return p.partitioner.Partition(partitionKey, p.partitionNumber)
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
	if len(partitionKey) < utils.TALOS_PARTITION_KEY_LENGTH_MINIMAL ||
		len(partitionKey) > utils.TALOS_PARTITION_KEY_LENGTH_MAXIMAL {
		return fmt.Errorf("Invalid partition key which length must be"+
			" at least %d and at most %d, but got: %d ",
			utils.TALOS_PARTITION_KEY_LENGTH_MINIMAL,
			utils.TALOS_PARTITION_KEY_LENGTH_MAXIMAL, len(partitionKey))
	}
	return nil
}

func (p *TalosProducer) IncreaseBufferedCount(incrementNumber int64,
	incrementBytes int64) {
	p.bufferedCount.Increase(incrementNumber, incrementBytes)
}

func (p *TalosProducer) DecreaseBufferedCount(decrementNumber int64,
	decrementBytes int64) {
	p.bufferedCount.Decrease(decrementNumber, decrementBytes)
}

func (p *TalosProducer) DescribeTopicInfo() (*topic.TopicTalosResourceName, int32, error) {
	request := &topic.GetDescribeInfoRequest{TopicName: p.topicName}
	response, err := p.talosAdmin.GetDescribeInfo(request)
	if err != nil {
		p.log.Error(err)
		return nil, -1, err
	}
	return response.GetTopicTalosResourceName(), response.GetPartitionNumber(), nil
}

func (c *TalosProducer) ProducerMonitorTask() {
	c.producerLock.Lock()
	defer c.producerLock.Unlock()
	metrics := make([]*utils.FalconMetric, 0)
	for _, p := range c.partitionSenderMap {
		metrics = append(metrics, p.NewFalconMetrics()...)
	}
	c.falconWriter.PushToFalcon(metrics)
}