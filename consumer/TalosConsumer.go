/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package consumer

import (
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/XiaoMi/talos-sdk-golang/admin"
	"github.com/XiaoMi/talos-sdk-golang/client"
	"github.com/XiaoMi/talos-sdk-golang/thrift/auth"
	"github.com/XiaoMi/talos-sdk-golang/thrift/consumer"
	"github.com/XiaoMi/talos-sdk-golang/thrift/topic"
	"github.com/XiaoMi/talos-sdk-golang/utils"
	"github.com/sirupsen/logrus"
)

type WorkerPair struct {
	workerId        string
	hasPartitionNum int
}

func NewWorkerPair(workerId string, hasPartitionNum int) WorkerPair {
	return WorkerPair{workerId, hasPartitionNum}
}

func (p WorkerPair) String() string {
	str := fmt.Sprintf("{'%s', %d}", p.workerId, p.hasPartitionNum)
	return str
}

type WorkerPairs []WorkerPair

func (p WorkerPairs) Len() int {
	return len(p)
}

// descending sort
func (p WorkerPairs) Less(i, j int) bool {
	if p[i].hasPartitionNum > p[j].hasPartitionNum {
		return true
	}

	if p[i].hasPartitionNum < p[j].hasPartitionNum {
		return false
	}

	return p[i].workerId > p[j].workerId
}

func (p WorkerPairs) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

type TalosConsumer struct {
	workerId                string
	consumerGroup           string
	messageProcessorFactory MessageProcessorFactory
	MessageReaderFactory    *TalosMessageReaderFactory
	partitionFetcherMap     map[int32]*PartitionFetcher
	talosConsumerConfig     *TalosConsumerConfig
	talosClientFactory      client.TalosClientFactoryInterface
	scheduleInfoCache       *client.ScheduleInfoCache
	talosAdmin              admin.Admin
	consumerClient          consumer.ConsumerService
	topicAbnormalCallback   client.TopicAbnormalCallback
	readWriteLock           sync.RWMutex
	falconWriter            *utils.FalconWriter
	// init by getting from rpc call as follows
	topicName              string
	partitionNumber        int
	topicTalosResourceName *topic.TopicTalosResourceName
	workerInfoMap          map[string][]int32
	partitionCheckpoint    map[int32]Long
	checkPartTaskChan      chan utils.StopSign
	checkWorkerTaskChan    chan utils.StopSign
	renewTaskChan          chan utils.StopSign
	monitorTaskChan        chan utils.StopSign
	WaitGroup              *sync.WaitGroup
	log                    *logrus.Logger
}

func NewTalosConsumerByProperties(properties *utils.Properties,
	messageProcessorFactory MessageProcessorFactory,
	abnormalCallback client.TopicAbnormalCallback) (*TalosConsumer, error) {

	topicName := properties.Get("galaxy.talos.topic.name")
	consumerGroupName := properties.Get("galaxy.talos.group.name")
	clientIdPrefix := properties.Get("galaxy.talos.client.prefix")
	secretKeyId := properties.Get("galaxy.talos.access.key")
	secretKey := properties.Get("galaxy.talos.access.secret")
	userType := auth.UserType_DEV_XIAOMI
	// credential
	credential := &auth.Credential{
		TypeA1:      &userType,
		SecretKeyId: &secretKeyId,
		SecretKey:   &secretKey,
	}
	consumerConfig := NewTalosConsumerConfigByProperties(properties)

	return NewDefaultTalosConsumer(consumerGroupName, consumerConfig, credential,
		topicName, NewTalosMessageReaderFactory(), messageProcessorFactory,
		clientIdPrefix, abnormalCallback, make(map[int32]Long, 0), utils.InitLogger())
}

func NewTalosConsumerByFilename(propertyFilename string,
	messageProcessorFactory MessageProcessorFactory,
	abnormalCallback client.TopicAbnormalCallback) (*TalosConsumer, error) {

	props := utils.LoadProperties(propertyFilename)
	topicName := props.Get("galaxy.talos.topic.name")
	consumerGroupName := props.Get("galaxy.talos.group.name")
	clientIdPrefix := props.Get("galaxy.talos.client.prefix")
	secretKeyId := props.Get("galaxy.talos.access.key")
	secretKey := props.Get("galaxy.talos.access.secret")
	userType := auth.UserType_DEV_XIAOMI
	// credential
	credential := &auth.Credential{
		TypeA1:      &userType,
		SecretKeyId: &secretKeyId,
		SecretKey:   &secretKey,
	}
	consumerConfig := NewTalosConsumerConfigByProperties(props)

	return NewDefaultTalosConsumer(consumerGroupName, consumerConfig, credential,
		topicName, NewTalosMessageReaderFactory(), messageProcessorFactory,
		clientIdPrefix, abnormalCallback, make(map[int32]Long, 0), utils.InitLogger())
}

func NewTalosConsumerWithLogger(propertyFilename string,
	messageProcessorFactory MessageProcessorFactory,
	abnormalCallback client.TopicAbnormalCallback, logger *logrus.Logger) (*TalosConsumer, error) {

	props := utils.LoadProperties(propertyFilename)
	topicName := props.Get("galaxy.talos.topic.name")
	consumerGroupName := props.Get("galaxy.talos.group.name")
	clientIdPrefix := props.Get("galaxy.talos.client.prefix")
	secretKeyId := props.Get("galaxy.talos.access.key")
	secretKey := props.Get("galaxy.talos.access.secret")
	userType := auth.UserType_DEV_XIAOMI
	// credential
	credential := &auth.Credential{
		TypeA1:      &userType,
		SecretKeyId: &secretKeyId,
		SecretKey:   &secretKey,
	}
	consumerConfig := NewTalosConsumerConfigByProperties(props)

	return NewDefaultTalosConsumer(consumerGroupName, consumerConfig, credential,
		topicName, NewTalosMessageReaderFactory(), messageProcessorFactory,
		clientIdPrefix, abnormalCallback, make(map[int32]Long, 0), logger)
}

func NewTalosConsumer(consumerGroupName string, consumerConfig *TalosConsumerConfig,
	credential *auth.Credential, topicName string,
	messageProcessorFactory MessageProcessorFactory, clientIdPrefix string,
	abnormalCallback client.TopicAbnormalCallback, logger *logrus.Logger) (*TalosConsumer, error) {
	return NewDefaultTalosConsumer(consumerGroupName, consumerConfig, credential,
		topicName, NewTalosMessageReaderFactory(), messageProcessorFactory,
		clientIdPrefix, abnormalCallback, make(map[int32]Long, 0), logger)
}

func NewDefaultTalosConsumer(consumerGroupName string, consumerConfig *TalosConsumerConfig,
	credential *auth.Credential, topicName string,
	messageReaderFactory *TalosMessageReaderFactory,
	messageProcessorFactory MessageProcessorFactory, clientIdPrefix string,
	abnormalCallback client.TopicAbnormalCallback,
	partitionCheckpoint map[int32]Long, logger *logrus.Logger) (*TalosConsumer, error) {

	workerId, err := utils.CheckAndGenerateClientId(clientIdPrefix)
	if err != nil {
		return nil, err
	}

	err = utils.CheckNameValidity(consumerGroupName)
	if err != nil {
		return nil, err
	}

	talosClientFactory := client.NewTalosClientFactory(consumerConfig.
		TalosClientConfig, credential)
	consumerClient := talosClientFactory.NewConsumerClientDefault()

	talosAdmin := admin.NewTalosAdmin(talosClientFactory)
	response, err := talosAdmin.GetDescribeInfo(&topic.GetDescribeInfoRequest{TopicName: topicName})
	if err != nil {
		return nil, err
	}
	topicTalosResourceName := response.GetTopicTalosResourceName()

	scheduleInfoCache := client.GetScheduleInfoCache(
		topicTalosResourceName, consumerConfig.TalosClientConfig,
		talosClientFactory.NewMessageClientDefault(), talosClientFactory, logger)

	falconWriter := utils.NewFalconWriter(consumerConfig.TalosClientConfig.FalconUrl(), logger)

	c := &TalosConsumer{
		workerId:                workerId,
		consumerGroup:           consumerGroupName,
		messageProcessorFactory: messageProcessorFactory,
		MessageReaderFactory:    messageReaderFactory,
		partitionFetcherMap:     make(map[int32]*PartitionFetcher),
		talosConsumerConfig:     consumerConfig,
		talosClientFactory:      talosClientFactory,
		talosAdmin:              talosAdmin,
		topicName:               topicName,
		topicTalosResourceName:  topicTalosResourceName,
		consumerClient:          consumerClient,
		topicAbnormalCallback:   abnormalCallback,
		partitionCheckpoint:     partitionCheckpoint,
		scheduleInfoCache:       scheduleInfoCache,
		falconWriter:            falconWriter,
		checkPartTaskChan:       make(chan utils.StopSign),
		checkWorkerTaskChan:     make(chan utils.StopSign),
		renewTaskChan:           make(chan utils.StopSign),
		monitorTaskChan:         make(chan utils.StopSign),
		WaitGroup:               new(sync.WaitGroup),
		log:                     logger,
	}
	c.log.Infof("The worker: %s is initializing...", workerId)

	// check and get topic info such as partitionNumber
	err = c.checkAndGetTopicInfo()
	if err != nil {
		return nil, err
	}

	// register self workerId
	err = c.registerSelf()
	if err != nil {
		return nil, err
	}

	// get worker info
	err = c.getWorkerInfo()
	if err != nil {
		return nil, err
	}

	// do balance and init simple consumer
	c.makeBalance()

	// start CheckPartitionTask/CheckWorkerInfoTask/RenewTask/MonitorTask
	c.WaitGroup.Add(4)

	go c.initCheckPartitionTask()
	go c.initCheckWorkerInfoTask()
	go c.initRenewTask()
	go c.initConsumerMonitorTask()

	return c, nil
}

func (c *TalosConsumer) checkAndGetTopicInfo() error {
	resourceNameFromServer, partitionNumber, err := c.DescribeTopicInfo()
	if err != nil {
		return err
	}

	if c.topicTalosResourceName.GetTopicTalosResourceName() !=
		resourceNameFromServer.GetTopicTalosResourceName() {
		return fmt.Errorf("The topic: %s not found ", c.topicTalosResourceName)
	}

	c.setPartitionNumber(partitionNumber)
	c.topicTalosResourceName = resourceNameFromServer
	c.log.Infof("The worker: %s check and get topic info done", c.workerId)
	return nil
}

func (c *TalosConsumer) registerSelf() error {
	consumeUnit := &consumer.ConsumeUnit{
		ConsumerGroup:          c.consumerGroup,
		TopicTalosResourceName: c.topicTalosResourceName,
		PartitionIdList:        make([]int32, 0),
		WorkerId:               c.workerId,
	}
	lockWorkerRequest := &consumer.LockWorkerRequest{ConsumeUnit: consumeUnit}
	lockWorkerResponse := consumer.NewLockWorkerResponse()

	var err error
	tryCount := c.talosConsumerConfig.GetSelfRegisterMaxRetry() + 1
	for tryCount > 0 {
		tryCount--
		lockWorkerResponse, err = c.consumerClient.LockWorker(lockWorkerRequest)
		if err != nil {
			continue
		}
		if lockWorkerResponse.GetRegisterSuccess() {
			c.log.Infof("The worker: %s register self success", c.workerId)
			return nil
		}
		c.log.Debugf("The worker: %s register self failed, make %d retry",
			c.workerId, tryCount+1)
	}
	return fmt.Errorf("The worker: %s register self failed: %s ",
		c.workerId, err.Error())
}

func (c *TalosConsumer) getWorkerInfo() error {
	queryWorkerRequest := &consumer.QueryWorkerRequest{
		ConsumerGroup:          c.consumerGroup,
		TopicTalosResourceName: c.topicTalosResourceName,
	}
	queryWorkerResponse, err := c.consumerClient.QueryWorker(queryWorkerRequest)
	if err != nil {
		return err
	}

	// if queryWorkerInfoMap size equals 0,
	// it represents hbase failed error, do not update local map
	// because registration, the queryWorkerInfoMap size >= 1 at least
	// if queryWorkerInfoMap not contains self, it indicates renew failed,
	// do not update local map to prevent a bad re-balance
	if _, ok := queryWorkerResponse.GetWorkerMap()[c.workerId]; !ok ||
		len(queryWorkerResponse.GetWorkerMap()) == 0 {
		return fmt.Errorf("query workerInfo failed, don't update local map")
	}
	c.readWriteLock.Lock()
	c.workerInfoMap = queryWorkerResponse.GetWorkerMap()
	c.readWriteLock.Unlock()
	return nil
}

func (c *TalosConsumer) calculateTargetList(copyPartitionNum, workerNumber int,
	targetList *[]int) error {
	if workerNumber == 1 {
		// one worker serving all partitions
		*targetList = append(*targetList, copyPartitionNum)
	} else if copyPartitionNum < workerNumber {
		// per worker per partition, the extra worker must be idle
		for i := 0; i < copyPartitionNum; i++ {
			*targetList = append(*targetList, 1)
		}
	} else {
		// calculate the target sequence
		sum := 0
		min := copyPartitionNum / workerNumber
		remainder := copyPartitionNum % workerNumber
		// add max by remainder
		for i := 0; i < remainder; i++ {
			*targetList = append(*targetList, min+1)
			sum += min + 1
		}

		// add min by (workerNumber - remainder)
		for i := 0; i < workerNumber-remainder; i++ {
			*targetList = append(*targetList, min)
			sum += min
		}
		if sum != copyPartitionNum {
			return fmt.Errorf("illegal partition number: %d", sum)
		}
	}

	// sort target by descending
	sort.Sort(sort.Reverse(sort.IntSlice(*targetList)))
	c.log.Infof("Worker: %s calculate target partitions done: %v",
		c.workerId, *targetList)
	return nil
}

func (c *TalosConsumer) calculateWorkerPairs(copyWorkerMap map[string][]int32,
	sortedWorkerPairs *WorkerPairs) {
	for workerId, partitionIdList := range copyWorkerMap {
		*sortedWorkerPairs = append(*sortedWorkerPairs,
			NewWorkerPair(workerId, len(partitionIdList)))
	}
	sort.Sort(sortedWorkerPairs) // descending
	c.log.Infof("worker: %s calculate sorted worker pairs: %v",
		c.workerId, sortedWorkerPairs)
}

func (c *TalosConsumer) makeBalance() {
	/**
	 * When start make balance, we deep copy 'partitionNumber' and 'workerInfoMap'
	 * to prevent both value appear inconsistent during the process makeBalance
	 */
	copyPartitionNum := c.partitionNumber
	copyWorkerInfoMap := c.deepCopyWorkerInfoMap()

	/**
	 * if workerInfoMap not contains workerId, there must be error in renew task.
	 * the renew task will cancel the consuming task and stop to read data,
	 * so just return and do not care balance.
	 */
	if _, ok := copyWorkerInfoMap[c.workerId]; !ok {
		c.log.Errorf("WorkerInfoMap not contains worker: %s. There may be some error"+
			" for renew task.", c.workerId)
		return
	}

	// calculate target and sorted worker pairs
	targetList := make([]int, 0)
	sortedWorkerPairs := make(WorkerPairs, 0)
	c.calculateTargetList(copyPartitionNum, len(copyWorkerInfoMap), &targetList)
	c.calculateWorkerPairs(copyWorkerInfoMap, &sortedWorkerPairs)
	//judge stealing or release partition
	toStealList := make([]int32, 0)
	toReleaseList := make([]int32, 0)

	for i := 0; i < sortedWorkerPairs.Len(); i++ {
		if sortedWorkerPairs[i].workerId == c.workerId {
			hasList := c.getHasList()
			has := len(hasList)

			// release all reduced partition and wait next cycle to balance
			if len(hasList) > 0 && int(hasList[len(hasList) - 1]) >= c.partitionNumber {
				for _, partitionId := range hasList {
					if int(partitionId) >= c.partitionNumber {
						toReleaseList = append(toReleaseList, partitionId)
					}
				}
				break
			}

			// workerNum > partitionNum, idle workers have no match target, do nothing
			if i >= len(targetList) {
				break
			}
			target := targetList[i]

			c.log.Infof("Worker: %s has: %d partition, target: %d", c.workerId, has, target)

			// a balanced state, do nothing
			if has == target {
				break
			} else if has > target {
				//release partitions
				toReleaseNum := has - target
				for toReleaseNum > 0 && len(hasList) > 0 {
					toReleaseList = append(toReleaseList, hasList[0])
					hasList = hasList[1:]
					toReleaseNum--
				}
			} else {
				// stealing partitions
				idlePartitions := c.getIdlePartitions()
				if len(idlePartitions) > 0 {
					toStealNum := target - has
					rand.Seed(time.Now().UnixNano())
					for toStealNum > 0 && len(idlePartitions) > 0 {
						randomIndex := rand.Intn(len(idlePartitions))
						toStealList = append(toStealList, idlePartitions[randomIndex])
						idlePartitions = append(idlePartitions[:randomIndex], idlePartitions[randomIndex+1:]...)
						toStealNum--
					}
				}
			} // else
			break
		}
	}

	// steal or release partition lock or reached a balance state
	if len(toStealList) > 0 && len(toReleaseList) > 0 {
		c.log.Errorf("make balance error: both toStealList and toReleaseList exist")
		return
	}
	if len(toStealList) > 0 {
		c.stealPartitionLock(toStealList)
	} else if len(toReleaseList) > 0 {
		c.releasePartitionLock(toReleaseList)
	} else {
		// do nothing when reach balance state
		c.log.Infof("The worker: %s have reached a balanced state.", c.workerId)
	}
}

func (c *TalosConsumer) stealPartitionLock(toStealList []int32) {
	c.log.Infof("Worker: %s try to steal %d partition: %v",
		c.workerId, len(toStealList), toStealList)
	// try to lock and invoke serving partition PartitionFetcher to 'LOCKED' state
	url := c.talosConsumerConfig.ServiceEndpoint() + utils.TALOS_MESSAGE_SERVICE_PATH
	c.readWriteLock.Lock()
	for _, partitionId := range toStealList {
		if _, ok := c.partitionFetcherMap[partitionId]; !ok {
			// Note 'partitionCheckPoint.get(partitionId)' may be null, it's ok
			messageReader := c.MessageReaderFactory.CreateMessageReader(c.talosConsumerConfig, c.log)
			partitionFetcher := NewPartitionFetcher(c.consumerGroup, c.topicName,
				c.topicTalosResourceName, partitionId, c.talosConsumerConfig,
				c.workerId, c.consumerClient, c.scheduleInfoCache,
				c.talosClientFactory.NewMessageClient(url),
				c.messageProcessorFactory.CreateProcessor(),
				messageReader, c.partitionCheckpoint[partitionId], c.log)
			c.partitionFetcherMap[partitionId] = partitionFetcher
		}
		c.partitionFetcherMap[partitionId].Lock()
	}
	c.readWriteLock.Unlock()
}

func (c *TalosConsumer) releasePartitionLock(toReleaseList []int32) {
	c.log.Infof("Worker: %s try to release %d parition: %v",
		c.workerId, len(toReleaseList), toReleaseList)
	// stop read, commit offset, unlock the partition async
	for _, partitionId := range toReleaseList {
		if _, ok := c.partitionFetcherMap[partitionId]; !ok {
			c.log.Errorf("partitionFetcher map not contains partition: %d", partitionId)
			continue
		}
		c.partitionFetcherMap[partitionId].Unlock()
	}
}

func (c *TalosConsumer) setPartitionNumber(partitionNum int32) {
	c.readWriteLock.Lock()
	c.partitionNumber = int(partitionNum)
	c.readWriteLock.Unlock()
}

func (c *TalosConsumer) getIdlePartitions() []int32 {
	if c.partitionNumber < 0 {
		c.log.Errorf("consumer has error partition num: %d", c.partitionNumber)
		return nil
	}
	idlePartitions := make([]int32, 0)

	c.readWriteLock.Lock()
	defer c.readWriteLock.Unlock()

	for i := 0; i < c.partitionNumber; i++ {
		idlePartitions = append(idlePartitions, int32(i))
	}

	for _, partitionIdList := range c.workerInfoMap {
		for _, servePartitionId := range partitionIdList {
			for j, idlePartitionId := range idlePartitions {
				if servePartitionId == idlePartitionId {
					idlePartitions = append(idlePartitions[:j], idlePartitions[j+1:]...)
					break
				}
			}
		}
	}
	return idlePartitions
}

func (c *TalosConsumer) getHasList() []int32 {
	hasList := make([]int32, 0)
	c.readWriteLock.Lock()
	defer c.readWriteLock.Unlock()
	for partitionId, partitionFetcher := range c.partitionFetcherMap {
		if partitionFetcher.IsServing() {
			hasList = append(hasList, partitionId)
		}
	}
	return hasList
}

func (c *TalosConsumer) cancelAllConsumingTask() {
	c.releasePartitionLock(c.getHasList())
}

func (c *TalosConsumer) shutDownAllFetcher() {
	for _, partitionFetcher := range c.partitionFetcherMap {
		partitionFetcher.Shutdown()
	}
}

func (c *TalosConsumer) ShutDown() {
	c.log.Infof("Worker: %s is shutting down...", c.workerId)
	c.checkWorkerTaskChan <- utils.Shutdown
	c.checkPartTaskChan <- utils.Shutdown
	c.shutDownAllFetcher()
	c.renewTaskChan <- utils.Shutdown
	c.monitorTaskChan <- utils.Shutdown
	close(c.checkWorkerTaskChan)
	close(c.checkPartTaskChan)
	close(c.renewTaskChan)
	close(c.monitorTaskChan)
	c.WaitGroup.Wait()
	c.log.Infof("Worker: %s is shutdown.", c.workerId)
}

func (c *TalosConsumer) deepCopyWorkerInfoMap() map[string][]int32 {
	copyMap := make(map[string][]int32, len(c.workerInfoMap))
	c.readWriteLock.Lock()
	defer c.readWriteLock.Unlock()
	for workerId, partitionIdList := range c.workerInfoMap {
		copyMap[workerId] = partitionIdList
	}
	return copyMap
}

func (c *TalosConsumer) DescribeTopicInfo() (*topic.TopicTalosResourceName, int32, error) {
	request := &topic.GetDescribeInfoRequest{TopicName: c.topicName}
	response, err := c.talosAdmin.GetDescribeInfo(request)
	if err != nil {
		return nil, -1, err
	}
	return response.GetTopicTalosResourceName(), response.GetPartitionNumber(), nil
}

func (c *TalosConsumer) getRenewPartitionList() []int32 {
	toRenewList := make([]int32, 0)
	c.readWriteLock.RLock()
	for partitionId, partitionFetcher := range c.partitionFetcherMap {
		if partitionFetcher.IsHoldingLock() {
			toRenewList = append(toRenewList, partitionId)
		}
	}
	c.readWriteLock.RUnlock()
	return toRenewList
}

func (c *TalosConsumer) initCheckPartitionTask() {
	defer c.WaitGroup.Done()
	// check and update partition number every 1 minutes delay by default
	duration := time.Duration(c.talosConsumerConfig.
		GetPartitionCheckInterval()) * time.Millisecond
	ticker := time.NewTicker(duration)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.CheckPartitionTask()
		case <-c.checkPartTaskChan:
			return
		}
	}
}

func (c *TalosConsumer) initCheckWorkerInfoTask() {
	defer c.WaitGroup.Done()
	// check worker info every 10 seconds delay by default
	duration := time.Duration(c.talosConsumerConfig.
		GetWorkerInfoCheckInterval()) * time.Millisecond
	ticker := time.NewTicker(duration)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.CheckWorkerInfoTask()
		case <-c.checkWorkerTaskChan:
			return
		}
	}
}

func (c *TalosConsumer) initRenewTask() {
	defer c.WaitGroup.Done()
	// renewTask every 7 seconds delay by default
	duration := time.Duration(c.talosConsumerConfig.GetRenewCheckInterval()) * time.Millisecond
	ticker := time.NewTicker(duration)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.ReNewTask()
		case <-c.renewTaskChan:
			return
		}
	}
}

func (c *TalosConsumer) initConsumerMonitorTask() {
	defer c.WaitGroup.Done()
	if !c.talosConsumerConfig.TalosClientConfig.ClientMonitorSwitch() {
		return
	}
	// push to falcon every 60 seconds delay by default
	duration := time.Duration(c.talosConsumerConfig.TalosClientConfig.
		ReportMetricInterval()) * time.Second
	ticker := time.NewTicker(duration)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.ConsumerMonitorTask()
		case <-c.monitorTaskChan:
			return
		}
	}
}

/**
 * Check Partition Task
 *
 * if partition number change, invoke ReBalanceTask
 */
func (c *TalosConsumer) CheckPartitionTask() {
	response, err := c.talosAdmin.GetDescribeInfo(&topic.GetDescribeInfoRequest{TopicName: c.topicName})
	if err != nil {
		c.log.Errorf("Exception in CheckPartitionTask: %s", err.Error())
		if utils.IsTopicNotExist(err) {
			c.cancelAllConsumingTask()
			c.topicAbnormalCallback.AbnormalHandler(c.topicTalosResourceName, err)
		}
		return
	}

	if c.topicTalosResourceName.GetTopicTalosResourceName() !=
		response.GetTopicTalosResourceName().GetTopicTalosResourceName() {
		err := fmt.Errorf("The topic: %s not exist. It might have been deleted. "+
			"The getMessage threads will be cancel. ", c.topicTalosResourceName.
			GetTopicTalosResourceName())
		c.log.Errorf(err.Error())
		c.cancelAllConsumingTask()
		c.topicAbnormalCallback.AbnormalHandler(c.topicTalosResourceName, err)
		return
	}

	topicPartitionNum := response.GetPartitionNumber()
	if int32(c.partitionNumber) < topicPartitionNum {
		c.log.Infof("partitionNumber changed from %d to %d, execute re-balance task.",
			c.partitionNumber, topicPartitionNum)
		// update partition number and call the re-balance task
		c.setPartitionNumber(topicPartitionNum)
		// call the re-balance task
		c.WaitGroup.Add(1)
		go c.ReBalanceTask()
	}
}

/**
 * Check Worker Info Task
 *
 * check alive worker number and get the worker serving map
 * 1) get the latest worker info and synchronized update the local workInfoMap
 * 2) invoke the ReBalanceTask every time
 *
 * Note:
 * a) current alive workers refer to scan 'consumerGroup+Topic+Worker'
 * b) all serving partitions got by the a)'s alive workers
 *
 * G+T+W    G+T+P
 * yes       no  -- normal, exist idle workers
 * no        yes -- abnormal, but ttl will fix it
 */
func (c *TalosConsumer) CheckWorkerInfoTask() {
	if err := c.getWorkerInfo(); err != nil {
		c.log.Errorf("Get worker info error: %s", err.Error())
	}
	// invoke the re-balance task every time
	c.WaitGroup.Add(1)
	go c.ReBalanceTask()
}

/**
 * Re-Balance Task
 *
 * This task just re-calculate the 'has'/'max'/'min' and try to steal/release
 * 'CheckPartitionTask' takes charge of updating partitionNumber
 * 'CheckWorkerInfoTask' takes charge of updating workerInfoMap
 */
func (c *TalosConsumer) ReBalanceTask() {
	defer c.WaitGroup.Done()
	c.makeBalance()
}

/**
 * ReNew Task (contains two sections per renew)
 *
 * Note: we make renew process outside rather than inner PartitionFetcher class
 * because:
 * 1) make the partitionFetcher heartbeat and worker heartbeat together
 * 2) renew all the serving partitions lock within one rpc process,
 *    which prevent massive rpc request to server
 *
 * when get what to renew, we take 'partitionFetcherMap' as guideline
 */
func (c *TalosConsumer) ReNewTask() {
	toRenewPartitionList := c.getRenewPartitionList()
	consumeUnit := &consumer.ConsumeUnit{
		ConsumerGroup:          c.consumerGroup,
		TopicTalosResourceName: c.topicTalosResourceName,
		PartitionIdList:        toRenewPartitionList,
		WorkerId:               c.workerId,
	}
	renewRequest := &consumer.RenewRequest{ConsumeUnit: consumeUnit}
	var renewResponse *consumer.RenewResponse
	var err error

	// plus 1 to include the first renew operation
	maxRetry := c.talosConsumerConfig.GetRenewMaxRetry() + 1
	for maxRetry > 0 {
		maxRetry--
		renewResponse, err = c.consumerClient.Renew(renewRequest)
		if err != nil {
			c.log.Errorf("Worker: %s renew error: %s", c.workerId, err.Error())
			continue
		}

		// 1) make heartbeat success and renew partitions success
		if renewResponse.GetHeartbeatSuccess() &&
			len(renewResponse.GetFailedPartitionList()) == 0 {
			c.log.Debugf("Worker: %s success heartbeat and renew partitions: %v",
				c.workerId, toRenewPartitionList)
			return
		}
	} //end for

	// 2) make heart beat failed, cancel all partitions
	// no need to renew anything, so block the renew thread and cancel all task
	if renewResponse != nil && !renewResponse.GetHeartbeatSuccess() {
		c.log.Errorf("Worker: %s failed to make heartbeat, cancel all consumer task",
			c.workerId)
		c.cancelAllConsumingTask()
	}

	// 3) make heartbeat success but renew some partitions failed
	// stop read, commit offset, unlock for renew failed partitions
	// the release process is graceful, so may be a long time,
	// do not block the renew thread and switch thread to re-balance thread
	if renewResponse != nil && len(renewResponse.GetFailedPartitionList()) > 0 {
		failedRenewList := renewResponse.GetFailedPartitionList()
		c.log.Errorf("Worker: %s failed to renew partitions: %v",
			c.workerId, failedRenewList)
		c.releasePartitionLock(failedRenewList)
	}
}

func (c *TalosConsumer) ConsumerMonitorTask() {
	metrics := make([]*utils.FalconMetric, 0)
	for _, p := range c.partitionFetcherMap {
		metrics = append(metrics, p.messageReader.NewFalconMetrics()...)
	}
	c.falconWriter.PushToFalcon(metrics)
}