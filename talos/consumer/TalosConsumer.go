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

	"github.com/XiaoMi/talos-sdk-golang/talos/client"
	"github.com/XiaoMi/talos-sdk-golang/talos/thrift/auth"
	"github.com/XiaoMi/talos-sdk-golang/talos/thrift/common"
	"github.com/XiaoMi/talos-sdk-golang/talos/thrift/consumer"
	"github.com/XiaoMi/talos-sdk-golang/talos/thrift/topic"
	"github.com/XiaoMi/talos-sdk-golang/talos/utils"
	log "github.com/alecthomas/log4go"
)

type StopSignType int

const (
	shutdown StopSignType = iota // 0
	running
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
	return p[i].hasPartitionNum > p[j].hasPartitionNum
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
	talosClientFactory      *client.TalosClientFactory
	scheduleInfoCache       *client.ScheduleInfoCache
	consumerClient          consumer.ConsumerService
	topicAbnormalCallback   client.TopicAbnormalCallback
	readWriteLock           sync.RWMutex
	// init by getting from rpc call as follows
	topicName              string
	partitionNumber        int
	topicTalosResourceName *topic.TopicTalosResourceName
	workerInfoMap          map[string][]int32
	partitionCheckpoint    map[int32]Long
	StopSign               chan StopSignType
	checkPartTaskSign      chan StopSignType
	checkWorkerTaskSign    chan StopSignType
	renewTaskSign          chan StopSignType
	waitGroup              *sync.WaitGroup
}

func NewTalosConsumer(consumerGroupName string, consumerConfig *TalosConsumerConfig,
	credential *auth.Credential, topicTalosResourceName *topic.TopicTalosResourceName,
	messageReaderFactory *TalosMessageReaderFactory,
	messageProcessorFactory MessageProcessorFactory, clientIdPrefix string,
	abnormalCallback client.TopicAbnormalCallback,
	partitionCheckpoint map[int32]Long) *TalosConsumer {

	workerId, err := utils.CheckAndGenerateClientId(clientIdPrefix)
	if err != nil {
		log.Error(err.Error())
		return nil
	}
	err = utils.CheckNameValidity(consumerGroupName)
	if err != nil {
		log.Error(err.Error())
		return nil
	}
	socketTimeout := int64(common.GALAXY_TALOS_CLIENT_ADMIN_TIMEOUT_MILLI_SECS_DEFAULT)
	talosClientFactory := client.NewTalosClientFactory(consumerConfig.TalosClientConfig,
		credential, time.Duration(socketTimeout*int64(time.Millisecond)))

	consumerClient := talosClientFactory.NewConsumerClient(
		consumerConfig.TalosClientConfig.ServiceEndpoint() + common.TALOS_CONSUMER_SERVICE_PATH)
	talosConsumer := &TalosConsumer{
		workerId:                workerId,
		consumerGroup:           consumerGroupName,
		messageProcessorFactory: messageProcessorFactory,
		MessageReaderFactory:    messageReaderFactory,
		partitionFetcherMap:     make(map[int32]*PartitionFetcher),
		talosConsumerConfig:     consumerConfig,
		talosClientFactory:      &talosClientFactory,
		consumerClient:          consumerClient,
		topicAbnormalCallback:   abnormalCallback,
		partitionCheckpoint:     partitionCheckpoint,
		StopSign:                make(chan StopSignType, 3),
		checkPartTaskSign:       make(chan StopSignType),
		checkWorkerTaskSign:     make(chan StopSignType),
		renewTaskSign:           make(chan StopSignType),
		waitGroup:               new(sync.WaitGroup),
	}
	log.Info("The worker: %s is initializing...", workerId)

	// check and get topic info such as partitionNumber
	talosConsumer.checkAndGetTopicInfo(topicTalosResourceName)
	// register self workerId
	talosConsumer.registerSelf()
	// get worker info
	talosConsumer.getWorkerInfo()
	// do balance and init simple consumer
	talosConsumer.makeBalance()

	// start CheckPartitionTask/CheckWorkerInfoTask/RenewTask
	talosConsumer.waitGroup.Add(3)
	go talosConsumer.initCheckPartitionTask()
	go talosConsumer.initCheckWorkerInfoTask()
	go talosConsumer.initRenewTask()

	return talosConsumer
}

func (c *TalosConsumer) checkAndGetTopicInfo(
	topicTalosResourceName *topic.TopicTalosResourceName) error {

	var err error
	c.topicName, err = utils.GetTopicNameByResourceName(
		topicTalosResourceName.GetTopicTalosResourceName())

	topic, err := c.DescribeTopic()
	if err != nil {
		log.Error("describeTopic error: %s", err.Error())
		return err
	}
	if topicTalosResourceName.GetTopicTalosResourceName() !=
		topic.GetTopicInfo().GetTopicTalosResourceName().GetTopicTalosResourceName() {
		err := fmt.Errorf("The topic: %s not found ",
			topicTalosResourceName.GetTopicTalosResourceName())
		return err
	}
	c.setPartitionNumber(topic.GetTopicAttribute().GetPartitionNumber())
	c.topicTalosResourceName = topicTalosResourceName
	log.Info("The worker: %s check and get topic info done", c.workerId)
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
			log.Error("The worker: %s register self got error: %s",
				c.workerId, err.Error())
			continue
		}
		if lockWorkerResponse.GetRegisterSuccess() {
			log.Info("The worker: %s register self success", c.workerId)
			return nil
		}
		log.Warn("The worker: %s register self failed, make %d retry",
			c.workerId, tryCount+1)
	}
	err = fmt.Errorf("The worker: %s register self failed. ", c.workerId)
	return err
}

func (c *TalosConsumer) getWorkerInfo() error {
	queryWorkerRequest := &consumer.QueryWorkerRequest{
		ConsumerGroup:          c.consumerGroup,
		TopicTalosResourceName: c.topicTalosResourceName,
	}
	queryWorkerResponse, err := c.consumerClient.QueryWorker(queryWorkerRequest)
	if err != nil {
		log.Error(err)
		return err
	}

	// if queryWorkerInfoMap size equals 0,
	// it represents hbase failed error, do not update local map
	// because registration, the queryWorkerInfoMap size >= 1 at least
	// if queryWorkerInfoMap not contains self, it indicates renew failed,
	// do not update local map to prevent a bad re-balance
	if _, ok := queryWorkerResponse.GetWorkerMap()[c.workerId]; len(
		queryWorkerResponse.GetWorkerMap()) == 0 || !ok {
		err = fmt.Errorf("hbase failed error, don't update local map")
		return err
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
			err := fmt.Errorf("illegal partition number: %d", sum)
			return err
		}
	}

	// sort target by descending
	sort.Sort(sort.Reverse(sort.IntSlice(*targetList)))
	log.Info("Worker: %s calculate target partitions done: %v",
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
	log.Info("worker: %s calculate sorted worker pairs: %v",
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
		log.Error("WorkerInfoMap not contains worker: %s. There may be some error"+
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

			// workerNum > partitionNum, idle workers have no match target, do nothing
			if i >= len(targetList) {
				break
			}
			target := targetList[i]
			log.Debug("Worker: %s has: %d partition, target: %d", c.workerId, has, target)

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
					rand.Seed(int64(time.Now().UnixNano()))
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
		log.Error("make balance error: both toStealList and toReleaseList exist")
		return
	}
	if len(toStealList) > 0 {
		c.stealPartitionLock(toStealList)
	} else if len(toReleaseList) > 0 {
		c.releasePartitionLock(toReleaseList)
	} else {
		// do nothing when reach balance state
		log.Info("The worker: %s have reached a balanced state.", c.workerId)
	}
}

func (c *TalosConsumer) stealPartitionLock(toStealList []int32) {
	log.Info("Worker: %s try to steal %d partition: %v",
		c.workerId, len(toStealList), toStealList)
	// try to lock and invoke serving partition PartitionFetcher to 'LOCKED' state
	url := c.talosConsumerConfig.ServiceEndpoint() + common.TALOS_MESSAGE_SERVICE_PATH
	c.readWriteLock.Lock()
	for _, partitionId := range toStealList {
		if _, ok := c.partitionFetcherMap[partitionId]; !ok {
			// Note 'partitionCheckPoint.get(partitionId)' may be null, it's ok
			messageReader := c.MessageReaderFactory.CreateMessageReader(c.talosConsumerConfig)
			partitionFetcher := NewPartitionFetcher(c.consumerGroup, c.topicName,
				c.topicTalosResourceName, partitionId, c.talosConsumerConfig,
				c.workerId, c.consumerClient, c.talosClientFactory.NewMessageClient(url),
				c.messageProcessorFactory.CreateProcessor(),
				messageReader, c.partitionCheckpoint[partitionId])
			c.partitionFetcherMap[partitionId] = partitionFetcher
		}
		c.partitionFetcherMap[partitionId].Lock()
	}
	c.readWriteLock.Unlock()
}

func (c *TalosConsumer) releasePartitionLock(toReleaseList []int32) {
	log.Info("Worker: %s try to release %d parition: %v",
		c.workerId, len(toReleaseList), toReleaseList)
	// stop read, commit offset, unlock the partition async
	for _, partitionId := range toReleaseList {
		if _, ok := c.partitionFetcherMap[partitionId]; !ok {
			log.Error("partitionFetcher map not contains partition: %d", partitionId)
			return
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
		log.Error("consumer has error partition num: %d", c.partitionNumber)
		return nil
	}
	idlePartitions := make([]int32, 0)
	c.readWriteLock.Lock()
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
	c.readWriteLock.Unlock()
	return idlePartitions
}

func (c *TalosConsumer) getHasList() []int32 {
	hasList := make([]int32, 0)
	c.readWriteLock.Lock()
	for partitionId, partitionFetcher := range c.partitionFetcherMap {
		if partitionFetcher.IsServing() {
			hasList = append(hasList, partitionId)
		}
	}
	c.readWriteLock.Unlock()
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
	log.Info("Worker: %s is shutting down...", c.workerId)
	c.shutDownAllFetcher()
	c.checkWorkerTaskSign <- shutdown
	c.checkPartTaskSign <- shutdown
	c.renewTaskSign <- shutdown
	close(c.checkWorkerTaskSign)
	close(c.checkPartTaskSign)
	close(c.renewTaskSign)
	log.Info("Worker: %s is shutdown.", c.workerId)
}

func (c *TalosConsumer) deepCopyWorkerInfoMap() map[string][]int32 {
	copyMap := make(map[string][]int32, len(c.workerInfoMap))
	c.readWriteLock.Lock()
	for workerId, partitionIdList := range c.workerInfoMap {
		copyMap[workerId] = partitionIdList
	}
	c.readWriteLock.Unlock()
	return copyMap
}

func (c *TalosConsumer) DescribeTopic() (*topic.Topic, error) {
	describeTopicRequest := &topic.DescribeTopicRequest{TopicName: c.topicName}
	topicClient := c.talosClientFactory.NewTopicClient(
		c.talosConsumerConfig.ServiceEndpoint() + common.TALOS_TOPIC_SERVICE_PATH)
	describeTopicResponse, err := topicClient.DescribeTopic(describeTopicRequest)
	if err != nil {
		log.Error("describeTopic error: %s", err.Error())
		return nil, err
	}
	topic := &topic.Topic{
		TopicInfo:      describeTopicResponse.GetTopicInfo(),
		TopicAttribute: describeTopicResponse.GetTopicAttribute(),
		TopicState:     describeTopicResponse.GetTopicState(),
		TopicQuota:     describeTopicResponse.GetTopicQuota(),
		TopicAcl:       describeTopicResponse.GetAclMap(),
	}
	return topic, nil
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
	defer c.waitGroup.Done()
	// check and update partition number every 1 minutes delay by default
	duration := time.Duration(c.talosConsumerConfig.
		GetPartitionCheckInterval()) * time.Millisecond
	ticker := time.NewTicker(duration)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.CheckPartitionTask()
		case <-c.checkPartTaskSign:
			c.StopSign <- shutdown
			return
		}
	}
}

func (c *TalosConsumer) initCheckWorkerInfoTask() {
	defer c.waitGroup.Done()
	// check worker info every 10 seconds delay by default
	duration := time.Duration(c.talosConsumerConfig.
		GetWorkerInfoCheckInterval()) * time.Millisecond
	ticker := time.NewTicker(duration)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.CheckWorkerInfoTask()
		case <-c.checkWorkerTaskSign:
			c.StopSign <- shutdown
			return
		}
	}
}

func (c *TalosConsumer) initRenewTask() {
	defer c.waitGroup.Done()
	// renewTask every 7 seconds delay by default
	duration := time.Duration(c.talosConsumerConfig.GetRenewCheckInterval()) * time.Millisecond
	ticker := time.NewTicker(duration)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.ReNewTask()
		case <-c.renewTaskSign:
			c.StopSign <- shutdown
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
	topic, err := c.DescribeTopic()
	if err != nil {
		log.Error("Exception in CheckPartitionTask: %s", err.Error())
		// if error is HBaseOperationFailed, just return
		// if error is TopicNotExist, cancel all reading task
		// TODO: add errorCode check , choose operation for different error
		//if common.ErrorCode_TOPIC_NOT_EXIST == err.ErrCode {
		//  c.cancelAllConsumingTask()
		//  c.topicAbnormalCallback.AbnormalHandler(c.topicTalosResourceName, err)
		//}
		return
	}
	if c.topicTalosResourceName.GetTopicTalosResourceName() !=
		topic.GetTopicInfo().GetTopicTalosResourceName().GetTopicTalosResourceName() {
		err := fmt.Errorf("The topic: %s not exist. It might have been deleted. "+
			"The getMessage threads will be cancel. ", c.topicTalosResourceName.
			GetTopicTalosResourceName())
		log.Error(err)
		c.cancelAllConsumingTask()
		c.topicAbnormalCallback.AbnormalHandler(c.topicTalosResourceName, err)
		return
	}
	topicPartitionNum := topic.GetTopicAttribute().GetPartitionNumber()
	if int32(c.partitionNumber) < topicPartitionNum {
		log.Info("partitionNumber changed from %d to %d, execute re-balance task.",
			c.partitionNumber, topicPartitionNum)
		// update partition number and call the re-balance task
		c.setPartitionNumber(topicPartitionNum)
		// call the re-balance task
		c.waitGroup.Add(1)
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
		log.Error("Get worker info error: %s", err.Error())
	}
	// invoke the re-balance task every time
	c.waitGroup.Add(1)
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
	defer c.waitGroup.Done()
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
	var renewResponse consumer.RenewResponse

	// plus 1 to include the first renew operation
	maxRetry := c.talosConsumerConfig.GetRenewMaxRetry() + 1
	for maxRetry > 0 {
		maxRetry--
		renewResponse, err := c.consumerClient.Renew(renewRequest)
		if err != nil {
			log.Error("Worker: %s renew error: %s", c.workerId, err.Error())
			continue
		}

		// 1) make heartbeat success and renew partitions success
		if renewResponse.GetHeartbeatSuccess() &&
			len(renewResponse.GetFailedPartitionList()) == 0 {
			log.Debug("Worker: %s success heartbeat and renew partitions: %v",
				c.workerId, toRenewPartitionList)
			return
		}
	} //end for

	// 2) make heart beat failed, cancel all partitions
	// no need to renew anything, so block the renew thread and cancel all task
	if !renewResponse.GetHeartbeatSuccess() {
		log.Error("Worker: %s failed to make heartbeat, cancel all consumer task",
			c.workerId)
		c.cancelAllConsumingTask()
	}

	// 3) make heartbeat success but renew some partitions failed
	// stop read, commit offset, unlock for renew failed partitions
	// the release process is graceful, so may be a long time,
	// do not block the renew thread and switch thread to re-balance thread
	if len(renewResponse.GetFailedPartitionList()) > 0 {
		failedRenewList := renewResponse.GetFailedPartitionList()
		log.Error("Worker: %s failed to renew partitions: %v",
			c.workerId, failedRenewList)
		c.releasePartitionLock(failedRenewList)
	}
}
