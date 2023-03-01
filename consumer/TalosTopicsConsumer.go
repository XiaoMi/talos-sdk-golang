/**
 * Copyright 2021, Xiaomi.
 * All rights reserved.
 * Author: fangchengjin@xiaomi.com
 */

package consumer

import (
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/XiaoMi/talos-sdk-golang/admin"
	"github.com/XiaoMi/talos-sdk-golang/client"
	"github.com/XiaoMi/talos-sdk-golang/thrift/auth"
	"github.com/XiaoMi/talos-sdk-golang/thrift/consumer"
	"github.com/XiaoMi/talos-sdk-golang/thrift/message"
	"github.com/XiaoMi/talos-sdk-golang/thrift/topic"
	"github.com/XiaoMi/talos-sdk-golang/utils"
	"github.com/sirupsen/logrus"
)

type TopicPartitions []*topic.TopicAndPartition

func (p TopicPartitions) Len() int {
	return len(p)
}

func (p TopicPartitions) Less(i, j int) bool {
	// partitionId ascending
	if p[i].GetPartitionId() < p[j].GetPartitionId() {
		return true
	}
	if p[i].GetPartitionId() > p[j].GetPartitionId() {
		return false
	}
	// topicName ascending
	return p[i].GetTopicName() < p[j].GetTopicName()
}

func (p TopicPartitions) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

type TalosTopicsConsumer struct {
	workerId                string
	consumerGroup           string
	topicGroup              string
	messageProcessorFactory MessageProcessorFactory
	MessageReaderFactory    *TalosMessageReaderFactory
	partitionFetcherMap     map[*topic.TopicAndPartition]*PartitionFetcher
	talosConsumerConfig     *TalosConsumerConfig
	talosClientFactory      client.TalosClientFactoryInterface
	scheduleInfoCache       *client.ScheduleInfoCache
	talosAdmin              admin.Admin
	consumerClient          consumer.ConsumerService
	topicAbnormalCallback   client.TopicAbnormalCallback
	readWriteLock           sync.RWMutex
	falconWriter            *utils.FalconWriter
	// init by getting from rpc call as follows
	topicPattern         string
	totalPartitionNumber int32
	topicPartitionMap    map[string]int32
	topicList            []*topic.TopicTalosResourceName
	workerInfoMap        map[string][]*topic.TopicAndPartition
	partitionCheckpoint  map[*topic.TopicAndPartition]Long
	checkPartTaskChan    chan utils.StopSign
	checkWorkerTaskChan  chan utils.StopSign
	renewTaskChan        chan utils.StopSign
	monitorTaskChan      chan utils.StopSign
	WaitGroup            *sync.WaitGroup
	log                  *logrus.Logger
}

func NewTalosMultiTopicsConsumerByProperties(properties *utils.Properties,
	messageProcessorFactory MessageProcessorFactory,
	abnormalCallback client.TopicAbnormalCallback) (*TalosTopicsConsumer, error) {

	topicPattern := properties.Get("galaxy.talos.topic.pattern")
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

	return NewDefaultTalosMultiTopicsConsumer(consumerGroupName, consumerConfig, credential,
		topicPattern, NewTalosMessageReaderFactory(), messageProcessorFactory,
		clientIdPrefix, abnormalCallback, make(map[*topic.TopicAndPartition]Long, 0), utils.InitLogger())
}

func NewTalosMultiTopicsConsumerByFilename(propertyFilename string,
	messageProcessorFactory MessageProcessorFactory,
	abnormalCallback client.TopicAbnormalCallback) (*TalosTopicsConsumer, error) {

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

	return NewDefaultTalosMultiTopicsConsumer(consumerGroupName, consumerConfig, credential,
		topicName, NewTalosMessageReaderFactory(), messageProcessorFactory,
		clientIdPrefix, abnormalCallback, make(map[*topic.TopicAndPartition]Long, 0), utils.InitLogger())
}

func NewTalosMultiTopicsConsumerWithLogger(propertyFilename string,
	messageProcessorFactory MessageProcessorFactory,
	abnormalCallback client.TopicAbnormalCallback, logger *logrus.Logger) (*TalosTopicsConsumer, error) {

	props := utils.LoadProperties(propertyFilename)
	topicGroup := props.Get("galaxy.talos.topic.group")
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

	return NewDefaultTalosMultiTopicsConsumer(consumerGroupName, consumerConfig, credential,
		topicGroup, NewTalosMessageReaderFactory(), messageProcessorFactory,
		clientIdPrefix, abnormalCallback, make(map[*topic.TopicAndPartition]Long, 0), logger)
}

func NewTalosMultiTopicsConsumer(consumerGroupName string, consumerConfig *TalosConsumerConfig,
	credential *auth.Credential, topicName string,
	messageProcessorFactory MessageProcessorFactory, clientIdPrefix string,
	abnormalCallback client.TopicAbnormalCallback, logger *logrus.Logger) (*TalosTopicsConsumer, error) {
	return NewDefaultTalosMultiTopicsConsumer(consumerGroupName, consumerConfig, credential,
		topicName, NewTalosMessageReaderFactory(), messageProcessorFactory,
		clientIdPrefix, abnormalCallback, make(map[*topic.TopicAndPartition]Long, 0), logger)
}

func NewDefaultTalosMultiTopicsConsumer(consumerGroupName string, consumerConfig *TalosConsumerConfig,
	credential *auth.Credential, topicGroupName string,
	messageReaderFactory *TalosMessageReaderFactory,
	messageProcessorFactory MessageProcessorFactory, clientIdPrefix string,
	abnormalCallback client.TopicAbnormalCallback,
	partitionCheckpoint map[*topic.TopicAndPartition]Long, logger *logrus.Logger) (*TalosTopicsConsumer, error) {

	workerId, err := utils.CheckAndGenerateClientId(clientIdPrefix)
	if err != nil {
		return nil, err
	}

	err = utils.CheckNameValidity(consumerGroupName)
	if err != nil {
		return nil, err
	}

	talosClientFactory := client.NewTalosClientFactory(consumerConfig.TalosClientConfig, credential)
	consumerClient := talosClientFactory.NewConsumerClientDefault()

	talosAdmin := admin.NewTalosAdmin(talosClientFactory)

	falconWriter := utils.NewFalconWriter(consumerConfig.TalosClientConfig.FalconUrl(), logger)

	c := &TalosTopicsConsumer{
		workerId:                workerId,
		consumerGroup:           consumerGroupName,
		messageProcessorFactory: messageProcessorFactory,
		MessageReaderFactory:    messageReaderFactory,
		partitionFetcherMap:     make(map[*topic.TopicAndPartition]*PartitionFetcher),
		talosConsumerConfig:     consumerConfig,
		talosClientFactory:      talosClientFactory,
		talosAdmin:              talosAdmin,
		topicGroup:              topicGroupName,
		topicPattern:            "",
		topicPartitionMap:       make(map[string]int32),
		topicList:               make([]*topic.TopicTalosResourceName, 0),
		consumerClient:          consumerClient,
		topicAbnormalCallback:   abnormalCallback,
		partitionCheckpoint:     partitionCheckpoint,
		scheduleInfoCache:       new(client.ScheduleInfoCache),
		falconWriter:            falconWriter,
		checkPartTaskChan:       make(chan utils.StopSign),
		checkWorkerTaskChan:     make(chan utils.StopSign),
		renewTaskChan:           make(chan utils.StopSign),
		monitorTaskChan:         make(chan utils.StopSign),
		WaitGroup:               new(sync.WaitGroup),
		log:                     logger,
	}

	response, err := talosAdmin.DescribeTopicGroup(&topic.DescribeTopicGroupRequest{TopicGroupName: topicGroupName})
	if err != nil {
		return nil, err
	}

	if response.IsSetTopicPattern() {
		c.topicPattern = response.GetTopicPattern()
		err = c.getTopicAndScheduleInfo()
		if err != nil {
			return nil, err
		}

		c.WaitGroup.Add(1)
		go c.initCheckTopicsTask()
	} else if response.IsSetTopicSet() {
		err = c.getTopicAndScheduleInfoByList(response.TopicSet)
		if err != nil {
			return nil, err
		}
	}

	c.log.Infof("The worker: %s is initializing...", workerId)

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
	c.WaitGroup.Add(3)

	go c.initCheckWorkerInfoTask()
	go c.initRenewTask()
	go c.initConsumerMonitorTask()

	return c, nil
}

func (c *TalosTopicsConsumer) getTopicAndScheduleInfo() error {
	response, err := c.talosAdmin.LookupTopics(&message.LookupTopicsRequest{TopicPattern: c.topicPattern})
	if err != nil {
		return err
	}

	if len(response.Topics) == 0 {
		return fmt.Errorf("no authorized topics match topic pattern: %s", c.topicPattern)
	}

	c.setTopics(response.Topics)
	c.log.Infof("The worker: %s check and get topic info done", c.workerId)
	return nil
}

func (c *TalosTopicsConsumer) getTopicAndScheduleInfoByList(topicList []string) error {
	for _, topicName := range topicList {
		err := utils.CheckNameValidity(topicName)
		if err != nil {
			return err
		}

		response, err := c.talosAdmin.GetDescribeInfo(&topic.GetDescribeInfoRequest{TopicName: topicName})
		if err != nil {
			return err
		}

		c.topicPartitionMap[response.TopicTalosResourceName.TopicTalosResourceName] = response.PartitionNumber
	}

	c.setTopics(c.topicPartitionMap)
	c.log.Infof("The worker: %s check and get topic info done", c.workerId)
	return nil
}

func (c *TalosTopicsConsumer) registerSelf() error {
	lockWorkerRequest := &consumer.MultiTopicsLockWorkerRequest{
		ConsumerGroup: c.consumerGroup,
		Topics:        c.topicList,
		WorkerId:      c.workerId,
	}
	lockWorkerResponse := consumer.NewMultiTopicsLockWorkerResponse()

	var err error
	tryCount := c.talosConsumerConfig.GetSelfRegisterMaxRetry() + 1
	for tryCount > 0 {
		tryCount--
		lockWorkerResponse, err = c.consumerClient.LockWorkerForMultiTopics(lockWorkerRequest)
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

func (c *TalosTopicsConsumer) getWorkerInfo() error {
	queryWorkerRequest := &consumer.MultiTopicsQueryWorkerRequest{
		ConsumerGroup:           c.consumerGroup,
		TopicTalosResourceNames: c.topicList,
	}
	queryWorkerResponse, err := c.consumerClient.QueryWorkerForMultiTopics(queryWorkerRequest)
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

func (c *TalosTopicsConsumer) calculateTargetList(copyPartitionNum, workerNumber int,
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

func (c *TalosTopicsConsumer) calculateWorkerPairs(copyWorkerMap map[string][]*topic.TopicAndPartition,
	sortedWorkerPairs *WorkerPairs) {
	for workerId, partitionIdList := range copyWorkerMap {
		*sortedWorkerPairs = append(*sortedWorkerPairs,
			NewWorkerPair(workerId, len(partitionIdList)))
	}
	sort.Sort(sortedWorkerPairs) // descending
	c.log.Infof("worker: %s calculate sorted worker pairs: %v",
		c.workerId, sortedWorkerPairs)
}

func (c *TalosTopicsConsumer) makeBalance() {
	/**
	 * When start make balance, we deep copy 'totalPartitionNumber' and 'workerInfoMap'
	 * to prevent both value appear inconsistent during the process makeBalance
	 */
	copyPartitionNum := c.totalPartitionNumber
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
	c.calculateTargetList(int(copyPartitionNum), len(copyWorkerInfoMap), &targetList)
	c.calculateWorkerPairs(copyWorkerInfoMap, &sortedWorkerPairs)
	//judge stealing or release partition
	toStealList := make([]*topic.TopicAndPartition, 0)
	toReleaseList := make([]*topic.TopicAndPartition, 0)

	for i := 0; i < sortedWorkerPairs.Len(); i++ {
		if sortedWorkerPairs[i].workerId == c.workerId {
			hasList := c.getHasList()
			has := len(hasList)

			// when a topic decreases partition, release all reduced partitions
			for _, tp := range hasList {
				topicName := tp.GetTopicTalosResourceName().GetTopicTalosResourceName()
				if partitionNum, ok := c.topicPartitionMap[topicName]; !ok ||
					tp.GetPartitionId() >= partitionNum {
					toReleaseList = append(toReleaseList, tp)
				}
			}
			// wait next cycle to balance
			if len(toReleaseList) > 0 {
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
				sort.Sort(hasList)
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

func (c *TalosTopicsConsumer) stealPartitionLock(toStealList TopicPartitions) {
	c.log.Infof("Worker: %s try to steal %d partition: %v", c.workerId, len(toStealList), toStealList)
	// try to lock and invoke serving partition PartitionFetcher to 'LOCKED' state
	url := c.talosConsumerConfig.ServiceEndpoint() + utils.TALOS_MESSAGE_SERVICE_PATH
	c.readWriteLock.Lock()
	for _, partition := range toStealList {
		if _, ok := c.partitionFetcherMap[partition]; !ok {
			// Note 'partitionCheckPoint.get(partitionId)' may be null, it's ok
			messageReader := c.MessageReaderFactory.CreateMessageReader(c.talosConsumerConfig, c.log)
			partitionFetcher := NewPartitionFetcher(c.consumerGroup, partition.TopicName,
				partition.TopicTalosResourceName, partition.PartitionId, c.talosConsumerConfig,
				c.workerId, c.consumerClient, c.scheduleInfoCache,
				c.talosClientFactory.NewMessageClient(url),
				c.messageProcessorFactory.CreateProcessor(),
				messageReader, c.partitionCheckpoint[partition], c.log)
			c.partitionFetcherMap[partition] = partitionFetcher
		}
		c.partitionFetcherMap[partition].Lock()
	}
	c.readWriteLock.Unlock()
}

func (c *TalosTopicsConsumer) releasePartitionLock(toReleaseList TopicPartitions) {
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

func (c *TalosTopicsConsumer) getIdlePartitions() TopicPartitions {
	if c.totalPartitionNumber < 0 {
		c.log.Errorf("consumer has error partition num: %d", c.totalPartitionNumber)
		return nil
	}
	idlePartitions := make(TopicPartitions, 0)

	c.readWriteLock.Lock()
	defer c.readWriteLock.Unlock()

	for t, partitionNum := range c.topicPartitionMap {
		for i := 0; i < int(partitionNum); i++ {
			topicName, _ := utils.GetTopicNameByResourceName(t)
			idelPartition := &topic.TopicAndPartition{
				TopicName:              topicName,
				TopicTalosResourceName: &topic.TopicTalosResourceName{TopicTalosResourceName: t},
				PartitionId:            int32(i),
			}
			idlePartitions = append(idlePartitions, idelPartition)
		}
	}

	for _, partitionIdList := range c.workerInfoMap {
		for _, servePartitionId := range partitionIdList {
			for j, idlePartitionId := range idlePartitions {
				if reflect.DeepEqual(servePartitionId, idlePartitionId) {
					idlePartitions = append(idlePartitions[:j], idlePartitions[j+1:]...)
					break
				}
			}
		}
	}
	sort.Sort(idlePartitions)
	return idlePartitions
}

func (c *TalosTopicsConsumer) getHasList() TopicPartitions {
	hasList := make(TopicPartitions, 0)
	c.readWriteLock.Lock()
	defer c.readWriteLock.Unlock()
	for partitionId, partitionFetcher := range c.partitionFetcherMap {
		if partitionFetcher.IsServing() {
			hasList = append(hasList, partitionId)
		}
	}
	return hasList
}

func (c *TalosTopicsConsumer) cancelAllConsumingTask() {
	c.releasePartitionLock(c.getHasList())
}

func (c *TalosTopicsConsumer) shutDownAllFetcher() {
	for _, partitionFetcher := range c.partitionFetcherMap {
		partitionFetcher.Shutdown()
	}
}

func (c *TalosTopicsConsumer) ShutDown() {
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

func (c *TalosTopicsConsumer) deepCopyWorkerInfoMap() map[string][]*topic.TopicAndPartition {
	copyMap := make(map[string][]*topic.TopicAndPartition, len(c.workerInfoMap))
	c.readWriteLock.Lock()
	defer c.readWriteLock.Unlock()
	for workerId, partitionIdList := range c.workerInfoMap {
		copyMap[workerId] = partitionIdList
	}
	return copyMap
}

func (c *TalosTopicsConsumer) setTopics(topics map[string]int32) {
	c.readWriteLock.Lock()
	defer c.readWriteLock.Unlock()

	c.topicPartitionMap = topics
	c.totalPartitionNumber = 0
	c.topicList = make([]*topic.TopicTalosResourceName, 0, len(topics))
	for t, p := range topics {
		c.topicList = append(c.topicList, &topic.TopicTalosResourceName{TopicTalosResourceName: t})
		c.totalPartitionNumber += p
	}

	for _, topic := range c.topicList {
		c.scheduleInfoCache = client.GetScheduleInfoCache(topic, c.talosConsumerConfig.TalosClientConfig,
			c.talosClientFactory.NewMessageClientDefault(), c.talosClientFactory, c.log)
	}
}

func (c *TalosTopicsConsumer) getRenewPartitionList() map[string][]int32 {
	toRenewList := make(map[string][]int32, 0)
	c.readWriteLock.RLock()
	for _, topic := range c.topicList {
		toRenewList[topic.TopicTalosResourceName] = make([]int32, 0)
	}
	for partition, partitionFetcher := range c.partitionFetcherMap {
		if partitionFetcher.IsHoldingLock() {
			topicResourceName := partition.TopicTalosResourceName.TopicTalosResourceName
			if _, ok := toRenewList[topicResourceName]; !ok {
				toRenewList[topicResourceName] = make([]int32, 0)
			}
			toRenewList[topicResourceName] = append(
				toRenewList[topicResourceName], partition.PartitionId)
		}
	}
	c.readWriteLock.RUnlock()
	return toRenewList
}

func (c *TalosTopicsConsumer) initCheckTopicsTask() {
	defer c.WaitGroup.Done()
	// check check all authorized topics that match given pattern every 10 minutes delay by default
	duration := time.Duration(c.talosConsumerConfig.
		GetTopicPatternCheckInterval()) * time.Millisecond
	ticker := time.NewTicker(duration)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.CheckTopicPatternTask()
		case <-c.checkPartTaskChan:
			return
		}
	}
}

func (c *TalosTopicsConsumer) initCheckWorkerInfoTask() {
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

func (c *TalosTopicsConsumer) initRenewTask() {
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

func (c *TalosTopicsConsumer) initConsumerMonitorTask() {
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
 * Check Topics Task
 *
 * if topics number or partition number of any single topic change, invoke ReBalanceTask
 */
func (c *TalosTopicsConsumer) CheckTopicPatternTask() {
	topicAndPartition := new(topic.TopicAndPartition)
	if len(c.topicList) == 0 {
		topicAndPartition = nil
	} else {
		topicName, _ := utils.GetTopicNameByResourceName(c.topicList[0].TopicTalosResourceName)
		topicAndPartition = &topic.TopicAndPartition{
			TopicName:              topicName,
			TopicTalosResourceName: c.topicList[0],
			PartitionId:            0,
		}
	}
	response, err := c.scheduleInfoCache.GetOrCreateMessageClient(topicAndPartition).LookupTopics(
		&message.LookupTopicsRequest{TopicPattern: c.topicPattern})
	if err != nil {
		c.log.Errorf("Exception in CheckTopicsTask: %s", err.Error())
		return
	}

	newTopicPartitionMap := response.GetTopics()
	if len(newTopicPartitionMap) == 0 {
		c.log.Errorf("CheckTopics error: No authorized topics match topic pattern: %s, "+
			"cancel all consume tasks.", c.topicPattern)
		c.setTopics(newTopicPartitionMap)
		c.cancelAllConsumingTask()
		return
	}

	if c.isTopicsOrPartitionsChanged(newTopicPartitionMap) {
		c.log.Infof("matched topic or partition number changed, execute a re-balance task.")
		// update topic list and partition number
		c.setTopics(newTopicPartitionMap)
		// call the re-balance task
		c.WaitGroup.Add(1)
		go c.CheckWorkerInfoTask()
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
func (c *TalosTopicsConsumer) CheckWorkerInfoTask() {
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
 * 'CheckPartitionTask' takes charge of updating totalPartitionNumber
 * 'CheckWorkerInfoTask' takes charge of updating workerInfoMap
 */
func (c *TalosTopicsConsumer) ReBalanceTask() {
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
func (c *TalosTopicsConsumer) ReNewTask() {
	toRenewPartitionList := c.getRenewPartitionList()
	consumeUnit := &consumer.MultiTopicsConsumeUnit{
		ConsumerGroup:   c.consumerGroup,
		TopicPartitions: toRenewPartitionList,
		WorkerId:        c.workerId,
	}
	renewRequest := &consumer.MultiTopicsRenewRequest{ConsumeUnit: consumeUnit}
	var renewResponse *consumer.MultiTopicsRenewResponse
	var err error

	// plus 1 to include the first renew operation
	maxRetry := c.talosConsumerConfig.GetRenewMaxRetry() + 1
	for maxRetry > 0 {
		maxRetry--
		renewResponse, err = c.consumerClient.RenewForMultiTopics(renewRequest)
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

func (c *TalosTopicsConsumer) ConsumerMonitorTask() {
	metrics := make([]*utils.FalconMetric, 0)
	for _, p := range c.partitionFetcherMap {
		metrics = append(metrics, p.messageReader.NewFalconMetrics()...)
	}
	c.falconWriter.PushToFalcon(metrics)
}

func (c *TalosTopicsConsumer) isTopicsOrPartitionsChanged(newTopicPartitionMap map[string]int32) bool {
	for topic, partitionNum := range c.topicPartitionMap {
		if newPartitionNum, ok := newTopicPartitionMap[topic]; ok {
			// partition number is not same
			if partitionNum != newPartitionNum {
				return true
			}
		} else {
			// some topics are deleted
			return true
		}
	}
	// check if add topics
	return len(c.topicPartitionMap) < len(newTopicPartitionMap)
}
