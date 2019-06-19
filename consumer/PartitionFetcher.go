/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package consumer

import (
	"sync"
	"sync/atomic"

	"talos-sdk-golang/client"
	"talos-sdk-golang/thrift/consumer"
	"talos-sdk-golang/thrift/message"
	"talos-sdk-golang/thrift/topic"
	"talos-sdk-golang/utils"

	log "github.com/sirupsen/logrus"
)

/**
 * State of PartitionFetcher
 *
 * The state evolution as follows:
 * INIT -> LOCKED;
 * LOCKED -> UNLOCKING;
 * LOCKED -> UNLOCKED;
 * UNLOCKING -> UNLOCKED;
 * UNLOCKED -> LOCKED;
 */
type TaskState int32

const (
	INIT TaskState = iota
	LOCKED
	UNLOCKING
	UNLOCKED
	SHUTDOWNED
)

func (state TaskState) String() string {
	switch state {
	case INIT:
		return "Init"
	case LOCKED:
		return "Locked"
	case UNLOCKING:
		return "Unlocking"
	case UNLOCKED:
		return "Unlocked"
	case SHUTDOWNED:
		return "Shutdown"
	default:
		return "UnKnowState"
	}
}

/**
 * PartitionFetcher
 *
 * Per partition per PartitionFetcher
 *
 * PartitionFetcher as the message process task for one partition, which has four state:
 * INIT, LOCKED, UNLOCKING, UNLOCKED
 * Every PartitionFetcher has one runnable FetcherStateMachine to fetch messages continuously.
 *
 * when standing be LOCKED, it continuously reading messages by SimpleConsumer.fetchMessage;
 * when standing be UNLOCKING, it stop to read, commit offset and release the partition lock;
 * when standing be UNLOCKED, it do not serve any partition and wait to be invoking;
 */
type PartitionFetcher struct {
	consumerGroup          string
	topicTalosResourceName *topic.TopicTalosResourceName
	partitionId            int32
	workerId               string
	consumerClient         consumer.ConsumerService
	curState               TaskState
	topicAndPartition      *topic.TopicAndPartition
	simpleConsumer         *SimpleConsumer
	messageReader          *TalosMessageReader
	wg                     *sync.WaitGroup
	fetcherLock            sync.Mutex
}

func NewPartitionFetcher(consumerGroup string, topicName string,
	topicTalosResourceName *topic.TopicTalosResourceName, partitionId int32,
	talosConsumerConfig *TalosConsumerConfig, workerId string,
	consumerClient consumer.ConsumerService, cache *client.ScheduleInfoCache,
	messageClient message.MessageService, messageProcessor MessageProcessor,
	messageReader *TalosMessageReader, outerCheckpoint Long) *PartitionFetcher {

	topicAndpartition := &topic.TopicAndPartition{
		TopicName:              topicName,
		TopicTalosResourceName: topicTalosResourceName,
		PartitionId:            partitionId,
	}

	simpleConsumer, err := NewSimpleConsumerForHighLvl(talosConsumerConfig, topicAndpartition,
		messageClient, cache)
	if err != nil {
		return nil
	}

	messageReader.SetWorkerId(workerId).
		SetConsumerGroup(consumerGroup).
		SetTopicAndPartition(topicAndpartition).
		SetSimpleConsumer(simpleConsumer).
		SetMessageProcessor(messageProcessor).
		SetConsumerClient(consumerClient).
		SetOuterCheckpoint(outerCheckpoint)

	log.Infof("The PartitionFetcher for topic: %s partition: %d init.",
		topicTalosResourceName.GetTopicTalosResourceName(), partitionId)

	return &PartitionFetcher{
		consumerGroup:          consumerGroup,
		topicTalosResourceName: topicTalosResourceName,
		partitionId:            partitionId,
		workerId:               workerId,
		consumerClient:         consumerClient,
		curState:               INIT,
		topicAndPartition:      topicAndpartition,
		simpleConsumer:         simpleConsumer,
		messageReader:          messageReader,
		wg:                     new(sync.WaitGroup),
	}
}

func (f *PartitionFetcher) fetcherStateMachine() {
	defer f.wg.Done()
	// try to lock partition from HBase, if failed, set to UNLOCKED and return;
	if !f.stealPartition() {
		f.updateState(UNLOCKED)
		return
	}

	// query start offset to read, if failed, clean and return;
	err := f.messageReader.InitStartOffset()
	if err != nil {
		log.Errorf("Worker: %s query partition offset error: %s "+
			"we will skip this partition", f.workerId, err.Error())
		f.clean()
		return
	}

	// reading data
	log.Infof("The workerId: %s is serving partition: %d from offset: %d",
		f.workerId, f.partitionId, atomic.LoadInt64(f.messageReader.StartOffset()))
	for f.GetCurState() == LOCKED {
		f.messageReader.FetchData()
	}

	// wait task quit gracefully: stop reading, commit offset, clean and shutdown
	f.messageReader.CleanReader()
	f.clean()
	log.Infof("The MessageProcessTask for topic: %v partition: %d is finished ",
		f.topicTalosResourceName, f.partitionId)
}

func (f *PartitionFetcher) IsServing() bool {
	f.fetcherLock.Lock()
	defer f.fetcherLock.Unlock()
	return f.curState == LOCKED
}

func (f *PartitionFetcher) IsHoldingLock() bool {
	f.fetcherLock.Lock()
	defer f.fetcherLock.Unlock()
	return f.curState == LOCKED || f.curState == UNLOCKING
}

func (f *PartitionFetcher) GetCurCheckpoint() int64 {
	f.fetcherLock.Lock()
	defer f.fetcherLock.Unlock()
	if !(f.curState == LOCKED || f.curState == UNLOCKING) {
		return int64(message.MessageOffset_START_OFFSET)
	}
	return f.messageReader.GetCurCheckpoint()
}

func (f *PartitionFetcher) GetCurState() TaskState {
	f.fetcherLock.Lock()
	defer f.fetcherLock.Unlock()
	return f.curState
}

func (f *PartitionFetcher) updateState(targetState TaskState) bool {
	f.fetcherLock.Lock()
	defer f.fetcherLock.Unlock()
	log.Infof("PartitionFetcher for Partition: %d update status from: %s to %s",
		f.partitionId, f.curState.String(), targetState.String())

	switch targetState {
	case INIT:
		log.Errorf("targetState can never be INIT, updateState error for: %d",
			f.partitionId)
	case LOCKED:
		if f.curState == INIT || f.curState == UNLOCKED {
			f.curState = LOCKED
			return true
		}
		log.Errorf("targetState is LOCKED, but curState is: %s for partition: %d",
			f.curState.String(), f.partitionId)
	case UNLOCKING:
		if f.curState == LOCKED {
			f.curState = UNLOCKING
			return true
		}
		log.Errorf("targetState is UNLOCKING, but curState is: %s for partition: %d",
			f.curState.String(), f.partitionId)
	case UNLOCKED:
		if f.curState == UNLOCKING || f.curState == LOCKED {
			f.curState = UNLOCKED
			return true
		}
		log.Errorf("targetState is UNLOCKED, but curState is: %s for partition: %d",
			f.curState.String(), f.partitionId)
	case SHUTDOWNED:
		f.curState = SHUTDOWNED
	default:
	}
	return false
}

func (f *PartitionFetcher) Lock() {
	if f.updateState(LOCKED) {
		f.wg.Add(1)
		go f.fetcherStateMachine()
		log.Infof("Worker: %s invoke partition: %d to 'LOCKED', try to serve it.",
			f.workerId, f.partitionId)
	}
}

func (f *PartitionFetcher) Unlock() {
	if f.updateState(UNLOCKING) {
		log.Infof("Worker: %s has set partition: %d  to 'UNLOCKING', "+
			"it is revoking gracefully.", f.workerId, f.partitionId)
	}
}

func (f *PartitionFetcher) Shutdown() {
	// set UNLOCKING to stop read and wait fetcher gracefully quit
	f.updateState(UNLOCKING)
	log.Infof("Worker: %s try to shutdown partition: %d",
		f.workerId, f.partitionId)
	f.wg.Wait()
	f.updateState(SHUTDOWNED)
}

/**
 * conditions for releasePartition:
 * 1) LOCKED, stealPartition success but get startOffset failed
 * 2) UNLOCKING, stop to serve this partition
 */
func (f *PartitionFetcher) releasePartition() {
	// release lock, if unlock failed, we just wait ttl work.
	toReleaseList := make([]int32, 0)
	toReleaseList = append(toReleaseList, f.partitionId)
	consumeUnit := &consumer.ConsumeUnit{
		ConsumerGroup:          f.consumerGroup,
		TopicTalosResourceName: f.topicTalosResourceName,
		PartitionIdList:        toReleaseList,
		WorkerId:               f.workerId}
	unlockRequest := &consumer.UnlockPartitionRequest{ConsumeUnit: consumeUnit}
	if err := f.consumerClient.UnlockPartition(unlockRequest); err != nil {
		log.Warnf("Worker: %s release partition error: %s",
			f.workerId, err.Error())
		return
	}
	log.Infof("Worker: %s success to release partition: %d",
		f.workerId, f.partitionId)
}

func (f *PartitionFetcher) stealPartition() bool {
	state := f.GetCurState()
	if state != LOCKED {
		log.Errorf("Worker: %s try to stealPartitionLock: %d but got state: %s",
			f.workerId, f.partitionId, state.String())
		return false
	}

	// steal lock, if lock failed, we skip it and wait next re-balance
	toStealList := make([]int32, 0)
	toStealList = append(toStealList, f.partitionId)
	consumeUnit := &consumer.ConsumeUnit{
		ConsumerGroup:          f.consumerGroup,
		TopicTalosResourceName: f.topicTalosResourceName,
		PartitionIdList:        toStealList,
		WorkerId:               f.workerId}

	lockRequest := &consumer.LockPartitionRequest{ConsumeUnit: consumeUnit}
	lockResponse, err := f.consumerClient.LockPartition(lockRequest)
	if err != nil {
		log.Errorf("Worker: %s steal partition error: %s", f.workerId, err.Error())
		return false
	}

	// get the successfully locked partition
	successPartitionList := lockResponse.GetSuccessPartitions()
	if len(successPartitionList) > 0 {
		err = utils.CheckArgument(successPartitionList[0] == f.partitionId)
		if err != nil {
			log.Errorf("lock partition failed: %s", err.Error())
			return false
		}
		log.Infof("Worker: %s success to lock partitions: %d",
			f.workerId, f.partitionId)
		return true
	}
	log.Errorf("Worker: %s failed to lock partitions: %d",
		f.workerId, f.partitionId)
	return false
}

func (f *PartitionFetcher) clean() {
	f.releasePartition()
	f.updateState(UNLOCKED)
}
