/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package consumer

import (
	"sync/atomic"

	"github.com/XiaoMi/talos-sdk-golang/talos/thrift/consumer"
	"github.com/XiaoMi/talos-sdk-golang/talos/thrift/topic"
	"github.com/XiaoMi/talos-sdk-golang/talos/utils"
	"github.com/XiaoMi/talos-sdk-golang/talos/thrift/message"
	log "github.com/alecthomas/log4go"
)

type Long struct {
	Valid bool
	Value int64
}

type MessageReader struct {
	commitThreshold int64
	commitInterval  int64
	fetchInterval   int64

	lastCommitTime int64
	lastFetchTime  int64

	startOffset      *int64
	finishedOffset   int64
	lastCommitOffset int64
	messageProcessor MessageProcessor

	workerId          string
	consumerGroup     string
	topicAndPartition *topic.TopicAndPartition
	consumerConfig    *TalosConsumerConfig
	simpleConsumer    *SimpleConsumer
	consumerClient    consumer.ConsumerService
	outerCheckpoint   Long
}

func NewMessageReader(consumerConfig *TalosConsumerConfig) *MessageReader {
	curTime := utils.CurrentTimeMills()
	initOffset := int64(-1)
	return &MessageReader{
		consumerConfig:   consumerConfig,
		lastCommitOffset: initOffset,
		finishedOffset:   initOffset,
		lastCommitTime:   curTime,
		lastFetchTime:    curTime,
		startOffset:      &initOffset,
		commitThreshold:  consumerConfig.GetCommitOffsetThreshold(),
		commitInterval:   consumerConfig.GetCommitOffsetInterval(),
		fetchInterval:    consumerConfig.GetFetchMessageInterval(),
		outerCheckpoint:  Long{},
	}
}

func (r *MessageReader) SetWorkerId(workerId string) *MessageReader {
	r.workerId = workerId
	return r
}

func (r *MessageReader) SetConsumerGroup(consumerGroup string) *MessageReader {
	if err := utils.CheckNameValidity(consumerGroup); err != nil {
		log.Error("consumerGroup: %s", err.Error())
		return nil
	}
	r.consumerGroup = consumerGroup
	return r
}

func (r *MessageReader) SetTopicAndPartition(topicAndPartition *topic.TopicAndPartition) *MessageReader {
	r.topicAndPartition = topicAndPartition
	return r
}

func (r *MessageReader) SetSimpleConsumer(simpleConsumer *SimpleConsumer) *MessageReader {
	r.simpleConsumer = simpleConsumer
	return r
}

func (r *MessageReader) SetMessageProcessor(messageProcessor MessageProcessor) *MessageReader {
	r.messageProcessor = messageProcessor
	return r
}

func (r *MessageReader) SetConsumerClient(consumerClient consumer.ConsumerService) *MessageReader {
	r.consumerClient = consumerClient
	return r
}

func (r *MessageReader) SetOuterCheckpoint(outerCheckpoint Long) *MessageReader {
	r.outerCheckpoint = outerCheckpoint
	return r
}

func (r *MessageReader) StartOffset() *int64 {
	return r.startOffset
}

func (r *MessageReader) GetCurCheckpoint() int64 {
	// init state or before the first committing
	if r.lastCommitOffset <= atomic.LoadInt64(r.startOffset) {
		return atomic.LoadInt64(r.startOffset)
	}

	// From lastCommitOffset + 1 when reading next time
	return r.lastCommitOffset + 1
}

func (r *MessageReader) ShouldCommit() bool {
	return (utils.CurrentTimeMills()-r.lastCommitTime >= r.commitInterval) ||
		(r.finishedOffset-r.lastCommitOffset >= r.commitThreshold)
}

func (r *MessageReader) processFetchException(err error) {
	if r.consumerConfig.resetLatestOffsetWhenOutOfRange {
		log.Warn("Got PartitionOutOfRange error, offset by current latest offset.")
		atomic.StoreInt64(r.startOffset, int64(message.MessageOffset_LATEST_OFFSET))
		r.lastCommitOffset = -1
		r.finishedOffset = -1
		r.lastCommitTime = utils.CurrentTimeMills()
	} else {
		log.Warn("Got PartitionOutOfRange error, reset offset by current start offset.")
		atomic.StoreInt64(r.startOffset, int64(message.MessageOffset_START_OFFSET))
		r.lastCommitOffset = -1
		r.finishedOffset = -1
		r.lastCommitTime = utils.CurrentTimeMills()
	}
	log.Warn("process unexcepted fetchException: %s", err.Error())
}
