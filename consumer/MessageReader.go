/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package consumer

import (
	"sync/atomic"
	"time"

	"github.com/XiaoMi/talos-sdk-golang/thrift/consumer"
	"github.com/XiaoMi/talos-sdk-golang/thrift/message"
	"github.com/XiaoMi/talos-sdk-golang/thrift/topic"
	"github.com/XiaoMi/talos-sdk-golang/utils"
	"github.com/sirupsen/logrus"
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
	log               *logrus.Logger
}

func NewMessageReader(consumerConfig *TalosConsumerConfig, logger *logrus.Logger) *MessageReader {
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
		log:              logger,
	}
}

func (r *MessageReader) SetWorkerId(workerId string) *MessageReader {
	r.workerId = workerId
	return r
}

func (r *MessageReader) SetConsumerGroup(consumerGroup string) *MessageReader {
	if err := utils.CheckNameValidity(consumerGroup); err != nil {
		r.log.Errorf("consumerGroup: %s", err.Error())
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
	startOffset := atomic.LoadInt64(r.startOffset)
	if r.lastCommitOffset <= startOffset {
		return startOffset
	}

	// From lastCommitOffset + 1 when reading next time
	return r.lastCommitOffset + 1
}

func (r *MessageReader) ShouldCommit(isContinuous bool) bool {
	if isContinuous {
		return (utils.CurrentTimeMills()-r.lastCommitTime >= r.commitInterval) ||
			(r.finishedOffset-r.lastCommitOffset >= r.commitThreshold)
	} else {
		return (utils.CurrentTimeMills()-r.lastCommitTime >= r.commitInterval) &&
			(r.finishedOffset > r.lastCommitOffset)
	}
}

func (r *MessageReader) processFetchException(err error) {
	if utils.IsPartitionNotServing(err) {
		r.log.Warnf("Partition: %d is not serving state: %s, "+
			"sleep a while for waiting it work.",
			r.topicAndPartition.GetPartitionId(), err.Error())
		time.Sleep(time.Duration(r.consumerConfig.GetWaitPartitionWorkingTime()) * time.Millisecond)
	} else if utils.IsOffsetOutOfRange(err) {
		// process message offset out of range, reset start offset
		if r.consumerConfig.resetLatestOffsetWhenOutOfRange {
			r.log.Warnf("Got PartitionOutOfRange error, offset by current latest offset.")
			atomic.StoreInt64(r.startOffset, int64(message.MessageOffset_LATEST_OFFSET))
			r.lastCommitTime = utils.CurrentTimeMills()
		} else {
			r.log.Warnf("Got PartitionOutOfRange error, reset offset by current start offset.")
			atomic.StoreInt64(r.startOffset, int64(message.MessageOffset_START_OFFSET))
			r.lastCommitOffset, r.finishedOffset = -1, -1
			r.lastCommitTime = utils.CurrentTimeMills()
		}
	} else {
		r.log.Errorf("Reading message from topic: %v of partition: %d failed: %s",
			r.topicAndPartition.GetTopicTalosResourceName(),
			r.topicAndPartition.GetPartitionId(), err)
	}
}
