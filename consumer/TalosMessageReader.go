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
	"github.com/XiaoMi/talos-sdk-golang/utils"
	"github.com/sirupsen/logrus"
)

type TalosMessageReader struct {
	*MessageReader
}

func NewTalosMessageReader(config *TalosConsumerConfig, logger *logrus.Logger) *TalosMessageReader {
	return &TalosMessageReader{
		MessageReader: NewMessageReader(config, logger),
	}
}

func (r *TalosMessageReader) InitStartOffset() error {
	// get last commit offset or init by outer checkPoint
	var readingStartOffset int64
	var err error
	if r.outerCheckpoint.Valid && r.outerCheckpoint.Value >= 0 {
		readingStartOffset = r.outerCheckpoint.Value
		// Long struct build for burning outerCheckpoint after the first reading
		r.outerCheckpoint.Valid = false
	} else {
		readingStartOffset, err = r.queryStartOffset()
		if err != nil {
			return err
		}
	}

	// when consumer starting up, checking:
	// 1) whether not exist last commit offset, which means 'readingStartOffset==-1'
	// 2) whether reset offset
	// 3) note that: the priority of 'reset-config' is larger than 'outer-checkPoint'
	if readingStartOffset == -1 || r.consumerConfig.GetResetOffsetWhenStart() {
		atomic.StoreInt64(r.startOffset, r.consumerConfig.GetResetOffsetValueWhenStart())
	} else {
		atomic.StoreInt64(r.startOffset, readingStartOffset)
	}

	// guarantee lastCommitOffset and finishedOffset correct
	if atomic.LoadInt64(r.startOffset) > 0 {
		r.lastCommitOffset = atomic.LoadInt64(r.startOffset) - 1
		r.finishedOffset = r.lastCommitOffset
	}
	r.log.Infof("Init startOffset: %d lastCommitOffset: %d for partition: %d ",
		atomic.LoadInt64(r.startOffset), r.lastCommitOffset,
		r.topicAndPartition.GetPartitionId())
	return nil
}

func (r *TalosMessageReader) CommitCheckPoint() error {
	err := r.innerCheckpoint()
	if err != nil {
		return err
	}
	return err
}

func (r *TalosMessageReader) FetchData() {
	// control fetch qps
	currentTime := utils.CurrentTimeMills()
	if currentTime-r.lastFetchTime < r.fetchInterval {
		sleepTime := r.lastFetchTime + r.fetchInterval - currentTime
		time.Sleep(time.Duration(sleepTime) * time.Millisecond)
	}

	// fetch data and process them
	r.log.Debugf("Reading message from offset: %d of partition: %d ",
		atomic.LoadInt64(r.startOffset), r.topicAndPartition.GetPartitionId())
	startFetchTime := utils.CurrentTimeMills()
	messageList, err := r.simpleConsumer.FetchMessage(
		atomic.LoadInt64(r.startOffset), r.consumerConfig.GetMaxFetchRecords())
	if err != nil {
		r.consumerMetrics.MarkFetchOrProcessFailedTimes()
		r.processFetchException(err)
		r.lastFetchTime = utils.CurrentTimeMills()
		return
	}

	r.lastFetchTime = utils.CurrentTimeMills()
	r.consumerMetrics.MarkFetchDuration(r.lastFetchTime - startFetchTime)
	//return and check should commit when no message get
	if messageList == nil || len(messageList) == 0 {
		r.CheckAndCommit(false)
		return
	}

	/**
	 * Note: We guarantee the committed offset must be the messages that
	 * have been processed by user's MessageProcessor;
	 */
	r.finishedOffset = messageList[len(messageList)-1].GetMessageOffset()
	startProcessTime := utils.CurrentTimeMills()
	r.messageProcessor.Process(messageList, r)
	r.consumerMetrics.MarkProcessDuration(utils.CurrentTimeMills() - startProcessTime)
	atomic.StoreInt64(r.startOffset, r.finishedOffset+1)
	r.CheckAndCommit(true)
}

func (r *TalosMessageReader) queryStartOffset() (int64, error) {
	queryOffsetRequest := &consumer.QueryOffsetRequest{
		ConsumerGroup:     r.consumerGroup,
		TopicAndPartition: r.topicAndPartition,
	}
	queryOffsetResponse, err := r.consumerClient.QueryOffset(queryOffsetRequest)
	if err != nil {
		return 0, err
	}

	committedOffset := queryOffsetResponse.GetMsgOffset()
	// 'committedOffset == -1' means not exist last committed offset
	// startOffset = committedOffset + 1
	if committedOffset == -1 {
		return committedOffset, nil
	} else {
		return committedOffset + 1, nil
	}
}

func (r *TalosMessageReader) innerCheckpoint() error {
	if r.consumerConfig.GetCheckpointAutoCommit() {
		if err := r.commitOffset(r.finishedOffset); err != nil {
			return err
		}
	}
	return nil
}

func (r *TalosMessageReader) CheckpointByFinishedOffset() bool {
	return r.Checkpoint(r.finishedOffset)
}

func (r *TalosMessageReader) Checkpoint(messageOffset int64) bool {
	r.log.Infof("Start checkpoint: %v", messageOffset)
	if r.consumerConfig.GetCheckpointAutoCommit() {
		r.log.Infof("You can not checkpoint through MessageCheckpointer when you set " +
			"\"galaxy.talos.consumer.checkpoint.message.offset\" as \"true\"")
		return false
	}

	if messageOffset <= r.lastCommitOffset || messageOffset > r.finishedOffset {
		r.log.Infof("checkpoint messageOffset: %v in wrong range, lastCheckpoint "+
			"messageOffset: %v , last deliver messageOffset: %v", messageOffset,
			r.lastCommitOffset, r.finishedOffset)
		return false
	}

	err := r.commitOffset(messageOffset)
	if err != nil {
		r.log.Errorf("Error: %s when getting messages from topic: %v, partition: %d",
			err.Error(), r.topicAndPartition.GetTopicTalosResourceName(),
			r.topicAndPartition.GetPartitionId())
		return false
	}
	return true
}

func (r *TalosMessageReader) commitOffset(messageOffset int64) error {
	checkPoint := &consumer.CheckPoint{
		ConsumerGroup:     r.consumerGroup,
		TopicAndPartition: r.topicAndPartition,
		MsgOffset:         messageOffset,
		WorkerId:          r.workerId,
	}
	// check whether to check last commit offset, first commit don't check
	if r.lastCommitOffset != -1 && r.consumerConfig.GetCheckLastCommitOffset() {
		checkPoint.LastCommitOffset = &r.lastCommitOffset
	}

	updateOffsetRequest := &consumer.UpdateOffsetRequest{Checkpoint: checkPoint}
	updateOffsetResponse, err := r.consumerClient.UpdateOffset(updateOffsetRequest)
	if err != nil {
		return err
	}

	// update startOffset as next message
	if updateOffsetResponse.GetSuccess() {
		r.lastCommitOffset = messageOffset
		r.lastCommitTime = utils.CurrentTimeMills()
		r.log.Infof("Worker: %s commit offset: %d for partition: %d",
			r.workerId, r.lastCommitOffset, r.topicAndPartition.GetPartitionId())
	} else {
		r.log.Errorf("Worker: %s commit offset: %d for partition: %d failed",
			r.workerId, r.lastCommitOffset, r.topicAndPartition.GetPartitionId())
	}
	return nil
}

func (r *TalosMessageReader) CleanReader() {
	// wait task quit gracefully: stop reading, commit offset, clean and shutdown
	if r.finishedOffset > r.lastCommitOffset {
		if err := r.CommitCheckPoint(); err != nil {
			r.log.Errorf("Topic: %s, partition: %d commit offset error: %s",
				r.topicAndPartition.GetTopicTalosResourceName(),
				r.topicAndPartition.GetPartitionId(), err.Error())
		}
	}
}

func (r *TalosMessageReader) CheckAndCommit(isContinuous bool) {
	if r.ShouldCommit(isContinuous) {
		err := r.innerCheckpoint()
		if err != nil {
			r.log.Errorf("commit offset error: %s, we skip to it.", err.Error())
		}
	}
}
