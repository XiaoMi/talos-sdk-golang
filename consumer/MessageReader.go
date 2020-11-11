/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package consumer

import (
	"strconv"
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

type ConsumerMetrics struct {
	fetchDuration      int64
	maxFetchDuration   int64
	processDuration    int64
	maxProcessDuration int64
	fetchTimes         int
	fetchInterval      int
	fetchFailedTimes   int
	consumerMetricsMap map[string]float64
}

func NewConsumerMetrics() *ConsumerMetrics {
	return &ConsumerMetrics{
		fetchDuration:      0,
		maxFetchDuration:   0,
		processDuration:    0,
		maxProcessDuration: 0,
		fetchTimes:         0,
		fetchInterval:      0,
		fetchFailedTimes:   0,
		consumerMetricsMap: make(map[string]float64),
	}
}

func (m *ConsumerMetrics) MarkFetchDuration(fetchDuration int64) {
	if fetchDuration > m.maxFetchDuration {
		m.maxFetchDuration = fetchDuration
	}

	m.fetchDuration = fetchDuration
	m.fetchTimes += 1
}

func (m *ConsumerMetrics) MarkFetchOrProcessFailedTimes() {
	m.fetchFailedTimes += 1
	m.fetchTimes += 1
}

func (m *ConsumerMetrics) MarkProcessDuration(processDuration int64) {
	if processDuration > m.maxProcessDuration {
		m.maxProcessDuration = processDuration
	}

	m.processDuration = processDuration
}

func (m *ConsumerMetrics) MarkFetchInterval(fetchInterval int) {
	m.fetchInterval = fetchInterval
}

func (m *ConsumerMetrics) updateMetricsMap() {
	m.consumerMetricsMap[utils.FETCH_MESSAGE_TIME] =         float64(m.fetchDuration)
	m.consumerMetricsMap[utils.MAX_FETCH_MESSAGE_TIME] =     float64(m.maxFetchDuration)
	m.consumerMetricsMap[utils.PROCESS_MESSAGE_TIME] =       float64(m.processDuration)
	m.consumerMetricsMap[utils.MAX_PROCESS_MESSAGE_TIME] =   float64(m.maxProcessDuration)
	m.consumerMetricsMap[utils.FETCH_MESSAGE_TIMES] =        float64(m.fetchTimes) / 60.0
	m.consumerMetricsMap[utils.FETCH_MESSAGE_FAILED_TIMES] = float64(m.fetchFailedTimes) / 60.0
}

func (m *ConsumerMetrics) initMetrics() {
	m.fetchDuration      = 0
	m.maxFetchDuration   = 0
	m.processDuration    = 0
	m.maxProcessDuration = 0
	m.fetchTimes         = 0
	m.fetchFailedTimes   = 0
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
	consumerMetrics   *ConsumerMetrics
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

func (r *MessageReader) InitConsumerMetrics() *MessageReader {
	r.consumerMetrics = NewConsumerMetrics()
	return r
}

func (r *MessageReader) NewFalconMetrics() []*utils.FalconMetric {
	var metrics []*utils.FalconMetric
	tags := utils.NewTags()
	tags.SetTag("clusterName", r.consumerConfig.ClusterName())
	tags.SetTag("topicName", r.topicAndPartition.GetTopicName())
	tags.SetTag("partitionId", strconv.Itoa(int(r.topicAndPartition.GetPartitionId())))
	tags.SetTag("ip", r.consumerConfig.ClientIp())
	tags.SetTag("type", r.consumerConfig.AlertType())

	r.consumerMetrics.updateMetricsMap()
	for name, value := range r.consumerMetrics.consumerMetricsMap {
		metric := utils.NewFalconMetric(r.consumerConfig.ConsumerMetricFalconEndpoint() + r.consumerGroup,
			name, r.consumerConfig.MetricFalconStep(), value, tags)
		metrics = append(metrics, metric)
	}
	r.consumerMetrics.initMetrics()
	return metrics
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
