/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package producer

import (
	"fmt"
	"strconv"
	"time"

	"sync/atomic"

	"github.com/XiaoMi/talos-sdk-golang/thrift/message"
	"github.com/XiaoMi/talos-sdk-golang/thrift/topic"
	"github.com/XiaoMi/talos-sdk-golang/utils"
	"github.com/sirupsen/logrus"
)

type ProducerMetrics struct {
	putMsgDuration     int64
	maxPutMsgDuration  int64
	minPutMsgDuration  int64
	putMsgTimes        int32
	putMsgFailedTimes  int32
	producerMetricsMap map[string]float64
}

func NewProducerMetrics() *ProducerMetrics {
	return &ProducerMetrics{
		putMsgDuration:     0,
		maxPutMsgDuration:  0,
		minPutMsgDuration:  0,
		putMsgTimes:        0,
		putMsgFailedTimes:  0,
		producerMetricsMap: make(map[string]float64),
	}
}

func (m *ProducerMetrics) MarkPutMsgDuration(putMsgDuration int64) {
	if putMsgDuration > atomic.LoadInt64(&m.maxPutMsgDuration) {
		atomic.StoreInt64(&m.maxPutMsgDuration, putMsgDuration)
	}

	if atomic.LoadInt64(&m.minPutMsgDuration) == 0 || putMsgDuration < atomic.LoadInt64(&m.minPutMsgDuration) {
		atomic.StoreInt64(&m.minPutMsgDuration, putMsgDuration)
	}

	atomic.StoreInt64(&m.putMsgDuration, putMsgDuration)
	atomic.AddInt32(&m.putMsgTimes, 1)
}

func (m *ProducerMetrics) MarkPutMsgFailedTimes() {
	atomic.AddInt32(&m.putMsgTimes, 1)
	atomic.AddInt32(&m.putMsgFailedTimes, 1)
}

func (m *ProducerMetrics) updateMetricsMap() {
	m.producerMetricsMap[utils.PUT_MESSAGE_TIME]         = float64(atomic.LoadInt64(&m.putMsgDuration))
	m.producerMetricsMap[utils.MAX_PUT_MESSAGE_TIME]     = float64(atomic.LoadInt64(&m.maxPutMsgDuration))
	m.producerMetricsMap[utils.MIN_PUT_MESSAGE_TIME]     = float64(atomic.LoadInt64(&m.minPutMsgDuration))
	m.producerMetricsMap[utils.PUT_MESSAGE_TIMES]        = float64(atomic.LoadInt32(&m.putMsgTimes)) / 60.0
	m.producerMetricsMap[utils.PUT_MESSAGE_FAILED_TIMES] = float64(atomic.LoadInt32(&m.putMsgFailedTimes)) / 60.0
}

func (m *ProducerMetrics) initMetrics() {
	atomic.StoreInt64(&m.putMsgDuration, 0)
	atomic.StoreInt64(&m.maxPutMsgDuration, 0)
	atomic.StoreInt64(&m.minPutMsgDuration, 0)
	atomic.StoreInt32(&m.putMsgTimes, 0)
	atomic.StoreInt32(&m.putMsgFailedTimes, 0)
}

type Sender interface {
	AddMessage(userMessageList []*UserMessage)
	Shutdown()
	MessageCallbackTask(userMessageResult *UserMessageResult)
	MessageWriterTask()
}

type PartitionSender struct {
	partitionId           int32
	requestId             atomic.Value
	clientId              string
	talosProducerConfig   *TalosProducerConfig
	messageClient         message.MessageService
	userMessageCallback   UserMessageCallback
	topicAndPartition     *topic.TopicAndPartition
	partitionMessageQueue *PartitionMessageQueue
	talosProducer         *TalosProducer
	userMessageResult     *UserMessageResult
	simpleProducer        *SimpleProducer
	MessageWriterStopSign chan utils.StopSign
	producerMetrics       *ProducerMetrics
	log                   *logrus.Logger
}

func NewPartitionSender(partitionId int32, topicName string,
	topicTalosResourceName *topic.TopicTalosResourceName, requestId atomic.Value,
	clientId string, producerConfig *TalosProducerConfig,
	messageClient message.MessageService, userMessageCallback UserMessageCallback,
	talosProducer *TalosProducer) *PartitionSender {

	topicAndPartition := &topic.TopicAndPartition{
		TopicName:              topicName,
		TopicTalosResourceName: topicTalosResourceName,
		PartitionId:            partitionId,
	}
	partitionMessageQueue := NewPartitionMessageQueue(producerConfig,
		partitionId, talosProducer)

	simpleProducer, err := NewSimpleProducerForHighLvl(producerConfig,
		topicAndPartition, messageClient, clientId, requestId,
		talosProducer.scheduleInfoCache, talosProducer.log)
	if err != nil {
		return nil
	}

	partitionSender := &PartitionSender{
		partitionId:           partitionId,
		requestId:             requestId,
		clientId:              clientId,
		talosProducerConfig:   producerConfig,
		messageClient:         messageClient,
		userMessageCallback:   userMessageCallback,
		talosProducer:         talosProducer,
		topicAndPartition:     topicAndPartition,
		partitionMessageQueue: partitionMessageQueue,
		simpleProducer:        simpleProducer,
		MessageWriterStopSign: make(chan utils.StopSign, 1),
		producerMetrics:       NewProducerMetrics(),
		log:                   talosProducer.log,
	}

	partitionSender.talosProducer.WaitGroup.Add(1)
	go partitionSender.MessageWriterTask()

	return partitionSender
}

func (s *PartitionSender) MessageCallbackTask(userMessageResult *UserMessageResult) {
	s.userMessageResult = userMessageResult
	if s.userMessageResult.IsSuccessful() {
		s.userMessageCallback.OnSuccess(s.userMessageResult)
	} else {
		s.userMessageCallback.OnError(s.userMessageResult)
	}
}

func (s *PartitionSender) MessageWriterTask() {
	defer s.talosProducer.WaitGroup.Done()

	for {
		select {
		case <-s.MessageWriterStopSign:
			s.log.Infof("MessageWriterTask stop")
			return
		default:
			messageList := s.partitionMessageQueue.GetMessageList()

			// when messageList return no message, this means TalosProducer not
			// alive and there is no more message to send , then we should exit
			// write message right now;
			if len(messageList) == 0 {
				// notify to wake up producer's
				if len(s.talosProducer.BufferFullChan) > 0 {
					<-s.talosProducer.BufferFullChan
				}
				break
			}

			err := s.putMessage(messageList)
			if err != nil {
				s.log.Errorf("PutMessageTask for topicAndPartition: %v error: %s",
					s.topicAndPartition, err.Error())
			}
			// when talosProducer buffer is full, take data and notify
			if len(s.talosProducer.BufferFullChan) > 0 {
				<-s.talosProducer.BufferFullChan
			}
		}
	}
}

func (s *PartitionSender) putMessage(messageList []*message.Message) error {
	userMessageResult := NewUserMessageResult(messageList, s.partitionId)

	// when TalosProducer is disabled, we just fail the message and inform user;
	// but when TalosProducer is shutdown, we will send the left message.
	if s.talosProducer.IsDisable() {
		return fmt.Errorf("The Topic: %s with resourceName: %s no longer exist. "+
			"Please check the topic and reconstruct the TalosProducer again. ",
			s.topicAndPartition.GetTopicName(),
			s.topicAndPartition.GetTopicTalosResourceName())
	}

	startPutMsgTime := utils.CurrentTimeMills()
	if err := s.simpleProducer.doPut(messageList); err != nil {
		s.producerMetrics.MarkPutMsgFailedTimes()
		s.log.Errorf("Failed to put %d messages for partition: %d",
			len(messageList), s.partitionId)
		for _, msg := range messageList {
			s.log.Debugf("%d: %s", msg.GetSequenceNumber(), string(msg.GetMessage()))
		}
		// putMessage failed callback
		userMessageResult.SetSuccessful(false).SetCause(err)
		go s.MessageCallbackTask(userMessageResult)

		// delay when partitionNotServing
		if utils.IsPartitionNotServing(err) {
			s.log.Warnf("partition: %d is not serving state, sleep "+
				"a while for waiting it work.", s.partitionId)
			time.Sleep(time.Duration(s.talosProducerConfig.GetWaitPartitionWorkingTime()) * time.Millisecond)
		}
		return err
	}
	s.producerMetrics.MarkPutMsgDuration(utils.CurrentTimeMills() - startPutMsgTime)
	// putMessage success callback
	userMessageResult.SetSuccessful(true)
	go s.MessageCallbackTask(userMessageResult)
	s.log.Debugf("put %d messages for partition: %d", len(messageList), s.partitionId)
	return nil
}

func (s *PartitionSender) AddMessage(userMessageList []*UserMessage) {
	s.partitionMessageQueue.AddMessage(userMessageList)
	s.log.Infof("add %d messages to partition: %d",
		len(userMessageList), s.partitionId)
}

func (s *PartitionSender) NewFalconMetrics() []*utils.FalconMetric {
	var metrics []*utils.FalconMetric

	tags := utils.NewTags()
	tags.SetTag("clusterName", s.talosProducerConfig.ClusterName())
	tags.SetTag("topicName", s.topicAndPartition.GetTopicName())
	tags.SetTag("partitionId", strconv.Itoa(int(s.topicAndPartition.GetPartitionId())))
	tags.SetTag("ip", s.talosProducerConfig.ClientIp())
	tags.SetTag("type", s.talosProducerConfig.AlertType())
	s.producerMetrics.updateMetricsMap()
	for name, value := range s.producerMetrics.producerMetricsMap {
		metric := utils.NewFalconMetric(s.talosProducerConfig.ProducerMetricFalconEndpoint() +
			s.topicAndPartition.GetTopicName(), name,
			s.talosProducerConfig.MetricFalconStep(), value, tags)
		metrics = append(metrics, metric)
	}
	s.producerMetrics.initMetrics()
	return metrics
}

func (s *PartitionSender) Shutdown() {
	// notify PartitionMessageQueue::getMessageList return;
	s.AddMessage(make([]*UserMessage, 0))
	s.MessageWriterStopSign <- utils.Shutdown
	s.partitionMessageQueue.shutdown()
	s.log.Infof("PartitionSender for partition: %d finish stop", s.partitionId)
}
