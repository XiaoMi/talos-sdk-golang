/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package producer

import (
	"container/list"
	"sync"
	"time"

	"github.com/XiaoMi/talos-sdk-golang/thrift/message"
	"github.com/XiaoMi/talos-sdk-golang/utils"
	"github.com/sirupsen/logrus"
)

type PartitionMessageQueue struct {
	userMessageList *list.List
	curMessageBytes int64
	partitionId     int32
	talosProducer   *TalosProducer
	maxBufferedTime int64
	maxPutMsgNumber int64
	maxPutMsgBytes  int64
	Mutex           sync.Mutex
	QueueEmptyChan  chan utils.LockState
	NofityChan      chan utils.LockState
	log             *logrus.Logger
}

// Constructor method for test
func NewPartitionMessageQueueForTest(producerConfig *TalosProducerConfig,
	partitionId int32, talosProducerMock *TalosProducer) *PartitionMessageQueue {
	return &PartitionMessageQueue{
		userMessageList: list.New(),
		curMessageBytes: 0,
		partitionId:     partitionId,
		talosProducer:   talosProducerMock,
		maxBufferedTime: producerConfig.GetMaxBufferedMsgTime(),
		maxPutMsgNumber: producerConfig.GetMaxPutMsgNumber(),
		maxPutMsgBytes:  producerConfig.GetMaxPutMsgBytes(),
	}
}

func NewPartitionMessageQueue(producerConfig *TalosProducerConfig,
	partitionId int32, talosProducer *TalosProducer) *PartitionMessageQueue {
	return &PartitionMessageQueue{
		userMessageList: list.New(),
		curMessageBytes: 0,
		partitionId:     partitionId,
		talosProducer:   talosProducer,
		maxBufferedTime: producerConfig.GetMaxBufferedMsgTime(),
		maxPutMsgNumber: producerConfig.GetMaxPutMsgNumber(),
		maxPutMsgBytes:  producerConfig.GetMaxPutMsgBytes(),
		QueueEmptyChan:  make(chan utils.LockState, 1),
		NofityChan:      make(chan utils.LockState),
		log:             talosProducer.log,
	}
}

func (q *PartitionMessageQueue) AddMessage(messageList []*UserMessage) {
	// notify partitionSender to getUserMessageList
	q.Mutex.Lock()
	defer q.Mutex.Unlock()

	incrementBytes := int64(0)
	for _, userMessage := range messageList {
		q.userMessageList.PushFront(userMessage)
		incrementBytes += userMessage.GetMessageSize()
	}
	q.curMessageBytes += incrementBytes
	// update total buffered count when add messageList
	q.talosProducer.IncreaseBufferedCount(int64(len(messageList)),
		incrementBytes)

	//notify partitionSender to getUserMessageList
	if len(q.QueueEmptyChan) > 0 && len(q.NofityChan) == 0 {
		<-q.QueueEmptyChan
		q.NofityChan <- utils.NOTIFY
	}
}

/**
 * return messageList, if not shouldPut, block in this method
 */
func (q *PartitionMessageQueue) GetMessageList() []*message.Message {
	q.Mutex.Lock()
	defer q.Mutex.Unlock()

	for !q.shouldPut() {
		if waitTime := q.getWaitTime(); waitTime == 0 {
			q.Mutex.Unlock()
			//block wehn queue size is 0
			q.QueueEmptyChan <- utils.NOTIFY
			<-q.NofityChan
			q.Mutex.Lock()
		} else {
			q.Mutex.Unlock()
			time.Sleep(time.Duration(waitTime) * time.Millisecond)
			q.Mutex.Lock()
		}
	}
	q.log.Debugf("getUserMessageList wake up for partition: %d", q.partitionId)

	returnList := make([]*message.Message, 0)
	returnMsgBytes, returnMsgNumber := int64(0), int64(0)

	for q.userMessageList.Len() > 0 &&
		returnMsgNumber < q.maxPutMsgNumber && returnMsgBytes < q.maxPutMsgBytes {
		// userMessageList pollLast
		userMessage := q.userMessageList.Remove(q.userMessageList.Back()).(*UserMessage)
		returnList = append(returnList, userMessage.GetMessage())
		q.curMessageBytes -= userMessage.GetMessageSize()
		returnMsgBytes += userMessage.GetMessageSize()
		returnMsgNumber++
	}

	// update total buffered count when poll messageList
	q.talosProducer.DecreaseBufferedCount(returnMsgNumber, returnMsgBytes)
	q.log.Debugf("Ready to put message batch: %d, queue size: %d and curBytes: %d"+
		" for partition: %d", len(returnList), q.userMessageList.Len(),
		q.curMessageBytes, q.partitionId)

	return returnList
}

func (q *PartitionMessageQueue) shouldPut() bool {
	// when TalosProducer is not active;
	if !q.talosProducer.IsActive() {
		return true
	}

	// when we have enough bytes data or enough number data;
	if q.curMessageBytes >= q.maxPutMsgBytes ||
		int64(q.userMessageList.Len()) >= q.maxPutMsgNumber {
		return true
	}

	// when there have at least one message and it has exist enough long time;
	if q.userMessageList.Len() > 0 && (utils.CurrentTimeMills()-
		q.userMessageList.Back().Value.(*UserMessage).GetTimestamp()) >= q.maxBufferedTime {
		return true
	}
	return false
}

/**
 * Note: 1. wait(0) represents wait infinite until be notified
 * 2. wait minimal 1 milli secs when time <= 0
 */
func (q *PartitionMessageQueue) getWaitTime() int64 {
	if q.userMessageList.Len() <= 0 {
		return 0
	}
	waitTime := q.userMessageList.Back().Value.(*UserMessage).GetTimestamp() +
		q.maxBufferedTime - utils.CurrentTimeMills()
	if waitTime > 0 {
		return waitTime
	} else {
		return 1
	}
}

func (q *PartitionMessageQueue) shutdown() {
	close(q.NofityChan)
	close(q.QueueEmptyChan)
}
