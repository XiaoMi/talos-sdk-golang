/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package producer

type PartitionMessageQueue struct {
	userMessageList []UserMessage // or list
	curMessageBytes int
	partitionId     int32
	talosProducer   *TalosProducer
	maxBufferedTime int64
	maxPutMsgNumber int64
	maxPutMsgBytes  int64
}

func NewPartitionMessageQueue(producerConfig *TalosProducerConfig,
	partitionId int32, talosProducer *TalosProducer) *PartitionMessageQueue {

	userMessageList := make([]UserMessage, 0)
	return &PartitionMessageQueue{
		userMessageList: userMessageList,
		curMessageBytes: 0,
		partitionId:     partitionId,
		talosProducer:   talosProducer,
		maxBufferedTime: producerConfig.GetMaxBufferedMsgTime(),
		maxPutMsgNumber: producerConfig.GetMaxPutMsgNumber(),
		maxPutMsgBytes:  producerConfig.GetMaxPutMsgBytes(),
	}
}

func (q *PartitionMessageQueue) AddMessage(messageList []UserMessage) {
	incrementBytes := 0
	for i, userMessage := range messageList {
		q.userMessageList = append(q.userMessageList, userMessage)
	}
	q.talosProducer.increaseBufferedCount()
}
