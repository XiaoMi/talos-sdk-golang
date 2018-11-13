/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package producer

import "container/list"

type PartitionMessageQueue struct {
	userMessageList list.List
	curMessageBytes int
	partitionId     int
	producer        TalosProducer
	maxBufferedTime int
	maxPutMsgNumber int
	maxPutMsgBytes  int
}
