/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package producer

type TalosProducer struct {
	partitioner                Partitioner
	updatePartitionIdInterval  int64
	lastUpdatePartitionIdTime  int64
	updatePartitionIdMsgNumber int64
	lastAddMsgNumber           int64
}
