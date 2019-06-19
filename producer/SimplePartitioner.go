/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package producer

import (
	"math"

	"github.com/XiaoMi/talos-sdk-golang/utils"
)

type SimplePartitioner struct {
}

func NewSimplePartitioner() *SimplePartitioner {
	return &SimplePartitioner{}
}

func (p *SimplePartitioner) Partition(partitionKey string, partitionNum int32) int32 {
	partitionInterval := math.MaxInt32 / partitionNum
	return (int32(utils.HashCode([]rune(partitionKey))&0x7FFFFFFF) / partitionInterval) % partitionNum
}
