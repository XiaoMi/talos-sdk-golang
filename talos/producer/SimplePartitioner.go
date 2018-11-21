/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package producer

import (
	"github.com/XiaoMi/talos-sdk-golang/talos/utils"
	"math"
)

type SimplePartitioner struct {
}

func NewSimplePartitioner() *SimplePartitioner {
	return &SimplePartitioner{}
}

func (p *SimplePartitioner) partition(partitionKey string, partitionNum int) int {
	partitionInterval := math.MaxInt32 / partitionNum
	return ((utils.HashCode([]rune(partitionKey)) & 0x7FFFFFFF) / partitionInterval) % partitionNum
}
