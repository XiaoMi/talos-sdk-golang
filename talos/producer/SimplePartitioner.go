/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package producer

import (
	"../utils"
	"math"
)

type SimplePartitioner struct {
}

func (p *SimplePartitioner) partition(partitionKey string, partitionNum int32) int32 {
	partitionInterval := math.MaxInt32 / partitionNum
	return (int32(utils.HashCode([]rune(partitionKey))&0x7FFFFFFF) / partitionInterval) % partitionNum
}
