/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com  
*/

package producer

import (
  "math"
  "../utils"
)

type SimplePartitioner struct {

}

func (p *SimplePartitioner) partition(partitionKey string, partitionNum int) int {
  partitionInterval := math.MaxInt32 / partitionNum
  return ((utils.HashCode([]rune(partitionKey)) & 0x7FFFFFFF) / partitionInterval) % partitionNum
}
