/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package producer

import (
	"../../../talos/producer"
	"github.com/nu7hatch/gouuid"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_Partition(t *testing.T) {
	partitioner := producer.SimplePartitioner{}
	for i := 0; i < 100; i++ {
		key := randomKey()
		assert.Equal(t, getPartitionId(hashCode([]rune(key))&0x7FFFFFFF), partitioner.Partition(key, 4))
	}
}

func getPartitionId(hashcode int) int32 {
	if hashcode >= 0 && hashcode < 536870911 {
		return 0
	} else if hashcode >= 536870911 && hashcode < 1073741822 {
		return 1
	} else if hashcode >= 1073741822 && hashcode < 1610612733 {
		return 2
	} else if hashcode >= 1610612733 && hashcode < 2147483644 {
		return 3
	}
	return 0
}

func randomKey() string {
	uid, _ := uuid.NewV4()
	return uid.String()
}

func hashCode(value []rune) int {
	h := 0
	if len(value) > 0 {
		val := value
		for i := 0; i < len(value); i++ {
			h = 31*h + int(val[i])
		}
	}
	return h
}
