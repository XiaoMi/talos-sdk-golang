/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package producer

type Partitioner interface {

	/**
	 * Get partition id for specified partitionKey
	 */
	partition(partititonKey string, partitionNumber int) int
}
