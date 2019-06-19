/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package consumer

type MessageProcessorFactory interface {

	/**
	 * Returns a message processor
	 */
	CreateProcessor() MessageProcessor
}
