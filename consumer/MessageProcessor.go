/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package consumer

import (
	"github.com/XiaoMi/talos-sdk-golang/thrift/message"
)

type MessageProcessor interface {

	/**
	 * User implement this method and process the messages read from Talos
	 * MessageCheckpointer -> TalosMessageReader
	 */
	Process(messages []*message.MessageAndOffset, messageCheckpointer MessageCheckpointer)
}
