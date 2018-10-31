/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com  
*/

package consumer

import (
  "github.com/XiaoMi/talos-sdk-golang/talos/thrift/topic"
  "../thrift/message"
)

type MessageProcessor interface {

  /**
   * TalosConsumer invoke init to indicate it will deliver message to the
   * MessageProcessor instance;
   */
  Init(topicAndPartition *topic.TopicAndPartition, startMessageOffset int64)

  /**
   * User implement this method and process the messages read from Talos
   * MessageCheckpointer -> TalosMessageReader
   */
  Process(messages []*message.MessageAndOffset, messageCheckpointer MessageCheckpointer)

  /**
   * TalosConsumer invoke shutdown to indicate it will no longer deliver message
   * to the MessageProcess instance.
   */
  Shutdown(messageCheckpointer MessageCheckpointer)
}








