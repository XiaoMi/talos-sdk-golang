/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com  
*/

package client

import (
  "../thrift/topic"
  log "github.com/alecthomas/log4go"
)

type SimpleTopicAbnormalCallback struct {

}

func (c *SimpleTopicAbnormalCallback) AbnormalHandler(
  topicTalosResourceName *topic.TopicTalosResourceName, err error) {
  log.Error("Topic abnormal exception %s, for topic: %s",
    topicTalosResourceName.GetTopicTalosResourceName(), err)
}
