/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com  
*/

package client

import (
  "github.com/XiaoMi/talos-sdk-golang/talos/thrift/topic"
)

type TopicAbnormalCallback interface {
  AbnormalHandler(topicTalosResourceName *topic.TopicTalosResourceName, err error)
}
