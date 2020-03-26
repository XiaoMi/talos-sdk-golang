/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package client

import (
	"github.com/XiaoMi/talos-sdk-golang/thrift/topic"
	log "github.com/sirupsen/logrus"
)

type SimpleTopicAbnormalCallback struct {
}

func NewSimpleTopicAbnormalCallback() *SimpleTopicAbnormalCallback {
	return &SimpleTopicAbnormalCallback{}
}

func (c *SimpleTopicAbnormalCallback) AbnormalHandler(resourceName *topic.TopicTalosResourceName, err error) {
	log.Errorf("Topic: %s has abnormal error: %s", resourceName.
		GetTopicTalosResourceName(), err)
}
