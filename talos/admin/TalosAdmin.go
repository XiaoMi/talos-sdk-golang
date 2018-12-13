/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package admin

import (
	"github.com/XiaoMi/talos-sdk-golang/talos/client"
	"github.com/XiaoMi/talos-sdk-golang/talos/thrift/auth"
	"github.com/XiaoMi/talos-sdk-golang/talos/thrift/message"
	"github.com/XiaoMi/talos-sdk-golang/talos/thrift/quota"
	"github.com/XiaoMi/talos-sdk-golang/talos/thrift/topic"
)

type Admin interface {
	DescribeTopic(request *topic.DescribeTopicRequest) (*topic.Topic, error)
}

type TalosAdmin struct {
	topicClient   topic.TopicService
	messageClient message.MessageService
	quotaClient   quota.QuotaService
	credential    *auth.Credential
}

func NewTalosAdmin(clientFactory *client.TalosClientFactory) *TalosAdmin {
	return &TalosAdmin{
		topicClient:   clientFactory.NewTopicClientDefault(),
		messageClient: clientFactory.NewMessageClientDefault(),
		quotaClient:   clientFactory.NewQuotaClientDefault(),
		credential:    clientFactory.GetCredential(),
	}
}

func (a *TalosAdmin) DescribeTopic(request *topic.DescribeTopicRequest) (*topic.Topic, error) {
	describeTopicResponse, err := a.topicClient.DescribeTopic(request)
	if err != nil {
		return nil, err
	}
	return &topic.Topic{
		TopicInfo:      describeTopicResponse.GetTopicInfo(),
		TopicAttribute: describeTopicResponse.GetTopicAttribute(),
		TopicState:     describeTopicResponse.GetTopicState(),
		TopicQuota:     describeTopicResponse.GetTopicQuota(),
		TopicAcl:       describeTopicResponse.GetAclMap(),
	}, nil
}
