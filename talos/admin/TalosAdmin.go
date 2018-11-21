/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package admin

import (
	"github.com/XiaoMi/talos-sdk-golang/talos/thrift/auth"
	"github.com/XiaoMi/talos-sdk-golang/talos/thrift/message"
	"github.com/XiaoMi/talos-sdk-golang/talos/thrift/quota"
	"github.com/XiaoMi/talos-sdk-golang/talos/thrift/topic"
)

type TalosAdmin struct {
	topicClient   topic.TopicService
	messageClient message.MessageService
	quotaClient   quota.QuotaService
	credential    *auth.Credential
}

//func NewTalosAdmin(clientFactory client.TalosClientFactory) *TalosAdmin {
//	clientConfig := clientFactory.TalosClientConfig
//	return &TalosAdmin{
//		topicClient:clientFactory.NewTopicClient(clientConfig.ServiceEndpoint()),
//		messageClient:
//		quotaClient   :
//		credential    :
//	}
//}

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
