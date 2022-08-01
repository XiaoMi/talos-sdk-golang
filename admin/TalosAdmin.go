/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package admin

import (
	"strings"

	"github.com/XiaoMi/talos-sdk-golang/client"
	"github.com/XiaoMi/talos-sdk-golang/thrift/auth"
	"github.com/XiaoMi/talos-sdk-golang/thrift/consumer"
	"github.com/XiaoMi/talos-sdk-golang/thrift/message"
	"github.com/XiaoMi/talos-sdk-golang/thrift/metric"
	"github.com/XiaoMi/talos-sdk-golang/thrift/quota"
	"github.com/XiaoMi/talos-sdk-golang/thrift/topic"
	"github.com/XiaoMi/talos-sdk-golang/utils"
)

type TalosAdmin struct {
	topicClient    topic.TopicService
	messageClient  message.MessageService
	quotaClient    quota.QuotaService
	consumerClient consumer.ConsumerService
	metricClient   metric.MetricService
	credential     *auth.Credential
}

func (a *TalosAdmin) ListTopicsByOrgId(orgId string) ([]*topic.Topic, error) {
	panic("implement me")
}

func NewTalosAdmin(clientFactory *client.TalosClientFactory) *TalosAdmin {
	return &TalosAdmin{
		topicClient:    clientFactory.NewTopicClientDefault(),
		messageClient:  clientFactory.NewMessageClientDefault(),
		quotaClient:    clientFactory.NewQuotaClientDefault(),
		consumerClient: clientFactory.NewConsumerClientDefault(),
		metricClient:   clientFactory.NewMetricClientDefault(),
		credential:     clientFactory.GetCredential(),
	}
}

func NewTalosAdminByConfig(talosClientConfig *client.TalosClientConfig,
	credential *auth.Credential) *TalosAdmin {
	talosClientFactory := client.NewTalosClientFactory(talosClientConfig, credential)
	return NewTalosAdmin(talosClientFactory)
}

func (a *TalosAdmin) CreateTopic(request *topic.CreateTopicRequest) (*topic.CreateTopicResponse, error) {
	if strings.HasPrefix(a.credential.GetSecretKeyId(), utils.TALOS_CLOUD_AK_PREFIX) ||
		strings.HasPrefix(a.credential.GetSecretKeyId(), utils.TALOS_GALAXY_AK_PREFIX) {
		utils.CheckCloudTopicNameValidity(request.GetTopicName())
	} else {
		utils.CheckNameValidity(request.GetTopicName())
	}
	return a.topicClient.CreateTopic(request)
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

func (a *TalosAdmin) DeleteTopic(request *topic.DeleteTopicRequest) error {
	return a.topicClient.DeleteTopic(request)
}

func (a *TalosAdmin) ChangeTopicAttribute(request *topic.ChangeTopicAttributeRequest) error {
	return a.topicClient.ChangeTopicAttribute(request)
}

func (a *TalosAdmin) ListTopics() ([]*topic.TopicInfo, error) {
	listTopicsResponse, err := a.topicClient.ListTopics()
	return listTopicsResponse.GetTopicInfos(), err
}

func (a *TalosAdmin) ListTopicsInfo() ([]*topic.Topic, error) {
	listTopicsInfoResponse, err := a.topicClient.ListTopicsInfo()
	return listTopicsInfoResponse.GetTopicList(), err
}

func (a *TalosAdmin) LookupTopics(request *message.LookupTopicsRequest) (
	*message.LookupTopicsResponse, error) {
	return a.messageClient.LookupTopics(request)
}

func (a *TalosAdmin) GetTopicOffset(request *message.GetTopicOffsetRequest) (
	[]*message.OffsetInfo, error) {
	getTopicOffsetResponse, err := a.messageClient.GetTopicOffset(request)
	return getTopicOffsetResponse.GetOffsetInfoList(), err
}

func (a *TalosAdmin) GetPartitionOffset(request *message.GetPartitionOffsetRequest) (
	*message.OffsetInfo, error) {
	getPartitionOffsetResponse, err := a.messageClient.GetPartitionOffset(request)
	return getPartitionOffsetResponse.GetOffsetInfo(), err
}

func (a *TalosAdmin) DescribeTopicGroup(request *topic.DescribeTopicGroupRequest) (
	*topic.TopicGroup, error) {
	getPartitionOffsetResponse, err := a.topicClient.DescribeTopicGroup(request)
	return getPartitionOffsetResponse.GetTopicGroup(), err
}

func (a *TalosAdmin) GetScheduleInfo(request *message.GetScheduleInfoRequest) (
	map[*topic.TopicAndPartition]string, error) {
	getScheduleInfoResponse, err := a.messageClient.GetScheduleInfo(request)
	return getScheduleInfoResponse.GetScheduleInfo(), err
}

func (a *TalosAdmin) SetPermission(request *topic.SetPermissionRequest) error {
	utils.CheckArgument(request.GetPermission() > 0)
	return a.topicClient.SetPermission(request)
}

func (a *TalosAdmin) ListPermission(request *topic.ListPermissionRequest) (
	map[string]int32, error) {
	listPermissionResponse, err := a.topicClient.ListPermission(request)
	return listPermissionResponse.GetPermissions(), err
}

func (a *TalosAdmin) QueryPermission(request *topic.GetPermissionRequest) (
	int32, error) {
	queryPermissionResponse, err := a.topicClient.GetPermission(request)
	return queryPermissionResponse.GetPermission(), err
}

func (a *TalosAdmin) RevokePermission(request *topic.RevokePermissionRequest) error {
	return a.topicClient.RevokePermission(request)
}

func (a *TalosAdmin) ApplyQuota(request *quota.ApplyQuotaRequest) error {
	return a.quotaClient.ApplyQuota(request)
}

func (a *TalosAdmin) ApproveQuota(request *quota.ApproveQuotaRequest) (
	*quota.ApproveQuotaResponse, error) {
	return a.quotaClient.ApproveQuota(request)
}

func (a *TalosAdmin) RevokeQuota(request *quota.RevokeQuotaRequest) (
	*quota.RevokeQuotaResponse, error) {
	return a.quotaClient.RevokeQuota(request)
}

func (a *TalosAdmin) ListQuota() (*quota.ListQuotaResponse, error) {
	return a.quotaClient.ListQuota()
}

func (a *TalosAdmin) ListPendingQuota() (*quota.ListPendingQuotaResponse, error) {
	return a.quotaClient.ListPendingQuota()
}

func (a *TalosAdmin) SetUserQuota(request *quota.SetUserQuotaRequest) error {
	return a.quotaClient.SetUserQuota(request)
}

func (a *TalosAdmin) ListAllUserQuota() (map[string]*quota.UserQuota, error) {
	listUserQuotaResponse, err := a.quotaClient.ListUserQuota()
	return listUserQuotaResponse.GetUserQuotaList(), err
}

func (a *TalosAdmin) DeleteUserQuota(request *quota.DeleteUserQuotaRequest) error {
	return a.quotaClient.DeleteUserQuota(request)
}

func (a *TalosAdmin) QueryUserQuota() (*quota.UserQuota, error) {
	queryUserQuotaResponse, err := a.quotaClient.QueryUserQuota()
	return queryUserQuotaResponse.GetUserQuota(), err
}

func (a *TalosAdmin) SetTopicQuota(request *topic.SetTopicQuotaRequest) error {
	return a.topicClient.SetTopicQuota(request)
}

func (a *TalosAdmin) QueryTopicQuota(request *topic.QueryTopicQuotaRequest) (
	*topic.QueryTopicQuotaResponse, error) {
	return a.topicClient.QueryTopicQuota(request)
}

func (a *TalosAdmin) DeleteTopicQuota(request *topic.DeleteTopicQuotaRequest) error {
	return a.topicClient.DeleteTopicQuota(request)
}

func (a *TalosAdmin) AddSubResourceName(request *topic.AddSubResourceNameRequest) error {
	utils.CheckAddSubResourceNameRequest(a.credential, request)
	return a.topicClient.AddSubResourceName(request)
}

func (a *TalosAdmin) GetOrgOffsetMap(request *consumer.QueryOrgOffsetRequest) (
	*consumer.QueryOrgOffsetResponse, error) {
	return a.consumerClient.QueryOrgOffset(request)
}

func (a *TalosAdmin) GetWorkerId(request *consumer.GetWorkerIdRequest) (string, error) {
	response, err := a.consumerClient.GetWorkerId(request)
	return response.GetWorkerId(), err
}

func (a *TalosAdmin) GetDescribeInfo(request *topic.GetDescribeInfoRequest) (
	*topic.GetDescribeInfoResponse, error) {
	return a.topicClient.GetDescribeInfo(request)
}
