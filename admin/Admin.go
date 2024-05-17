/**
 * Copyright 2019, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package admin

import (
	"github.com/XiaoMi/talos-sdk-golang/thrift/consumer"
	"github.com/XiaoMi/talos-sdk-golang/thrift/message"
	"github.com/XiaoMi/talos-sdk-golang/thrift/quota"
	"github.com/XiaoMi/talos-sdk-golang/thrift/topic"
)

type Admin interface {
	CreateTopic(request *topic.CreateTopicRequest) (*topic.CreateTopicResponse, error)
	DescribeTopic(request *topic.DescribeTopicRequest) (*topic.Topic, error)
	GetDescribeInfo(request *topic.GetDescribeInfoRequest) (*topic.GetDescribeInfoResponse, error)
	DeleteTopic(request *topic.DeleteTopicRequest) error
	ChangeTopicAttribute(request *topic.ChangeTopicAttributeRequest) error
	ListTopics() ([]*topic.TopicInfo, error)
	ListTopicsByOrgId(orgId string) ([]*topic.Topic, error)
	ListTopicsInfo() ([]*topic.Topic, error)
	LookupTopics(request *topic.LookupTopicsRequest) (*topic.LookupTopicsResponse, error)
	GetTopicOffset(request *message.GetTopicOffsetRequest) ([]*message.OffsetInfo, error)
	GetPartitionOffset(request *message.GetPartitionOffsetRequest) (*message.OffsetInfo, error)
	GetScheduleInfo(request *message.GetScheduleInfoRequest) (map[*topic.TopicAndPartition]string, error)
	SetPermission(request *topic.SetPermissionRequest) error
	ListPermission(request *topic.ListPermissionRequest) (map[string]int32, error)
	QueryPermission(request *topic.GetPermissionRequest) (int32, error)
	RevokePermission(request *topic.RevokePermissionRequest) error
	ApplyQuota(request *quota.ApplyQuotaRequest) error
	ApproveQuota(request *quota.ApproveQuotaRequest) (*quota.ApproveQuotaResponse, error)
	RevokeQuota(request *quota.RevokeQuotaRequest) (*quota.RevokeQuotaResponse, error)
	ListQuota() (*quota.ListQuotaResponse, error)
	ListPendingQuota() (*quota.ListPendingQuotaResponse, error)
	SetUserQuota(request *quota.SetUserQuotaRequest) error
	ListAllUserQuota() (map[string]*quota.UserQuota, error)
	DeleteUserQuota(request *quota.DeleteUserQuotaRequest) error
	QueryUserQuota() (*quota.UserQuota, error)
	SetTopicQuota(request *topic.SetTopicQuotaRequest) error
	QueryTopicQuota(request *topic.QueryTopicQuotaRequest) (*topic.QueryTopicQuotaResponse, error)
	DeleteTopicQuota(request *topic.DeleteTopicQuotaRequest) error
	AddSubResourceName(request *topic.AddSubResourceNameRequest) error
	GetOrgOffsetMap(request *consumer.QueryOrgOffsetRequest) (*consumer.QueryOrgOffsetResponse, error)
	GetWorkerId(request *consumer.GetWorkerIdRequest) (string, error)
}
