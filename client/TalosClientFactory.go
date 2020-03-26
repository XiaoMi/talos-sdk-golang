/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package client

import (
	"fmt"
	"net"
	"net/http"
	"runtime"
	"time"

	"github.com/XiaoMi/talos-sdk-golang/thrift/auth"
	"github.com/XiaoMi/talos-sdk-golang/thrift/common"
	"github.com/XiaoMi/talos-sdk-golang/thrift/consumer"
	"github.com/XiaoMi/talos-sdk-golang/thrift/message"
	"github.com/XiaoMi/talos-sdk-golang/thrift/quota"
	"github.com/XiaoMi/talos-sdk-golang/thrift/topic"
	"github.com/XiaoMi/talos-sdk-golang/utils"

	"github.com/XiaoMi/talos-sdk-golang/thrift/thrift"
)

//interface for mock
type TalosClientFactoryInterface interface {
	NewTopicClient(url string) topic.TopicService
	NewMessageClient(url string) message.MessageService
	NewConsumerClient(url string) consumer.ConsumerService
	NewQuotaClient(url string) quota.QuotaService
	NewTopicClientDefault() topic.TopicService
	NewMessageClientDefault() message.MessageService
	NewConsumerClientDefault() consumer.ConsumerService
	NewQuotaClientDefault() quota.QuotaService
}

type TalosClientFactory struct {
	talosClientConfig *TalosClientConfig
	credential        *auth.Credential
	httpClient        *http.Client
	agent             string
}

func NewTalosClientFactory(ClientConfig *TalosClientConfig,
	credential *auth.Credential) *TalosClientFactory {
	version := common.NewVersion()
	versionStr := fmt.Sprintf("%d.%d.%s", version.Major,
		version.Minor, version.Details)
	agent := fmt.Sprintf("Go-SDK/%s Go/%s-%s-%s", versionStr,
		runtime.GOOS, runtime.GOARCH, runtime.Version())
	httpClient := &http.Client{
		Transport: &http.Transport{
			Dial: func(network, addr string) (net.Conn, error) {
				return net.DialTimeout(network, addr,
					time.Duration(ClientConfig.ClientConnTimeout())*time.Millisecond)
			},
		},
	}
	return &TalosClientFactory{
		talosClientConfig: ClientConfig,
		credential:        credential,
		httpClient:        httpClient,
		agent:             agent,
	}
}

func (cf *TalosClientFactory) NewTopicClientDefault() topic.TopicService {
	cf.checkCredential()
	return cf.NewTopicClient(cf.talosClientConfig.ServiceEndpoint() +
		utils.TALOS_TOPIC_SERVICE_PATH)
}

func (cf *TalosClientFactory) NewTopicClient(url string) topic.TopicService {
	transportFactory := NewTalosHttpClientTransportFactory(url,
		cf.credential, cf.httpClient, cf.agent)
	return &TopicClientProxy{factory: transportFactory, clockOffset: 0}
}

func (cf *TalosClientFactory) NewMessageClientDefault() message.MessageService {
	cf.checkCredential()
	return cf.NewMessageClient(cf.talosClientConfig.ServiceEndpoint() +
		utils.TALOS_MESSAGE_SERVICE_PATH)
}

func (cf *TalosClientFactory) NewMessageClient(url string) message.MessageService {
	transportFactory := NewTalosHttpClientTransportFactory(url,
		cf.credential, cf.httpClient, cf.agent)
	return &MessageClientProxy{factory: transportFactory, clockOffset: 0}
}

func (cf *TalosClientFactory) NewQuotaClientDefault() quota.QuotaService {
	cf.checkCredential()
	return cf.NewQuotaClient(cf.talosClientConfig.ServiceEndpoint() +
		utils.TALOS_QUOTA_SERVICE_PATH)
}

func (cf *TalosClientFactory) NewQuotaClient(url string) quota.QuotaService {
	transportFactory := NewTalosHttpClientTransportFactory(url,
		cf.credential, cf.httpClient, cf.agent)
	return &QuotaClientProxy{factory: transportFactory, clockOffset: 0}
}

func (cf *TalosClientFactory) NewConsumerClientDefault() consumer.ConsumerService {
	cf.checkCredential()
	return cf.NewConsumerClient(cf.talosClientConfig.ServiceEndpoint() +
		utils.TALOS_CONSUMER_SERVICE_PATH)
}

func (cf *TalosClientFactory) NewConsumerClient(url string) consumer.ConsumerService {
	transportFactory := NewTalosHttpClientTransportFactory(url,
		cf.credential, cf.httpClient, cf.agent)
	return &ConsumerClientProxy{factory: transportFactory, clockOffset: 0}
}

func (cf *TalosClientFactory) SetCredential(credential *auth.Credential) *TalosClientFactory {
	cf.credential = credential
	return cf
}

func (cf *TalosClientFactory) GetCredential() *auth.Credential {
	return cf.credential
}

func (cf *TalosClientFactory) SetHttpClient(httpClient *http.Client) *TalosClientFactory {
	cf.httpClient = httpClient
	return cf
}

func (cf *TalosClientFactory) GetHttpClient() *http.Client {
	return cf.httpClient
}

/**
 * Topic client proxy
 */
type TopicClientProxy struct {
	factory     *TalosHttpClientTransportFactory
	clockOffset int64
}

func (p *TopicClientProxy) GetServiceVersion() (r *common.Version, err error) {
	transport := p.factory.GetTransportWithClockOffset(nil,
		p.clockOffset, "type=getServerVersion")
	defer transport.Close()
	client := topic.NewTopicServiceClientFactory(transport,
		thrift.NewTCompactProtocolFactory())
	return client.GetServiceVersion()
}

func (p *TopicClientProxy) ValidClientVersion(clientVersion *common.Version) (err error) {
	transport := p.factory.GetTransportWithClockOffset(nil,
		p.clockOffset, "type=validClientVersion")
	defer transport.Close()
	client := topic.NewTopicServiceClientFactory(transport,
		thrift.NewTCompactProtocolFactory())
	return client.ValidClientVersion(clientVersion)
}

func (p *TopicClientProxy) CreateTopic(request *topic.
	CreateTopicRequest) (r *topic.CreateTopicResponse, err error) {
	transport := p.factory.GetTransportWithClockOffset(nil,
		p.clockOffset, "type=createTopic")
	defer transport.Close()
	client := topic.NewTopicServiceClientFactory(transport,
		thrift.NewTCompactProtocolFactory())
	return client.CreateTopic(request)
}

func (p *TopicClientProxy) DeleteTopic(request *topic.
	DeleteTopicRequest) (err error) {
	transport := p.factory.GetTransportWithClockOffset(nil,
		p.clockOffset, "type=deleteTopic")
	defer transport.Close()
	client := topic.NewTopicServiceClientFactory(transport,
		thrift.NewTCompactProtocolFactory())
	return client.DeleteTopic(request)
}

func (p *TopicClientProxy) ChangeTopicAttribute(request *topic.
	ChangeTopicAttributeRequest) (err error) {
	transport := p.factory.GetTransportWithClockOffset(nil,
		p.clockOffset, "type=changeTopicAttribute")
	defer transport.Close()
	client := topic.NewTopicServiceClientFactory(transport,
		thrift.NewTCompactProtocolFactory())
	return client.ChangeTopicAttribute(request)
}

func (p *TopicClientProxy) DescribeTopic(request *topic.
	DescribeTopicRequest) (r *topic.DescribeTopicResponse, err error) {
	transport := p.factory.GetTransportWithClockOffset(nil,
		p.clockOffset, "type=describeTopic")
	defer transport.Close()
	client := topic.NewTopicServiceClientFactory(transport,
		thrift.NewTCompactProtocolFactory())
	return client.DescribeTopic(request)
}

func (p *TopicClientProxy) GetDescribeInfo(request *topic.GetDescribeInfoRequest) (
	r *topic.GetDescribeInfoResponse, err error) {
	transport := p.factory.GetTransportWithClockOffset(nil,
		p.clockOffset, "type=getDescribeInfo")
	defer transport.Close()
	client := topic.NewTopicServiceClientFactory(transport,
		thrift.NewTCompactProtocolFactory())
	return client.GetDescribeInfo(request)
}

func (p *TopicClientProxy) ListTopicsInfo() (r *topic.ListTopicsInfoResponse, err error) {
	transport := p.factory.GetTransportWithClockOffset(nil,
		p.clockOffset, "type=listTopicsInfo")
	defer transport.Close()
	client := topic.NewTopicServiceClientFactory(transport,
		thrift.NewTCompactProtocolFactory())
	return client.ListTopicsInfo()
}

func (p *TopicClientProxy) ListTopics() (r *topic.ListTopicsResponse, err error) {
	transport := p.factory.GetTransportWithClockOffset(nil,
		p.clockOffset, "type=listTopics")
	defer transport.Close()
	client := topic.NewTopicServiceClientFactory(transport,
		thrift.NewTCompactProtocolFactory())
	return client.ListTopics()
}

func (p *TopicClientProxy) GetBindTopics() (r *topic.ListTopicsResponse, err error) {
	transport := p.factory.GetTransportWithClockOffset(nil,
		p.clockOffset, "type=getBindTopics")
	defer transport.Close()
	client := topic.NewTopicServiceClientFactory(transport,
		thrift.NewTCompactProtocolFactory())
	return client.GetBindTopics()
}

func (p *TopicClientProxy) SetTopicQuota(request *topic.
	SetTopicQuotaRequest) (err error) {
	transport := p.factory.GetTransportWithClockOffset(nil,
		p.clockOffset, "type=setTopicQuota")
	defer transport.Close()
	client := topic.NewTopicServiceClientFactory(transport,
		thrift.NewTCompactProtocolFactory())
	return client.SetTopicQuota(request)
}

func (p *TopicClientProxy) QueryTopicQuota(request *topic.
	QueryTopicQuotaRequest) (r *topic.QueryTopicQuotaResponse, err error) {
	transport := p.factory.GetTransportWithClockOffset(nil,
		p.clockOffset, "type=queryTopicQuota")
	defer transport.Close()
	client := topic.NewTopicServiceClientFactory(transport,
		thrift.NewTCompactProtocolFactory())
	return client.QueryTopicQuota(request)
}

func (p *TopicClientProxy) DeleteTopicQuota(request *topic.
	DeleteTopicQuotaRequest) (err error) {
	transport := p.factory.GetTransportWithClockOffset(nil,
		p.clockOffset, "type=deleteTopicQuota")
	defer transport.Close()
	client := topic.NewTopicServiceClientFactory(transport,
		thrift.NewTCompactProtocolFactory())
	return client.DeleteTopicQuota(request)
}

func (p *TopicClientProxy) SetPermission(request *topic.
	SetPermissionRequest) (err error) {
	transport := p.factory.GetTransportWithClockOffset(nil,
		p.clockOffset, "type=setPermission")
	defer transport.Close()
	client := topic.NewTopicServiceClientFactory(transport,
		thrift.NewTCompactProtocolFactory())
	return client.SetPermission(request)
}

func (p *TopicClientProxy) RevokePermission(request *topic.
	RevokePermissionRequest) (err error) {
	transport := p.factory.GetTransportWithClockOffset(nil,
		p.clockOffset, "type=revokePermission")
	defer transport.Close()
	client := topic.NewTopicServiceClientFactory(transport,
		thrift.NewTCompactProtocolFactory())
	return client.RevokePermission(request)
}

func (p *TopicClientProxy) ListPermission(request *topic.
	ListPermissionRequest) (r *topic.ListPermissionResponse, err error) {
	transport := p.factory.GetTransportWithClockOffset(nil,
		p.clockOffset, "type=listPermission")
	defer transport.Close()
	client := topic.NewTopicServiceClientFactory(transport,
		thrift.NewTCompactProtocolFactory())
	return client.ListPermission(request)
}

func (p *TopicClientProxy) GetPermission(request *topic.
	GetPermissionRequest) (r *topic.GetPermissionResponse, err error) {
	transport := p.factory.GetTransportWithClockOffset(nil,
		p.clockOffset, "type=getPermission")
	defer transport.Close()
	client := topic.NewTopicServiceClientFactory(transport,
		thrift.NewTCompactProtocolFactory())
	return client.GetPermission(request)
}

func (p *TopicClientProxy) AddSubResourceName(request *topic.
	AddSubResourceNameRequest) (err error) {
	transport := p.factory.GetTransportWithClockOffset(nil,
		p.clockOffset, "type=addSubResourceName")
	defer transport.Close()
	client := topic.NewTopicServiceClientFactory(transport,
		thrift.NewTCompactProtocolFactory())
	return client.AddSubResourceName(request)
}

/**
 * message client proxy
 */
type MessageClientProxy struct {
	factory     *TalosHttpClientTransportFactory
	clockOffset int64
}

func (p *MessageClientProxy) GetServiceVersion() (r *common.Version, err error) {
	transport := p.factory.GetTransportWithClockOffset(nil,
		p.clockOffset, "type=getServerVersion")
	defer transport.Close()
	client := message.NewMessageServiceClientFactory(transport,
		thrift.NewTCompactProtocolFactory())
	return client.GetServiceVersion()
}

func (p *MessageClientProxy) ValidClientVersion(clientVersion *common.Version) (err error) {
	transport := p.factory.GetTransportWithClockOffset(nil,
		p.clockOffset, "type=validClientVersion")
	defer transport.Close()
	client := message.NewMessageServiceClientFactory(transport,
		thrift.NewTCompactProtocolFactory())
	return client.ValidClientVersion(clientVersion)
}

func (p *MessageClientProxy) PutMessage(request *message.
	PutMessageRequest) (r *message.PutMessageResponse, err error) {
	transport := p.factory.GetTransportWithClockOffset(nil,
		p.clockOffset, "type=putMessage")
	defer transport.Close()
	client := message.NewMessageServiceClientFactory(transport,
		thrift.NewTCompactProtocolFactory())
	return client.PutMessage(request)
}

func (p *MessageClientProxy) GetMessage(request *message.
	GetMessageRequest) (r *message.GetMessageResponse, err error) {
	transport := p.factory.GetTransportWithClockOffset(nil,
		p.clockOffset, "type=getMessage")
	defer transport.Close()
	client := message.NewMessageServiceClientFactory(transport,
		thrift.NewTCompactProtocolFactory())
	return client.GetMessage(request)
}

func (p *MessageClientProxy) GetTopicOffset(request *message.
	GetTopicOffsetRequest) (r *message.GetTopicOffsetResponse, err error) {
	transport := p.factory.GetTransportWithClockOffset(nil,
		p.clockOffset, "type=getTopicOffset")
	defer transport.Close()
	client := message.NewMessageServiceClientFactory(transport,
		thrift.NewTCompactProtocolFactory())
	return client.GetTopicOffset(request)
}

func (p *MessageClientProxy) GetPartitionOffset(request *message.
	GetPartitionOffsetRequest) (r *message.GetPartitionOffsetResponse, err error) {
	transport := p.factory.GetTransportWithClockOffset(nil,
		p.clockOffset, "type=getPartitionOffset")
	defer transport.Close()
	client := message.NewMessageServiceClientFactory(transport,
		thrift.NewTCompactProtocolFactory())
	return client.GetPartitionOffset(request)
}

func (p *MessageClientProxy) GetPartitionsOffset(request *message.
	GetPartitionsOffsetRequest) (r *message.GetPartitionsOffsetResponse, err error) {
	transport := p.factory.GetTransportWithClockOffset(nil,
		p.clockOffset, "type=getPartitionsOffset")
	defer transport.Close()
	client := message.NewMessageServiceClientFactory(transport,
		thrift.NewTCompactProtocolFactory())
	return client.GetPartitionsOffset(request)
}

func (p *MessageClientProxy) GetScheduleInfo(request *message.
	GetScheduleInfoRequest) (r *message.GetScheduleInfoResponse, err error) {
	transport := p.factory.GetTransportWithClockOffset(nil,
		p.clockOffset, "type=getScheduleInfo")
	defer transport.Close()
	client := message.NewMessageServiceClientFactory(transport,
		thrift.NewTCompactProtocolFactory())
	return client.GetScheduleInfo(request)
}

/**
 * consumer client proxy
 */
type ConsumerClientProxy struct {
	factory     *TalosHttpClientTransportFactory
	clockOffset int64
}

func (p *ConsumerClientProxy) GetServiceVersion() (r *common.Version, err error) {
	transport := p.factory.GetTransportWithClockOffset(nil,
		p.clockOffset, "type=getServerVersion")
	defer transport.Close()
	client := consumer.NewConsumerServiceClientFactory(transport,
		thrift.NewTCompactProtocolFactory())
	return client.GetServiceVersion()
}

func (p *ConsumerClientProxy) ValidClientVersion(clientVersion *common.Version) (err error) {
	transport := p.factory.GetTransportWithClockOffset(nil,
		p.clockOffset, "type=validClientVersion")
	defer transport.Close()
	client := consumer.NewConsumerServiceClientFactory(transport,
		thrift.NewTCompactProtocolFactory())
	return client.ValidClientVersion(clientVersion)
}

func (p *ConsumerClientProxy) LockPartition(request *consumer.
	LockPartitionRequest) (r *consumer.LockPartitionResponse, err error) {
	transport := p.factory.GetTransportWithClockOffset(nil,
		p.clockOffset, "type=lockPartition")
	defer transport.Close()
	client := consumer.NewConsumerServiceClientFactory(transport,
		thrift.NewTCompactProtocolFactory())
	return client.LockPartition(request)
}

func (p *ConsumerClientProxy) LockWorker(request *consumer.
	LockWorkerRequest) (r *consumer.LockWorkerResponse, err error) {
	transport := p.factory.GetTransportWithClockOffset(nil,
		p.clockOffset, "type=lockWorker")
	defer transport.Close()
	client := consumer.NewConsumerServiceClientFactory(transport,
		thrift.NewTCompactProtocolFactory())
	return client.LockWorker(request)
}

func (p *ConsumerClientProxy) UnlockPartition(request *consumer.
	UnlockPartitionRequest) (err error) {
	transport := p.factory.GetTransportWithClockOffset(nil,
		p.clockOffset, "type=unlockPartition")
	defer transport.Close()
	client := consumer.NewConsumerServiceClientFactory(transport,
		thrift.NewTCompactProtocolFactory())
	return client.UnlockPartition(request)
}

func (p *ConsumerClientProxy) Renew(request *consumer.
	RenewRequest) (r *consumer.RenewResponse, err error) {
	transport := p.factory.GetTransportWithClockOffset(nil,
		p.clockOffset, "type=renew")
	defer transport.Close()
	client := consumer.NewConsumerServiceClientFactory(transport,
		thrift.NewTCompactProtocolFactory())
	return client.Renew(request)
}

func (p *ConsumerClientProxy) UpdateOffset(request *consumer.
	UpdateOffsetRequest) (r *consumer.UpdateOffsetResponse, err error) {
	transport := p.factory.GetTransportWithClockOffset(nil,
		p.clockOffset, "type=updateOffset")
	defer transport.Close()
	client := consumer.NewConsumerServiceClientFactory(transport,
		thrift.NewTCompactProtocolFactory())
	return client.UpdateOffset(request)
}

func (p *ConsumerClientProxy) QueryOffset(request *consumer.
	QueryOffsetRequest) (r *consumer.QueryOffsetResponse, err error) {
	transport := p.factory.GetTransportWithClockOffset(nil,
		p.clockOffset, "type=queryOffset")
	defer transport.Close()
	client := consumer.NewConsumerServiceClientFactory(transport,
		thrift.NewTCompactProtocolFactory())
	return client.QueryOffset(request)
}

func (p *ConsumerClientProxy) QueryOrgOffset(request *consumer.QueryOrgOffsetRequest) (
	r *consumer.QueryOrgOffsetResponse, err error) {
	transport := p.factory.GetTransportWithClockOffset(nil,
		p.clockOffset, "type=queryOrgOffset")
	defer transport.Close()
	client := consumer.NewConsumerServiceClientFactory(transport, thrift.NewTCompactProtocolFactory())
	return client.QueryOrgOffset(request)
}

func (p *ConsumerClientProxy) QueryWorker(request *consumer.
	QueryWorkerRequest) (r *consumer.QueryWorkerResponse, err error) {
	transport := p.factory.GetTransportWithClockOffset(nil,
		p.clockOffset, "type=queryWorker")
	defer transport.Close()
	client := consumer.NewConsumerServiceClientFactory(transport,
		thrift.NewTCompactProtocolFactory())
	return client.QueryWorker(request)
}

func (p *ConsumerClientProxy) GetWorkerId(request *consumer.GetWorkerIdRequest) (
	r *consumer.GetWorkerIdResponse, err error) {
	transport := p.factory.GetTransportWithClockOffset(nil,
		p.clockOffset, "type=getWorkerId")
	defer transport.Close()
	client := consumer.NewConsumerServiceClientFactory(transport,
		thrift.NewTCompactProtocolFactory())
	return client.GetWorkerId(request)
}

/**
 * quota client proxy
 */
type QuotaClientProxy struct {
	factory     *TalosHttpClientTransportFactory
	clockOffset int64
}

func (p *QuotaClientProxy) GetServiceVersion() (r *common.Version, err error) {
	transport := p.factory.GetTransportWithClockOffset(nil,
		p.clockOffset, "type=getServerVersion")
	defer transport.Close()
	client := quota.NewQuotaServiceClientFactory(transport,
		thrift.NewTCompactProtocolFactory())
	return client.GetServiceVersion()
}

func (p *QuotaClientProxy) ValidClientVersion(clientVersion *common.Version) (err error) {
	transport := p.factory.GetTransportWithClockOffset(nil,
		p.clockOffset, "type=validClientVersion")
	defer transport.Close()
	client := quota.NewQuotaServiceClientFactory(transport,
		thrift.NewTCompactProtocolFactory())
	return client.ValidClientVersion(clientVersion)
}

func (p *QuotaClientProxy) SetUserQuota(request *quota.SetUserQuotaRequest) (err error) {
	transport := p.factory.GetTransportWithClockOffset(nil,
		p.clockOffset, "type=setUserQuota")
	defer transport.Close()
	client := quota.NewQuotaServiceClientFactory(transport,
		thrift.NewTCompactProtocolFactory())
	return client.SetUserQuota(request)
}

func (p *QuotaClientProxy) ListUserQuota() (r *quota.ListUserQuotaResponse, err error) {
	transport := p.factory.GetTransportWithClockOffset(nil,
		p.clockOffset, "type=listUserQuota")
	defer transport.Close()
	client := quota.NewQuotaServiceClientFactory(transport,
		thrift.NewTCompactProtocolFactory())
	return client.ListUserQuota()
}

func (p *QuotaClientProxy) QueryUserQuota() (r *quota.QueryUserQuotaResponse, err error) {
	transport := p.factory.GetTransportWithClockOffset(nil,
		p.clockOffset, "type=queryUserQuota")
	defer transport.Close()
	client := quota.NewQuotaServiceClientFactory(transport,
		thrift.NewTCompactProtocolFactory())
	return client.QueryUserQuota()
}

func (p *QuotaClientProxy) DeleteUserQuota(request *quota.DeleteUserQuotaRequest) (err error) {
	transport := p.factory.GetTransportWithClockOffset(nil,
		p.clockOffset, "type=deleteUserQuota")
	defer transport.Close()
	client := quota.NewQuotaServiceClientFactory(transport,
		thrift.NewTCompactProtocolFactory())
	return client.DeleteUserQuota(request)
}

func (p *QuotaClientProxy) ApplyQuota(request *quota.ApplyQuotaRequest) (err error) {
	transport := p.factory.GetTransportWithClockOffset(nil,
		p.clockOffset, "type=applyQuota")
	defer transport.Close()
	client := quota.NewQuotaServiceClientFactory(transport,
		thrift.NewTCompactProtocolFactory())
	return client.ApplyQuota(request)
}

func (p *QuotaClientProxy) AutoApplyQuota(request *quota.AutoApplyQuotaRequest) (err error) {
	transport := p.factory.GetTransportWithClockOffset(nil,
		p.clockOffset, "type=autoApplyQuota")
	defer transport.Close()
	client := quota.NewQuotaServiceClientFactory(transport,
		thrift.NewTCompactProtocolFactory())
	return client.AutoApplyQuota(request)
}

func (p *QuotaClientProxy) ListQuota() (r *quota.ListQuotaResponse, err error) {
	transport := p.factory.GetTransportWithClockOffset(nil,
		p.clockOffset, "type=listQuota")
	defer transport.Close()
	client := quota.NewQuotaServiceClientFactory(transport,
		thrift.NewTCompactProtocolFactory())
	return client.ListQuota()
}

func (p *QuotaClientProxy) ListPendingQuota() (r *quota.ListPendingQuotaResponse, err error) {
	transport := p.factory.GetTransportWithClockOffset(nil,
		p.clockOffset, "type=listPendingQuota")
	defer transport.Close()
	client := quota.NewQuotaServiceClientFactory(transport,
		thrift.NewTCompactProtocolFactory())
	return client.ListPendingQuota()
}

func (p *QuotaClientProxy) ApproveQuota(request *quota.ApproveQuotaRequest) (r *quota.ApproveQuotaResponse, err error) {
	transport := p.factory.GetTransportWithClockOffset(nil,
		p.clockOffset, "type=approveQuota")
	defer transport.Close()
	client := quota.NewQuotaServiceClientFactory(transport,
		thrift.NewTCompactProtocolFactory())
	return client.ApproveQuota(request)
}

func (p *QuotaClientProxy) RevokeQuota(request *quota.RevokeQuotaRequest) (r *quota.RevokeQuotaResponse, err error) {
	transport := p.factory.GetTransportWithClockOffset(nil,
		p.clockOffset, "type=revokeQuota")
	defer transport.Close()
	client := quota.NewQuotaServiceClientFactory(transport,
		thrift.NewTCompactProtocolFactory())
	return client.RevokeQuota(request)
}

func (cf *TalosClientFactory) checkCredential() error {
	if cf.credential == nil {
		return fmt.Errorf("Credential is not set! ")
	}
	return nil
}
