/**
 * Copyright 2019, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package main

import (
	"github.com/XiaoMi/talos-sdk-golang/admin"
	"github.com/XiaoMi/talos-sdk-golang/client"
	"github.com/XiaoMi/talos-sdk-golang/thrift/auth"
	"github.com/XiaoMi/talos-sdk-golang/thrift/authorization"
	"github.com/XiaoMi/talos-sdk-golang/thrift/message"
	"github.com/XiaoMi/talos-sdk-golang/thrift/topic"
	"github.com/XiaoMi/talos-sdk-golang/utils"

	"github.com/XiaoMi/talos-sdk-golang/utils/thrift"
	log "github.com/sirupsen/logrus"
)

const (
	// authenticate for team
	secretKeyId     = ""
	secretKey       = ""
	serviceEndpoint = ""

	// another teamId used to be grant permission
	anotherTeamId = ""

	// attention that the topic name to be created is 'orgId/topicName'
	topicName      = ""
	orgId          = ""
	cloudTopicName = orgId + "/" + topicName

	partitionNumber = 8
)

type TalosAdminDemo struct {
	talosAdmin *admin.TalosAdmin
}

func NewTalosAdminDemo() *TalosAdminDemo {
	props := utils.NewProperties()
	props.SetProperty("galaxy.talos.service.endpoint", serviceEndpoint)
	clientConfig := client.NewTalosClientConfigByProperties(props)

	userType := auth.UserType_DEV_XIAOMI
	credential := &auth.Credential{
		TypeA1:      &userType,
		SecretKeyId: thrift.StringPtr(secretKeyId),
		SecretKey:   thrift.StringPtr(secretKey),
	}

	talosAdmin := admin.NewTalosAdminByConfig(clientConfig, credential)
	return &TalosAdminDemo{talosAdmin: talosAdmin}
}

// create topic specified partitionNumber and topicName
func (demo TalosAdminDemo) createTopic() *topic.CreateTopicResponse {
	topicAttribute := topic.NewTopicAttribute()
	topicAttribute.PartitionNumber = thrift.Int32Ptr(partitionNumber)

	// Note: authorization must use 'teamId' and only identifier setting is required
	grant := authorization.NewGrantee()
	grant.Identifier = thrift.StringPtr(anotherTeamId)
	aclMap := make(map[*authorization.Grantee]topic.Permission)
	aclMap[grant] = topic.Permission_TOPIC_READ_AND_MESSAGE_FULL_CONTROL
	// Note: using cloudTopicName instead of original topic name
	request := &topic.CreateTopicRequest{
		TopicName:      cloudTopicName,
		TopicAttribute: topicAttribute,
		AclMap:         aclMap,
	}

	response, err := demo.talosAdmin.CreateTopic(request)
	if err != nil {
		log.Error("createTopic failed: %s", topicName)
		return nil
	}
	log.Info("createTopic sucess: %s", topicName)
	return response
}

// get topicTalosResourceName by topicName
func (demo TalosAdminDemo) getTopicTalosResourceName() (
	*topic.TopicTalosResourceName, error) {
	topic, err := demo.talosAdmin.DescribeTopic(&topic.
		DescribeTopicRequest{TopicName: topicName})
	if err != nil {
		log.Error("getTopicTalosResourceName failed: %s", topicName)
		return nil, err
	}
	resourceName := topic.GetTopicInfo().GetTopicTalosResourceName()
	log.Info("Topic resource is: %s", resourceName.String())
	return resourceName, err
}

func (demo TalosAdminDemo) deleteTopic(resourceName *topic.TopicTalosResourceName) error {
	request := &topic.DeleteTopicRequest{TopicTalosResourceName: resourceName}
	err := demo.talosAdmin.DeleteTopic(request)
	if err != nil {
		log.Error("Topic failed to delete: %s", resourceName)
		return err
	}
	log.Info("Topic success to delete: %s", resourceName)
	return err
}

func (demo TalosAdminDemo) getTopicOffset(resourceName *topic.TopicTalosResourceName) error {
	request := &message.GetTopicOffsetRequest{TopicTalosResourceName: resourceName}
	offsetInfoList, err := demo.talosAdmin.GetTopicOffset(request)
	if err != nil {
		log.Error("Topic failed to getTopicOffset: %s", resourceName)
		return err
	}
	log.Info("Topic success to getTopicOffset: %s", offsetInfoList)
	return err
}

func (demo TalosAdminDemo) listTopics() ([]*topic.TopicInfo, error) {
	topicInfoList, err := demo.talosAdmin.ListTopics()
	if err != nil {
		log.Error("Topic failed to listTopics: %s")
		return nil, err
	}
	log.Info("Topic success to listTopics: %s", topicInfoList)
	for _, topic := range topicInfoList {
		log.Info("Topic info: %s", topic)
	}
	return topicInfoList, err
}

func (demo TalosAdminDemo) listTopicsInfo() ([]*topic.Topic, error) {
	topicInfoList, err := demo.talosAdmin.ListTopicsInfo()
	if err != nil {
		log.Error("Topic failed to listTopics: %s")
		return nil, err
	}
	log.Info("Topic success to listTopics: %s", topicInfoList)
	for _, topicInfo := range topicInfoList {
		log.Info("Topic info: %s", topicInfo)
	}
	return topicInfoList, err
}

func main() {
	utils.InitLog()
	talosAdminDemo := NewTalosAdminDemo()
	talosAdminDemo.createTopic()
	//resourceName, _ :=
	talosAdminDemo.getTopicTalosResourceName()
	//talosAdminDemo.deleteTopic(resourceName)
}
