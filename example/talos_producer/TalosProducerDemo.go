/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package main

import (
	"flag"
	"time"

	"github.com/XiaoMi/talos-sdk-golang/talos/client"
	"github.com/XiaoMi/talos-sdk-golang/talos/producer"
	"github.com/XiaoMi/talos-sdk-golang/talos/thrift/auth"
	"github.com/XiaoMi/talos-sdk-golang/talos/thrift/common"
	"github.com/XiaoMi/talos-sdk-golang/talos/thrift/message"
	"github.com/XiaoMi/talos-sdk-golang/talos/thrift/topic"
	"github.com/XiaoMi/talos-sdk-golang/talos/utils"
	log "github.com/alecthomas/log4go"
)

type MyMessageCallback struct {
}

func (c *MyMessageCallback) OnSuccess(result *producer.UserMessageResult) {
	for _, msg := range result.GetMessageList() {
		log.Info("success to put message: %s", string(msg.GetMessage()))
	}
	log.Info("put success")
}

func (c *MyMessageCallback) OnError(userMessageResult *producer.UserMessageResult) {

}

func main() {
	log.AddFilter("stdout", log.DEBUG, log.NewConsoleLogWriter())
	log.AddFilter("file", log.DEBUG, log.NewFileLogWriter("talos_producer.log", false))
	defer log.Close()

	// init client config by put $your_propertyFile in your classpath
	// with the content of:
	/*
	   galaxy.talos.service.endpoint=$talosServiceURI
	*/
	var propertyFilename string
	flag.StringVar(&propertyFilename, "conf", "talos-sdk-golang/example/simple_consumer/simpleProducer.conf", "conf: talosConsumer.conf'")
	flag.Parse()
	props := utils.LoadProperties(propertyFilename)

	topicName := ""
	secretKeyId := ""
	secretKey := ""
	userType := auth.UserType_DEV_XIAOMI
	socketTimeout := time.Duration(common.GALAXY_TALOS_CLIENT_ADMIN_TIMEOUT_MILLI_SECS_DEFAULT * time.Second)
	// credential
	credential := &auth.Credential{
		TypeA1:      &userType,
		SecretKeyId: &secretKeyId,
		SecretKey:   &secretKey,
	}

	clientConfig := client.NewTalosClientConfigByProperties(props)
	producerConfig := producer.NewTalosProducerConfigByProperties(props)
	endpoint := clientConfig.ServiceEndpoint()
	clientFactory := client.NewTalosClientFactory(clientConfig, credential, socketTimeout)
	topicClient := clientFactory.NewTopicClient(endpoint + common.TALOS_TOPIC_SERVICE_PATH)

	describeTopicRequest := &topic.DescribeTopicRequest{TopicName: topicName}
	var topicTalosResourceName *topic.TopicTalosResourceName

	// get topic info
	describeTopicResponse, err := topicClient.DescribeTopic(describeTopicRequest)
	if err != nil {
		log.Error("describeTopic error: %s", err.Error())
	} else {
		topic := topic.Topic{
			TopicInfo:      describeTopicResponse.GetTopicInfo(),
			TopicAttribute: describeTopicResponse.GetTopicAttribute(),
			TopicState:     describeTopicResponse.GetTopicState(),
			TopicQuota:     describeTopicResponse.GetTopicQuota(),
			TopicAcl:       describeTopicResponse.GetAclMap(),
		}
		topicTalosResourceName = topic.GetTopicInfo().GetTopicTalosResourceName()
	}
	// init talosConsumer
	talosProducer := producer.NewTalosProducer(producerConfig,
		credential, topicTalosResourceName, new(client.SimpleTopicAbnormalCallback),
		new(MyMessageCallback))
	log.Debug("New TalosProducer success")
	messageStr := "This message is a text string."
	msg := message.NewMessage()
	msg.Message = []byte(messageStr)
	messageList := make([]*message.Message, 0)
	messageList = append(messageList, msg)
	log.Debug("Talos producer start putMessage")
	e := talosProducer.AddUserMessage(messageList)
	if e != nil {
		log.Error(e.Error())
	}
	log.Info("Talos producer is shutdown...")
}
