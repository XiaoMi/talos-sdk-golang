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
	"github.com/XiaoMi/talos-sdk-golang/talos/consumer"
	"github.com/XiaoMi/talos-sdk-golang/talos/thrift/auth"
	"github.com/XiaoMi/talos-sdk-golang/talos/thrift/common"
	"github.com/XiaoMi/talos-sdk-golang/talos/thrift/message"
	"github.com/XiaoMi/talos-sdk-golang/talos/thrift/topic"

	log "github.com/alecthomas/log4go"
)

/**
 * Add your process logic for fetched messages
 */
func Process(messageList []*message.MessageAndOffset) {
	for i := 0; i < len(messageList); i++ {
		log.Info("get message: %s success", messageList[i].GetMessage().GetMessage())
	}
	log.Info("total process %d message", len(messageList))
}

func main() {
	log.AddFilter("file", log.INFO, log.NewFileLogWriter("simple_consumer.log", false))
	defer log.Close()
	// init client config by put $your_propertyFile in current directory
	// with the content of:
	/*
		    galaxy.talos.service.endpoint=$talosServiceURI
				set your conf path, AK:SK, topicName, and partitionId
	*/
	var propertyFilename string
	flag.StringVar(&propertyFilename, "conf", "simpleConsumer.conf", "conf: simpleConsumer.conf'")
	flag.Parse()
	secretKeyId := ""
	secretKey := ""
	topicName := ""
	partitionId := int32(0) // must assign partitionId

	clientConfig := client.NewTalosClientConfigByFilename(propertyFilename)
	consumerConfig, _ := consumer.NewTalosConsumerConfigByFilename(propertyFilename)

	//finishedOffset=-2 -> actualStartOffset=-1 and maxFetchNum=1000 set as default
	finishedOffset := int64(consumer.GALAXY_TALOS_CONSUMER_START_RESET_OFFSET_AS_END)
	maxFetchNum := consumerConfig.GetMaxFetchRecords()

	userType := auth.UserType_DEV_XIAOMI
	credential := &auth.Credential{
		TypeA1:      &userType,
		SecretKeyId: &secretKeyId,
		SecretKey:   &secretKey,
	}

	endpoint := clientConfig.ServiceEndpoint()
	socketTimeout := int64(common.GALAXY_TALOS_CLIENT_ADMIN_TIMEOUT_MILLI_SECS_DEFAULT)

	clientFactory := client.NewTalosClientFactory(clientConfig, credential,
		time.Duration(socketTimeout*int64(time.Second)))
	topicClient := clientFactory.NewTopicClient(endpoint + common.TALOS_TOPIC_SERVICE_PATH)
	messageClient := clientFactory.NewMessageClient(endpoint + common.TALOS_MESSAGE_SERVICE_PATH)

	describeTopicRequest := &topic.DescribeTopicRequest{TopicName: topicName}
	var topicTalosResourceName *topic.TopicTalosResourceName

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
	topicAndPartition := &topic.TopicAndPartition{
		TopicName:              topicName,
		TopicTalosResourceName: topicTalosResourceName,
		PartitionId:            partitionId,
	}

	simpleConsumer := consumer.NewSimpleConsumer(consumerConfig, topicAndPartition, messageClient, "")
	messageList := make([]*message.MessageAndOffset, 0)
	stopSign := make(chan int)

	messageList, err = simpleConsumer.FetchMessage(finishedOffset+1, maxFetchNum)
	if err != nil {
		log.Error(err.Error())
	}

	ticker := time.NewTicker(time.Duration(1 * time.Second))
	defer ticker.Stop()
	go func() {
		for {
			select {
			case <-ticker.C:
				messageList, err = simpleConsumer.FetchMessage(finishedOffset+1, maxFetchNum)
				if err != nil {
					log.Error(err.Error())
				}
				if len(messageList) > 0 {
					finishedOffset = messageList[len(messageList)-1].GetMessageOffset()
					Process(messageList)
				}
			case <-stopSign:
				stopSign <- 1
			}
		}
	}()
	<-stopSign
}
