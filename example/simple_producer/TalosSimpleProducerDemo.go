/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package main

import (
	"flag"
	"sync/atomic"
	"time"

	"github.com/XiaoMi/talos-sdk-golang/talos/admin"
	"github.com/XiaoMi/talos-sdk-golang/talos/client"
	"github.com/XiaoMi/talos-sdk-golang/talos/producer"
	"github.com/XiaoMi/talos-sdk-golang/talos/thrift/auth"
	"github.com/XiaoMi/talos-sdk-golang/talos/thrift/common"
	"github.com/XiaoMi/talos-sdk-golang/talos/thrift/message"
	"github.com/XiaoMi/talos-sdk-golang/talos/thrift/topic"
	"github.com/XiaoMi/talos-sdk-golang/thrift"
	"github.com/alecthomas/log4go"
)

func main() {
	log4go.AddFilter("stdout", log4go.INFO, log4go.NewConsoleLogWriter())
	log4go.AddFilter("file", log4go.INFO, log4go.NewFileLogWriter("simple_consumer.log4go", false))
	defer log4go.Close()

	var propertyFilename string
	flag.StringVar(&propertyFilename, "conf", "simpleProducer.conf", "conf: simpleProducer.conf'")
	flag.Parse()
	secretKeyId := ""
	secretKey := ""
	topicName := ""
	partitionId := int32(0) // must assign partitionId
	successPutNumber := thrift.Int64Ptr(0)

	clientConfig := client.NewTalosClientConfigByFilename(propertyFilename)
	producerConfig := producer.NewTalosProducerConfigByFilename(propertyFilename)

	userType := auth.UserType_DEV_XIAOMI
	credential := &auth.Credential{
		TypeA1:      &userType,
		SecretKeyId: &secretKeyId,
		SecretKey:   &secretKey,
	}

	socketTimeout := common.GALAXY_TALOS_CLIENT_ADMIN_TIMEOUT_MILLI_SECS_DEFAULT * time.Second

	clientFactory := client.NewTalosClientFactory(clientConfig, credential,
		time.Duration(socketTimeout))
	talosAdmin := admin.NewTalosAdmin(&clientFactory)

	describeTopicRequest := &topic.DescribeTopicRequest{TopicName: topicName}
	var topicTalosResourceName *topic.TopicTalosResourceName

	topic1, err := talosAdmin.DescribeTopic(describeTopicRequest)
	if err != nil {
		log4go.Error("describeTopic error: %s", err.Error())
	}
	topicTalosResourceName = topic1.GetTopicInfo().GetTopicTalosResourceName()
	topicAndPartition := &topic.TopicAndPartition{
		TopicName:              topicName,
		TopicTalosResourceName: topicTalosResourceName,
		PartitionId:            partitionId,
	}

	simpleProducer := producer.NewSimpleProducerDefault(producerConfig, topicAndPartition, credential)
	messageStr := "test message: this message is a text string."
	msg := &message.Message{
		Message: []byte(messageStr),
	}
	msgList := make([]*message.Message, 0)
	msgList = append(msgList, msg)

	// a toy demo for putting messages to Talos server continuously
	// by using a infinite loop
	for true {
		err := simpleProducer.PutMessageList(msgList)
		if err != nil {
			log4go.Error("putMessageList error: %s, try again", err.Error())
			return
		}
		time.Sleep(2000 * time.Millisecond)
		log4go.Info("success put message count: %d", atomic.LoadInt64(successPutNumber))
		atomic.StoreInt64(successPutNumber, (*successPutNumber)+1)
	}
}
