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

	"github.com/XiaoMi/talos-sdk-golang/talos/client"
	"github.com/XiaoMi/talos-sdk-golang/talos/consumer"
	"github.com/XiaoMi/talos-sdk-golang/talos/thrift/common"
	"github.com/XiaoMi/talos-sdk-golang/talos/thrift/message"
	"github.com/XiaoMi/talos-sdk-golang/talos/thrift/topic"
	"github.com/XiaoMi/talos-sdk-golang/utils"
	"github.com/XiaoMi/talos-sdk-golang/talos/thrift/auth"

	log "github.com/alecthomas/log4go"
)

/**
 * Returns a talos message processor to processing data for a (assigned) partition.
 */
type MyMessageProcessorFactory struct {
}

// using for thread-safe when processing different partition data
func (f *MyMessageProcessorFactory) CreateProcessor() consumer.MessageProcessor {
	processor := new(MyMessageProcessor)
	return processor
}

type MyMessageProcessor struct {
}

func (p *MyMessageProcessor) Init(topicAndPartition *topic.TopicAndPartition,
	startMessageOffset int64) {

}

var successGetNumber = new(int64)

func (p *MyMessageProcessor) Process(messages []*message.MessageAndOffset,
	messageCheckpointer consumer.MessageCheckpointer) {
	for _, msg := range messages {
		log.Info("Message content: %s", string(msg.GetMessage().GetMessage()))
	}
	atomic.AddInt64(successGetNumber, int64(len(messages)))
	log.Info("Consuming total data so far: %d", atomic.LoadInt64(successGetNumber))

	/** if user has set 'galaxy.talos.consumer.checkpoint.auto.commit' to false,
	 * then you can call the 'checkpoint' to commit the list of messages.
	 */
	//messageCheckpointer.CheckpointByFinishedOffset()
}

func (p *MyMessageProcessor) Shutdown(messageCheckpointer consumer.MessageCheckpointer) {

}

func main() {
	log.AddFilter("file", log.INFO, log.NewFileLogWriter("talos_consumer.log", false))
	defer log.Close()

	// init client config by put $your_propertyFile in your classpath
	// with the content of:
	/*
    galaxy.talos.service.endpoint=$talosServiceURI
  */
	var propertyFilename string
	flag.StringVar(&propertyFilename, "conf", "talosConsumer.conf", "conf: talosConsumer.conf'")
	flag.Parse()
	props := utils.LoadProperties(propertyFilename)

	topicName := props.Get("galaxy.talos.topic.name")
	consumerGroup := props.Get("galaxy.talos.group.name")
	clientPrefix := props.Get("galaxy.talos.client.prefix")
	secretKeyId := props.Get("galaxy.talos.access.key")
	secretKey := props.Get("galaxy.talos.access.secret")
	userType := auth.UserType_DEV_XIAOMI
	socketTimeout := time.Duration(common.GALAXY_TALOS_CLIENT_ADMIN_TIMEOUT_MILLI_SECS_DEFAULT * time.Second)
	// credential
	credential := &auth.Credential{
		TypeA1:      &userType,
		SecretKeyId: &secretKeyId,
		SecretKey:   &secretKey,
	}

	clientConfig := client.NewTalosClientConfigByProperties(props)
	consumerConfig, _ := consumer.NewTalosConsumerConfigByProperties(props)
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
	talosConsumer := consumer.NewTalosConsumer(consumerGroup, consumerConfig,
		credential, topicTalosResourceName, new(consumer.TalosMessageReaderFactory),
		new(MyMessageProcessorFactory), clientPrefix,
		new(client.SimpleTopicAbnormalCallback), make(map[int32]consumer.Long))

	for i := 0; i < 3; i++ {
		<-talosConsumer.StopSign
	}
	log.Info("Talos consumer is shutdown...")
}
