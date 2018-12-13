/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package main

import (
	"flag"
	"time"
	"fmt"
	"sync/atomic"

	"../../talos/producer"
	"github.com/XiaoMi/talos-sdk-golang/talos/admin"
	"github.com/XiaoMi/talos-sdk-golang/talos/client"
	"github.com/XiaoMi/talos-sdk-golang/talos/thrift/auth"
	"github.com/XiaoMi/talos-sdk-golang/talos/thrift/common"
	"github.com/XiaoMi/talos-sdk-golang/talos/thrift/message"
	"github.com/XiaoMi/talos-sdk-golang/talos/thrift/topic"
	"github.com/XiaoMi/talos-sdk-golang/talos/utils"
	log "github.com/alecthomas/log4go"
)

type MyMessageCallback struct {
}

func (c *MyMessageCallback) OnSuccess(userMessageResult *producer.UserMessageResult) {
	atomic.StoreInt64(successPutNumber, atomic.LoadInt64(successPutNumber)+int64(len(userMessageResult.GetMessageList())))
	count := atomic.LoadInt64(successPutNumber)

	for _, msg := range userMessageResult.GetMessageList() {
		log.Info("success to put message: %s", string(msg.GetMessage()))
	}
	log.Info("success to put message: %d so far.", count)
}

func (c *MyMessageCallback) OnError(userMessageResult *producer.UserMessageResult) {
	for _, msg := range userMessageResult.GetMessageList() {
		log.Info("failed to put message: %d , will retry to put it.", msg)
	}
	err := talosProducer.AddUserMessage(userMessageResult.GetMessageList())
	if err != nil {
		log.Error("put message retry failed: %s", err.Error())
	}
}

var successPutNumber *int64
var talosProducer *producer.TalosProducer

func main() {
	log.AddFilter("stdout", log.INFO, log.NewConsoleLogWriter())
	log.AddFilter("file", log.INFO, log.NewFileLogWriter("talos_producer.log", false))
	defer log.Close()
	successPutNumber = new(int64)
	atomic.StoreInt64(successPutNumber, 0)
	// init client config by put $your_propertyFile in your classpath
	// with the content of:
	/*
	   galaxy.talos.service.endpoint=$talosServiceURI
	*/
	var propertyFilename string
	flag.StringVar(&propertyFilename, "conf", "talosProducer.conf", "conf: talosConsumer.conf'")
	flag.Parse()
	props := utils.LoadProperties(propertyFilename)

  topicName := props.Get("galaxy.talos.topic.name")
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
	producerConfig := producer.NewTalosProducerConfigByProperties(props)
	clientFactory := client.NewTalosClientFactory(clientConfig, credential, socketTimeout)

	talosAdmin := admin.NewTalosAdmin(&clientFactory)
	topic, err := talosAdmin.DescribeTopic(&topic.DescribeTopicRequest{topicName})
	if err != nil {
		log.Error(err)
		return
	}
	topicTalosResourceName := topic.GetTopicInfo().GetTopicTalosResourceName()

	talosProducer := producer.NewTalosProducer(producerConfig,
		credential, topicTalosResourceName, new(client.SimpleTopicAbnormalCallback),
		new(MyMessageCallback))

	toPutMsgNumber := 7
	messageList := make([]*message.Message, 0)
	for i := 0; i < toPutMsgNumber; i++ {
		messageStr := fmt.Sprintf("This message is a text string. messageId: %d", i)
		msg := &message.Message{Message: []byte(messageStr)}
		messageList = append(messageList, msg)
	}

	if err := talosProducer.AddUserMessage(messageList); err != nil {
		log.Error(err)
		return
	}
	time.Sleep(5000 * time.Millisecond)

	//talosProducer.Shutdown()
	//log.Info("Talos producer is shutdown...")
}
