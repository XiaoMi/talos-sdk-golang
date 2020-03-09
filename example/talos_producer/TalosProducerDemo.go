/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package main

import (
	"flag"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/XiaoMi/talos-sdk-golang/client"
	"github.com/XiaoMi/talos-sdk-golang/producer"
	"github.com/XiaoMi/talos-sdk-golang/thrift/message"

	"github.com/XiaoMi/talos-sdk-golang/utils"

	log "github.com/sirupsen/logrus"
)

type MyMessageCallback struct {
}

func (c *MyMessageCallback) OnSuccess(userMessageResult *producer.UserMessageResult) {
	atomic.StoreInt64(successPutNumber, atomic.LoadInt64(successPutNumber)+int64(len(userMessageResult.GetMessageList())))
	count := atomic.LoadInt64(successPutNumber)

	for _, msg := range userMessageResult.GetMessageList() {
		log.Infof("success to put message: %s", string(msg.GetMessage()))
	}
	log.Infof("success to put message: %d so far.", count)
}

func (c *MyMessageCallback) OnError(userMessageResult *producer.UserMessageResult) {
	for _, msg := range userMessageResult.GetMessageList() {
		log.Infof("failed to put message: %d , will retry to put it.", msg)
	}
	err := talosProducer.AddUserMessage(userMessageResult.GetMessageList())
	if err != nil {
		log.Errorf("put message retry failed: %s", err.Error())
	}
}

var successPutNumber *int64
var talosProducer *producer.TalosProducer

func main() {
	utils.InitLog()
	successPutNumber = new(int64)
	atomic.StoreInt64(successPutNumber, 0)
	// init client config by put $your_propertyFile in your classpath
	// with the content of:
	/*
	   galaxy.talos.service.endpoint=$talosServiceURI
	*/
	var propertyFilename string
	flag.StringVar(&propertyFilename, "conf", "talosProducer.conf", "conf: talosProducer.conf'")
	flag.Parse()

	var err error
	talosProducer, err = producer.NewTalosProducerByFilename(propertyFilename,
		client.NewSimpleTopicAbnormalCallback(), new(MyMessageCallback))
	if err != nil {
		log.Errorf("Init talosProducer failed: %s", err.Error())
		return
	}
	toPutMsgNumber := 20
	messageList := make([]*message.Message, 0)
	for i := 0; i < toPutMsgNumber; i++ {
		messageStr := fmt.Sprintf("This message is a text string. messageId: %d", i)
		msg := &message.Message{Message: []byte(messageStr)}
		messageList = append(messageList, msg)
	}

	for {
		if err := talosProducer.AddUserMessage(messageList); err != nil {
			log.Error(err)
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}
