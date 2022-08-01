/**
 * Copyright 2021, Xiaomi.
 * All rights reserved.
 * Author: fangchengjin@xiaomi.com
 */

package main

import (
	"flag"
	"sync/atomic"
	"time"

	"github.com/XiaoMi/talos-sdk-golang/client"
	"github.com/XiaoMi/talos-sdk-golang/consumer"
	"github.com/XiaoMi/talos-sdk-golang/thrift/message"
	"github.com/XiaoMi/talos-sdk-golang/utils"
	"github.com/sirupsen/logrus"
)

/**
 * Returns a talos message processor to processing data for a (assigned) partition.
 */
type MyMessageProcessorFactory struct {
	log *logrus.Logger
}

func NewMyMessageProcessorFactory(logger *logrus.Logger) *MyMessageProcessorFactory {
	return &MyMessageProcessorFactory{log: logger}
}

// using for thread-safe when processing different partition data
func (f *MyMessageProcessorFactory) CreateProcessor() consumer.MessageProcessor {
	processor := &MyMessageProcessor{log: f.log}
	return processor
}

var successGetNumber = new(int64)

type MyMessageProcessor struct {
	log *logrus.Logger
}

func (p *MyMessageProcessor) Process(messages []*message.MessageAndOffset,
	messageCheckpointer consumer.MessageCheckpointer) {
	for _, msg := range messages {
		p.log.Infof("Message content: %s", string(msg.GetMessage().GetMessage()))
	}
	atomic.AddInt64(successGetNumber, int64(len(messages)))
	p.log.Infof("Consuming total data so far: %d", atomic.LoadInt64(successGetNumber))

	/** if user has set 'galaxy.talos.consumer.checkpoint.auto.commit' to false,
	 * then you can call the 'checkpoint' to commit the list of messages.
	 */
	//messageCheckpointer.CheckpointByFinishedOffset()
}

func main() {
	log := utils.InitLogger()
	// init client config by put $your_propertyFile in your classpath
	// with the content of:
	/*
	   galaxy.talos.service.endpoint=$talosServiceURI
	*/
	var propertyFilename string
	flag.StringVar(&propertyFilename, "conf", "talosConsumer.txt", "conf: talosConsumer.txt'")
	flag.Parse()

	// init talosConsumer
	talosConsumer, err := consumer.NewTalosMultiTopicsConsumerWithLogger(propertyFilename, NewMyMessageProcessorFactory(log),
		client.NewSimpleTopicAbnormalCallback(), log)
	if err != nil {
		log.Errorf("init talosMultiTopicsConsumer failed: %s", err.Error())
		return
	}

	go func() {
		time.Sleep(5 * time.Second)
		// talosConsumer.ShutDown()
	}()

	// block main function
	talosConsumer.WaitGroup.Wait()

}
