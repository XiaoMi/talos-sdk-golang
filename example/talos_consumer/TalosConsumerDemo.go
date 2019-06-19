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

	"github.com/XiaoMi/talos-sdk-golang/client"
	"github.com/XiaoMi/talos-sdk-golang/consumer"
	"github.com/XiaoMi/talos-sdk-golang/thrift/message"
	"github.com/XiaoMi/talos-sdk-golang/utils"

	log "github.com/sirupsen/logrus"
)

/**
 * Returns a talos message processor to processing data for a (assigned) partition.
 */
type MyMessageProcessorFactory struct {
}

func NewMyMessageProcessorFactory() *MyMessageProcessorFactory {
	return &MyMessageProcessorFactory{}
}

// using for thread-safe when processing different partition data
func (f *MyMessageProcessorFactory) CreateProcessor() consumer.MessageProcessor {
	processor := new(MyMessageProcessor)
	return processor
}

var successGetNumber = new(int64)

type MyMessageProcessor struct {
}

func (p *MyMessageProcessor) Process(messages []*message.MessageAndOffset,
	messageCheckpointer consumer.MessageCheckpointer) {
	for _, msg := range messages {
		log.Infof("Message content: %s", string(msg.GetMessage().GetMessage()))
	}
	atomic.AddInt64(successGetNumber, int64(len(messages)))
	log.Infof("Consuming total data so far: %d", atomic.LoadInt64(successGetNumber))

	/** if user has set 'galaxy.talos.consumer.checkpoint.auto.commit' to false,
	 * then you can call the 'checkpoint' to commit the list of messages.
	 */
	//messageCheckpointer.CheckpointByFinishedOffset()
}

func main() {
	utils.InitLog()
	// init client config by put $your_propertyFile in your classpath
	// with the content of:
	/*
	   galaxy.talos.service.endpoint=$talosServiceURI
	*/
	var propertyFilename string
	flag.StringVar(&propertyFilename, "conf", "talosConsumer.conf", "conf: talosConsumer.conf'")
	flag.Parse()

	// init talosConsumer
	talosConsumer, err := consumer.NewTalosConsumerByFilename(propertyFilename, NewMyMessageProcessorFactory(),
		client.NewSimpleTopicAbnormalCallback())
	log.Infof("12124124")
	if err != nil {
		log.Errorf("init talosConsumer failed: %s", err.Error())
		return
	}

	go func() {
		time.Sleep(5 * time.Second)
		talosConsumer.ShutDown()
	}()

	// block main function
	talosConsumer.WaitGroup.Wait()

}
