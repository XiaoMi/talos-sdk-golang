/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package client

import (
	"sync"
	"time"

	"github.com/XiaoMi/talos-sdk-golang/thrift/message"
	"github.com/XiaoMi/talos-sdk-golang/thrift/topic"
	"github.com/XiaoMi/talos-sdk-golang/utils"

	"github.com/sirupsen/logrus"
)

type ScheduleInfoCacheInterface interface {
	GetOrCreateMessageClient(topicAndPartition *topic.TopicAndPartition) message.MessageService
	GetScheduleInfo(name *topic.TopicTalosResourceName) error
	UpdateScheduleInfoCache()
	GetScheduleInfoTask() error
	Shutdown(topicTalosResourceName *topic.TopicTalosResourceName)
}

var (
	cacheLock            sync.Mutex
	ScheduleInfoCacheMap = make(map[string]*ScheduleInfoCache)
)

type ScheduleInfoCache struct {
	topicTalosResourceName *topic.TopicTalosResourceName
	talosClientFactory     TalosClientFactoryInterface
	scheduleInfoMap        map[string]map[int32]string
	messageClient          message.MessageService
	messageClientMap       map[string]message.MessageService
	isAutoLocation         bool
	talosClientConfig      *TalosClientConfig
	infoLock               sync.RWMutex
	clientMapLock          sync.RWMutex
	log                    *logrus.Logger
}

func NewNonAutoLocationScheduleInfoCache(topicTalosResourceName *topic.TopicTalosResourceName,
	talosClientConfig *TalosClientConfig, messageClient message.MessageService, logger *logrus.Logger) *ScheduleInfoCache {
	c := &ScheduleInfoCache{
		topicTalosResourceName: topicTalosResourceName,
		talosClientConfig:      talosClientConfig,
		isAutoLocation:         false,
		messageClient:          messageClient,
		messageClientMap:       make(map[string]message.MessageService),
		scheduleInfoMap:        make(map[string]map[int32]string),
		log:                    logger,
	}
	c.log.Warnf("SimpleProducer or SimpleConsumer was built using improperly" +
		" constructed function.Auto location was forbidden")
	return c
}

func NewAutoLocationScheduleInfoCache(topicTalosResourceName *topic.TopicTalosResourceName,
	talosClientConfig *TalosClientConfig, messageClient message.MessageService,
	talosClientFactory TalosClientFactoryInterface, logger *logrus.Logger) *ScheduleInfoCache {
	c := &ScheduleInfoCache{
		topicTalosResourceName: topicTalosResourceName,
		talosClientConfig:      talosClientConfig,
		isAutoLocation:         talosClientConfig.IsAutoLocation(),
		messageClient:          messageClient,
		messageClientMap:       make(map[string]message.MessageService),
		talosClientFactory:     talosClientFactory,
		scheduleInfoMap:        make(map[string]map[int32]string),
		log:                    logger,
	}
	c.log.Infof("Auto location is %v for request of %s ",
		talosClientConfig.IsAutoLocation(),
		topicTalosResourceName.GetTopicTalosResourceName())

	//get and update scheduleInfoMap
	err := c.GetScheduleInfo(topicTalosResourceName)
	if err != nil {
		c.log.Errorf("Exception in GetScheduleInfoTask: %s", err.Error())
		return c
	}

	return c
}

func GetScheduleInfoCache(topicTalosResourceName *topic.TopicTalosResourceName,
	talosClientConfig *TalosClientConfig, messageClient message.MessageService,
	talosClientFactory TalosClientFactoryInterface, logger *logrus.Logger) *ScheduleInfoCache {
	cacheLock.Lock()
	defer cacheLock.Unlock()
	if _, ok := ScheduleInfoCacheMap[topicTalosResourceName.GetTopicTalosResourceName()]; !ok {
		if talosClientFactory == nil {
			// this case should not exist normally, only when interface of simpleAPI improper used
			ScheduleInfoCacheMap[topicTalosResourceName.GetTopicTalosResourceName()] = NewNonAutoLocationScheduleInfoCache(
				topicTalosResourceName, talosClientConfig, messageClient, logger)
		} else {
			ScheduleInfoCacheMap[topicTalosResourceName.GetTopicTalosResourceName()] = NewAutoLocationScheduleInfoCache(
				topicTalosResourceName, talosClientConfig, messageClient, talosClientFactory, logger)
		}
	}
	return ScheduleInfoCacheMap[topicTalosResourceName.GetTopicTalosResourceName()]
}

func (c *ScheduleInfoCache) IsAutoLocation() bool {
	return c.isAutoLocation
}

func (c *ScheduleInfoCache) UpdateScheduleInfoCache() {
	if c.IsAutoLocation() {
		//shutdown
		go func() {
			err := c.GetScheduleInfoTask()
			if err != nil {
				c.log.Errorf(err.Error())
			}
		}()
	}
}

func (c *ScheduleInfoCache) GetOrCreateMessageClient(topicAndPartition *topic.
	TopicAndPartition) message.MessageService {
	if c.scheduleInfoMap == nil {
		c.UpdateScheduleInfoCache()
		return c.messageClient
	}

	host, ok := c.scheduleInfoMap[topicAndPartition.TopicName][topicAndPartition.PartitionId]
	if !ok {
		c.UpdateScheduleInfoCache()
		return c.messageClient
	}

	c.clientMapLock.Lock()
	messageClient, ok := c.messageClientMap[host]
	c.clientMapLock.Unlock()

	if !ok {
		messageClient = c.talosClientFactory.NewMessageClient("http://" + host + utils.TALOS_MESSAGE_SERVICE_PATH)
		c.clientMapLock.Lock()
		c.messageClientMap[host] = messageClient
		c.clientMapLock.Unlock()
	}
	return messageClient
}

func (c *ScheduleInfoCache) GetScheduleInfoTask() (err error) {
	for maxRetry := c.talosClientConfig.ScheduleInfoMaxRetry() + 1; maxRetry > 0; maxRetry-- {
		// get and update scheduleInfoMap
		err = c.GetScheduleInfo(c.topicTalosResourceName)
		if err != nil {
			if utils.IsTopicNotExist(err) {
				return
			}
			continue
		}
		// to prevent frequent ScheduleInfo call
		time.Sleep(time.Second * 10)
		return nil
	}
	return
}

func (c *ScheduleInfoCache) GetScheduleInfo(resourceName *topic.TopicTalosResourceName) error {
	// judge isAutoLocation several place to make sure request server only when need.
	// 1.before send Executor task make sure send Executor task when need;
	// 2.judge in getScheduleInfo is the Final guarantee good for code extendibility;
	if c.IsAutoLocation() {
		response, err := c.messageClient.GetScheduleInfo(
			&message.GetScheduleInfoRequest{TopicTalosResourceName: resourceName})
		if err != nil {
			return err
		}
		c.infoLock.Lock()
		scheduleInfoMap := make(map[string]map[int32]string)
		for k, v := range response.GetScheduleInfo() {
			if _, ok := scheduleInfoMap[k.TopicName]; !ok {
				scheduleInfoMap[k.TopicName] = make(map[int32]string)
			}
			scheduleInfoMap[k.TopicName][k.PartitionId] = v
		}
		c.scheduleInfoMap = scheduleInfoMap
		c.infoLock.Unlock()
		c.log.Debugf("get ScheduleInfoMap success: %v", c.scheduleInfoMap)
	}
	return nil
}

func (c *ScheduleInfoCache) Shutdown(topicTalosResourceName *topic.TopicTalosResourceName) {
	//log.Infof("scheduleInfoCache of %s is shutting down...",
	//	topicTalosResourceName.GetTopicTalosResourceName())
	//ScheduleInfoCacheMap[topicTalosResourceName].statusChan <- utils.Shutdown
	c.log.Infof("scheduleInfoCache of %s was shutdown.",
		topicTalosResourceName.GetTopicTalosResourceName())
}
