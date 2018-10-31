/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package client

import (
	"errors"
	"sync"
	"time"

	"../thrift/message"
	"../thrift/topic"

	log "github.com/alecthomas/log4go"
)

type ScheduleInfo interface {
	GetOrCreateMessageClient(topicAndPartition *topic.TopicAndPartition) message.MessageService
	GetScheduleInfoCache(topicTalosResourceName *topic.TopicTalosResourceName) *ScheduleInfoCache
	SetIsAutoLocaton(autoLocation bool)
	IsAutoLocation() bool
	GetScheduleInfo(name *topic.TopicTalosResourceName) error
	InitGetScheduleInfoTask()
	UpdateScheduleInfoCache()
	GetScheduleInfoTask() error
	Shutdown(topicTalosResourceName *topic.TopicTalosResourceName)
}

type ScheduleInfoCache struct {
	topicTalosResourceName *topic.TopicTalosResourceName
	talosClientFactory     TalosClient
	scheduleInfoMap        map[*topic.TopicAndPartition]string
	messageClient          message.MessageService
	messageClientMap       map[string]message.MessageService
	isAutoLocation         bool
	talosClientConfig      *TalosClientConfig
	rwLock                 sync.RWMutex
	scheduleInfoCacheMap   map[*topic.TopicTalosResourceName]*ScheduleInfoCache
	stopSign               chan int
}

func NewScheduleInfoCache(topicTalosResourceName *topic.TopicTalosResourceName,
	talosClientConfig *TalosClientConfig, messageClient message.MessageService,
	talosClientFactory TalosClient) *ScheduleInfoCache {

	var scheduleInfoCache *ScheduleInfoCache
	if talosClientFactory == nil {
		scheduleInfoCache = NewScheduleInfoCacheByNonAutoLocation(
			topicTalosResourceName, talosClientConfig, messageClient)
	} else {
		scheduleInfoCache = NewScheduleInfoCacheByAutoLocation(
			topicTalosResourceName, talosClientConfig, messageClient, talosClientFactory)
	}
	scheduleInfoCache.scheduleInfoCacheMap[topicTalosResourceName] = scheduleInfoCache
	return scheduleInfoCache
}

func NewScheduleInfoCacheByAutoLocation(topicTalosResourceName *topic.TopicTalosResourceName,
	talosClientConfig *TalosClientConfig, messageClient message.MessageService,
	talosClientFactory TalosClient) *ScheduleInfoCache {
	scheduleInfoCache := &ScheduleInfoCache{
		topicTalosResourceName: topicTalosResourceName,
		talosClientConfig:      talosClientConfig,
		isAutoLocation:         talosClientConfig.IsAutoLocation(),
		messageClient:          messageClient,
		messageClientMap:       make(map[string]message.MessageService),
		talosClientFactory:     talosClientFactory,
		stopSign:               make(chan int),
		scheduleInfoCacheMap:   make(map[*topic.TopicTalosResourceName]*ScheduleInfoCache),
		scheduleInfoMap:        make(map[*topic.TopicAndPartition]string),
	}
	if talosClientConfig.IsAutoLocation() {
		log.Info("Auto location is enabled for request of %s ",
			topicTalosResourceName.GetTopicTalosResourceName())
	} else {
		log.Info("Auto location is forbidden for request of %s ",
			topicTalosResourceName.GetTopicTalosResourceName())
	}
	//get and update scheduleInfoMap
	err := scheduleInfoCache.GetScheduleInfo(topicTalosResourceName)
	if err != nil {
		log.Error("Exception in GetScheduleInfoTask: %s", err.Error())
		return nil
	}
	go scheduleInfoCache.InitGetScheduleInfoTask()
	return scheduleInfoCache
}

func NewScheduleInfoCacheByNonAutoLocation(topicTalosResourceName *topic.TopicTalosResourceName,
	talosClientConfig *TalosClientConfig, messageClient message.MessageService) *ScheduleInfoCache {

	scheduleInfoCache := &ScheduleInfoCache{
		topicTalosResourceName: topicTalosResourceName,
		talosClientConfig:      talosClientConfig,
		isAutoLocation:         false,
		messageClient:          messageClient,
		stopSign:               make(chan int),
		messageClientMap:       make(map[string]message.MessageService),
		scheduleInfoCacheMap:   make(map[*topic.TopicTalosResourceName]*ScheduleInfoCache),
		scheduleInfoMap:        make(map[*topic.TopicAndPartition]string),
	}
	log.Warn("SimpleProducer or SimpleConsumer was built using improperly " +
		"constructed function.Auto location was forbidden")
	return scheduleInfoCache
}

func (c *ScheduleInfoCache) GetScheduleInfoCache(
	topicTalosResourceName *topic.TopicTalosResourceName) *ScheduleInfoCache {

	if _, ok := c.scheduleInfoCacheMap[topicTalosResourceName]; !ok {
		log.Error("scheduleInfoCacheMap not contains sheduleInfoCache: %s",
			topicTalosResourceName.GetTopicTalosResourceName())
		return nil
	}
	return c.scheduleInfoCacheMap[topicTalosResourceName]
}

func (c *ScheduleInfoCache) SetIsAutoLocaton(autoLocation bool) {
	c.isAutoLocation = autoLocation
}

func (c *ScheduleInfoCache) IsAutoLocation() bool {
	return c.isAutoLocation
}

func (c *ScheduleInfoCache) GetScheduleInfo(name *topic.TopicTalosResourceName) error {
	// judge isAutoLocation serveral place to make sure request server only when need.
	// 1.before send Executor task make sure send Executor task when need;
	// 2.judge in getScheduleInfo is the Final guarantee good for code extendibility;
	if c.IsAutoLocation() {
		request := &message.GetScheduleInfoRequest{TopicTalosResourceName: name}
		response, err := c.messageClient.GetScheduleInfo(request)
		if err != nil {
			return err
		}
		topicScheduleInfoMap := response.GetScheduleInfo()
		c.rwLock.Lock()
		c.scheduleInfoMap = topicScheduleInfoMap
		c.rwLock.Unlock()
		log.Debug("get ScheduleInfoMap success: %v", c.scheduleInfoMap)
	}
	return nil
}

func (c *ScheduleInfoCache) InitGetScheduleInfoTask() {
	if c.IsAutoLocation() {
		// run getScheduleInfoTask per ten minutes
		duration := time.Duration(GALAXY_TALOS_CLIENT_SCHEDULE_INFO_INTERVAL_DEFAULT) *
			time.Millisecond
		ticker := time.NewTicker(duration)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				err := c.GetScheduleInfoTask()
				if err != nil {
					log.Error(err.Error())
				}
			case <-c.stopSign:
				return
			}
		}
	}
}

func (c *ScheduleInfoCache) UpdateScheduleInfoCache() {
	if c.IsAutoLocation() {
		//shutdown
		go func() {
			err := c.GetScheduleInfoTask()
			if err != nil {
				log.Error(err.Error())
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
	host, ok := c.scheduleInfoMap[topicAndPartition]
	if !ok {
		c.UpdateScheduleInfoCache()
		return c.messageClient
	}
	messageClient, ok := c.messageClientMap[host]
	if !ok {
		messageClient = c.talosClientFactory.NewMessageClient("http://" + host)
		c.messageClientMap[host] = messageClient
	}
	return messageClient
}

func (c *ScheduleInfoCache) GetScheduleInfoTask() error {
	for maxRetry := c.talosClientConfig.ScheduleInfoMaxRetry() + 1; maxRetry > 0; maxRetry-- {
		// get and update scheduleInfoMap
		if err := c.GetScheduleInfo(c.topicTalosResourceName); err != nil {
			log.Error("Error in GetScheduleInfoTask: %s", err.Error())
			return err
		}
		// to prevent frequent ScheduleInfo call
		time.Sleep(time.Second * 10)
		return nil
	}
	return errors.New("unKnow error when goroutine sleep ")
}

func (c *ScheduleInfoCache) Shutdown(topicTalosResourceName *topic.TopicTalosResourceName) {
	log.Info("scheduleInfoCache of %s is shutting down...",
		topicTalosResourceName.GetTopicTalosResourceName())
	c.scheduleInfoCacheMap[topicTalosResourceName].stopSign <- 0
	log.Info("scheduleInfoCache of %s shutdown.",
		topicTalosResourceName.GetTopicTalosResourceName())
}

