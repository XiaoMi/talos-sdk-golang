/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package producer

import (
	"fmt"
	"strconv"

	"github.com/XiaoMi/talos-sdk-golang/client"
	"github.com/XiaoMi/talos-sdk-golang/thrift/message"
	"github.com/XiaoMi/talos-sdk-golang/utils"
)

type TalosProducerConfig struct {
	maxBufferedMsgNumber      int64
	maxBufferedMsgBytes       int64
	maxBufferedMsgTime        int64
	maxPutMsgNumber           int64
	maxPutMsgBytes            int64
	threadPoolsize            int64
	checkPartitionInterval    int64
	updatePartitionIdInterval int64
	waitPartitionWorkingTime  int64
	updatePartitionMsgNum     int64
	compressionType           string
	*client.TalosClientConfig
}

func NewTalosProducerConfigByDefault() *TalosProducerConfig {
	talosProducerConfig, err := initProducerConfig(utils.NewProperties())
	if err != nil {
		return nil
	}
	return talosProducerConfig
}

func NewTalosProducerConfigByFilename(filename string) *TalosProducerConfig {
	return NewTalosProducerConfigByProperties(utils.LoadProperties(filename))
}

func NewTalosProducerConfigByProperties(props *utils.Properties) *TalosProducerConfig {
	talosProducerConfig, err := initProducerConfig(props)
	if err != nil {
		return nil
	}
	if err = talosProducerConfig.CheckParameter(); err != nil {
		return nil
	}
	return talosProducerConfig
}

func NewTalosProducerConfigForTest(props *utils.Properties, checkParameter bool) *TalosProducerConfig {
	talosProducerConfig, err := initProducerConfig(props)
	if err != nil {
		return nil
	}
	if checkParameter {
		if err = talosProducerConfig.CheckParameter(); err != nil {
			return nil
		}
	}
	return talosProducerConfig
}

func initProducerConfig(props *utils.Properties) (*TalosProducerConfig, error) {
	talosClientConfig := client.InitClientConfig(props)
	maxBufferedMsgNumber, _ := strconv.ParseInt(props.GetProperty(
		GALAXY_TALOS_PRODUCER_MAX_BUFFERED_MESSAGE_NUMBER,
		strconv.Itoa(GALAXY_TALOS_PRODUCER_MAX_BUFFERED_MESSAGE_NUMBER_DEFAULT)), 10, 64)
	maxBufferedMsgBytes, _ := strconv.ParseInt(props.GetProperty(
		GALAXY_TALOS_PRODUCER_MAX_BUFFERED_MESSAGE_BYTES,
		strconv.Itoa(GALAXY_TALOS_PRODUCER_MAX_BUFFERED_MESSAGE_BYTES_DEFAULT)), 10, 64)
	maxBufferedMsgTime, _ := strconv.ParseInt(props.GetProperty(
		GALAXY_TALOS_PRODUCER_MAX_BUFFERED_MILLI_SECS,
		strconv.Itoa(GALAXY_TALOS_PRODUCER_MAX_BUFFERED_MILLI_SECS_DEFAULT)), 10, 64)
	maxPutMsgNumber, _ := strconv.ParseInt(props.GetProperty(
		GALAXY_TALOS_PRODUCER_MAX_PUT_MESSAGE_NUMBER,
		strconv.Itoa(GALAXY_TALOS_PRODUCER_MAX_PUT_MESSAGE_NUMBER_DEFAULT)), 10, 64)
	maxPutMsgBytes, _ := strconv.ParseInt(props.GetProperty(
		GALAXY_TALOS_PRODUCER_MAX_PUT_MESSAGE_BYTES,
		strconv.Itoa(GALAXY_TALOS_PRODUCER_MAX_PUT_MESSAGE_BYTES_DEFAULT)), 10, 64)
	threadPoolsize, _ := strconv.ParseInt(props.GetProperty(
		GALAXY_TALOS_PRODUCER_THREAD_POOL_SIZE,
		strconv.Itoa(GALAXY_TALOS_PRODUCER_THREAD_POOL_SIZE_DEFAULT)), 10, 64)
	checkPartitionInterval, _ := strconv.ParseInt(props.GetProperty(
		GALAXY_TALOS_PRODUCER_CHECK_PARTITION_INTERVAL,
		strconv.Itoa(GALAXY_TALOS_PRODUCER_CHECK_PARTITION_INTERVAL_DEFAULT)), 10, 64)
	updatePartitionIdInterval, _ := strconv.ParseInt(props.GetProperty(
		GALAXY_TALOS_PRODUCER_UPDATE_PARTITIONID_INTERVAL,
		strconv.Itoa(GALAXY_TALOS_PRODUCER_UPDATE_PARTITIONID_INTERVAL_DEFAULT)), 10, 64)
	waitPartitionWorkingTime, _ := strconv.ParseInt(props.GetProperty(
		GALAXY_TALOS_PRODUCER_WAIT_PARTITION_WORKING_TIME,
		strconv.Itoa(GALAXY_TALOS_PRODUCER_WAIT_PARTITION_WORKING_TIME_DEFAULT)), 10, 64)
	updatePartitionMsgNum, _ := strconv.ParseInt(props.GetProperty(
		GALAXY_TALOS_PRODUCER_UPDATE_PARTITION_MSGNUMBER,
		strconv.Itoa(GALAXY_TALOS_PRODUCER_UPDATE_PARTITION_MSGNUMBER_DEFAULT)), 10, 64)
	compressionType := props.GetProperty(
		GALAXY_TALOS_PRODUCER_COMPRESSION_TYPE,
		GALAXY_TALOS_PRODUCER_COMPRESSION_TYPE_DEFAULT)
	if compressionType != "NONE" && compressionType != "SNAPPY" &&
		compressionType != "GZIP" {
		return nil, fmt.Errorf("Unsupported Compression Type: %v ", compressionType)
	}
	return &TalosProducerConfig{
		maxBufferedMsgNumber:      maxBufferedMsgNumber,
		maxBufferedMsgBytes:       maxBufferedMsgBytes,
		maxBufferedMsgTime:        maxBufferedMsgTime,
		maxPutMsgNumber:           maxPutMsgNumber,
		maxPutMsgBytes:            maxPutMsgBytes,
		threadPoolsize:            threadPoolsize,
		checkPartitionInterval:    checkPartitionInterval,
		updatePartitionIdInterval: updatePartitionIdInterval,
		waitPartitionWorkingTime:  waitPartitionWorkingTime,
		updatePartitionMsgNum:     updatePartitionMsgNum,
		compressionType:           compressionType,
		TalosClientConfig:         talosClientConfig,
	}, nil
}

func (p *TalosProducerConfig) CheckParameter() error {
	err := utils.CheckParameterRange(GALAXY_TALOS_PRODUCER_MAX_PUT_MESSAGE_NUMBER,
		p.maxPutMsgNumber,
		GALAXY_TALOS_PRODUCER_MAX_PUT_MESSAGE_NUMBER_MINIMUM,
		GALAXY_TALOS_PRODUCER_MAX_PUT_MESSAGE_NUMBER_MAXIMUM)
	if err != nil {
		return err
	}

	err = utils.CheckParameterRange(GALAXY_TALOS_PRODUCER_MAX_PUT_MESSAGE_BYTES,
		p.maxPutMsgBytes,
		GALAXY_TALOS_PRODUCER_MAX_PUT_MESSAGE_BYTES_MINIMUM,
		GALAXY_TALOS_PRODUCER_MAX_PUT_MESSAGE_BYTES_MAXIMUM)
	if err != nil {
		return err
	}

	err = utils.CheckParameterRange(GALAXY_TALOS_PRODUCER_CHECK_PARTITION_INTERVAL,
		p.checkPartitionInterval,
		GALAXY_TALOS_PRODUCER_CHECK_PARTITION_INTERVAL_MINIMUM,
		GALAXY_TALOS_PRODUCER_CHECK_PARTITION_INTERVAL_MAXIMUM)
	if err != nil {
		return err
	}

	err = utils.CheckParameterRange(GALAXY_TALOS_PRODUCER_UPDATE_PARTITIONID_INTERVAL,
		p.updatePartitionIdInterval,
		GALAXY_TALOS_PRODUCER_UPDATE_PARTITIONID_INTERVAL_MINIMUM,
		GALAXY_TALOS_PRODUCER_UPDATE_PARTITIONID_INTERVAL_MAXIMUM)
	if err != nil {
		return err
	}
	return nil
}

func (p *TalosProducerConfig) GetMaxBufferedMsgNumber() int64 {
	return p.maxBufferedMsgNumber
}

func (p *TalosProducerConfig) GetMaxBufferedMsgBytes() int64 {
	return p.maxBufferedMsgBytes
}

func (p *TalosProducerConfig) GetMaxBufferedMsgTime() int64 {
	return p.maxBufferedMsgTime
}

func (p *TalosProducerConfig) GetMaxPutMsgNumber() int64 {
	return p.maxPutMsgNumber
}

func (p *TalosProducerConfig) GetMaxPutMsgBytes() int64 {
	return p.maxPutMsgBytes
}

func (p *TalosProducerConfig) GetThreadPoolsize() int64 {
	return p.threadPoolsize
}

func (p *TalosProducerConfig) GetCheckPartitionInterval() int64 {
	return p.checkPartitionInterval
}

func (p *TalosProducerConfig) GetUpdatePartitionIdInterval() int64 {
	return p.updatePartitionIdInterval
}

func (p *TalosProducerConfig) GetWaitPartitionWorkingTime() int64 {
	return p.waitPartitionWorkingTime
}

func (p *TalosProducerConfig) GetUpdatePartitionMsgNum() int64 {
	return p.updatePartitionMsgNum
}

func (p *TalosProducerConfig) GetCompressionType() message.MessageCompressionType {
	if p.compressionType == "NONE" {
		return message.MessageCompressionType_NONE
	} else if p.compressionType == "SNAPPY" {
		return message.MessageCompressionType_SNAPPY
	} else {
		err := utils.CheckArgument(p.compressionType == "GZIP")
		if err != nil {
			return message.MessageCompressionType(0)
		}
		return message.MessageCompressionType_GZIP
	}
}

func (p *TalosProducerConfig) SetMaxBufferedMsgNumber(maxBufferedMsgNumber int64) {
	p.maxBufferedMsgNumber = maxBufferedMsgNumber
}

func (p *TalosProducerConfig) SetMaxBufferedMsgBytes(maxBufferedMsgBytes int64) {
	p.maxBufferedMsgBytes = maxBufferedMsgBytes
}

func (p *TalosProducerConfig) SetMaxBufferedMsgTime(maxBufferedMsgTime int64) {
	p.maxBufferedMsgTime = maxBufferedMsgTime
}

func (p *TalosProducerConfig) SetMaxPutMsgNumber(maxPutMsgNumber int64) {
	err := utils.CheckParameterRange(
		GALAXY_TALOS_PRODUCER_MAX_PUT_MESSAGE_NUMBER,
		p.maxPutMsgNumber,
		GALAXY_TALOS_PRODUCER_MAX_PUT_MESSAGE_NUMBER_MINIMUM,
		GALAXY_TALOS_PRODUCER_MAX_PUT_MESSAGE_NUMBER_MAXIMUM)
	if err != nil {
		return
	}
	p.maxPutMsgNumber = maxPutMsgNumber
}

func (p *TalosProducerConfig) SetMaxPutMsgBytes(maxPutMsgBytes int64) {
	err := utils.CheckParameterRange(
		GALAXY_TALOS_PRODUCER_MAX_PUT_MESSAGE_BYTES,
		p.maxPutMsgBytes,
		GALAXY_TALOS_PRODUCER_MAX_PUT_MESSAGE_BYTES_MINIMUM,
		GALAXY_TALOS_PRODUCER_MAX_PUT_MESSAGE_BYTES_MAXIMUM)
	if err != nil {
		return
	}
	p.maxPutMsgBytes = maxPutMsgBytes
}

func (p *TalosProducerConfig) SetThreadPoolsize(threadPoolsize int64) {
	p.threadPoolsize = threadPoolsize
}

func (p *TalosProducerConfig) SetCheckPartitionInterval(checkPartitionInterval int64) {
	err := utils.CheckParameterRange(
		GALAXY_TALOS_PRODUCER_CHECK_PARTITION_INTERVAL,
		p.checkPartitionInterval,
		GALAXY_TALOS_PRODUCER_CHECK_PARTITION_INTERVAL_MINIMUM,
		GALAXY_TALOS_PRODUCER_CHECK_PARTITION_INTERVAL_MAXIMUM)
	if err != nil {
		return
	}
	p.checkPartitionInterval = checkPartitionInterval
}

func (p *TalosProducerConfig) SetUpdatePartitionIdInterval(updatePartitionIdInterval int64) {
	err := utils.CheckParameterRange(GALAXY_TALOS_PRODUCER_UPDATE_PARTITIONID_INTERVAL,
		p.updatePartitionIdInterval,
		GALAXY_TALOS_PRODUCER_UPDATE_PARTITIONID_INTERVAL_MINIMUM,
		GALAXY_TALOS_PRODUCER_UPDATE_PARTITIONID_INTERVAL_MAXIMUM)
	if err != nil {
		return
	}
	p.updatePartitionIdInterval = updatePartitionIdInterval

}

func (p *TalosProducerConfig) SetWaitPartitionWorkingTime(waitPartitionWorkingTime int64) {
	p.waitPartitionWorkingTime = waitPartitionWorkingTime
}

func (p *TalosProducerConfig) SetUpdatePartitionMsgNum(updatePartitionMsgNum int64) {
	p.updatePartitionMsgNum = updatePartitionMsgNum
}

func (p *TalosProducerConfig) SetCompressionType(compressionType string) {
	p.compressionType = compressionType
}
