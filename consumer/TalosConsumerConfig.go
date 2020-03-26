/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package consumer

import (
	"strconv"

	"github.com/XiaoMi/talos-sdk-golang/client"
	"github.com/XiaoMi/talos-sdk-golang/utils"
)

type TalosConsumerConfig struct {
	partitionCheckInterval          int64
	workerInfoCheckInterval         int64
	renewCheckInterval              int64
	renewMaxRetry                   int64
	maxFetchRecords                 int64
	selfRegisterMaxRetry            int64
	commitOffsetThreshold           int64
	commitOffsetInterval            int64
	fetchMessageInterval            int64
	checkLastCommitOffset           bool
	waitPartitionWorkingTime        int64
	resetLatestOffsetWhenOutOfRange bool
	checkpointAutoCommit            bool
	resetOffsetWhenStart            bool
	resetOffsetValueWhenStart       int64
	*client.TalosClientConfig
}

func NewTalosConsumerConfigByDefault() *TalosConsumerConfig {
	return initConsumerConfig(utils.NewProperties())
}

func NewTalosConsumerConfigByFilename(filename string) *TalosConsumerConfig {
	return NewTalosConsumerConfigByProperties(utils.LoadProperties(filename))
}

func NewTalosConsumerConfigByProperties(props *utils.Properties) *TalosConsumerConfig {
	talosConsumerConfig := initConsumerConfig(props)
	if err := talosConsumerConfig.CheckParameter(); err != nil {
		return nil
	}
	return talosConsumerConfig
}

func initConsumerConfig(props *utils.Properties) *TalosConsumerConfig {
	talosClientConfig := client.InitClientConfig(props)
	partitionCheckInterval, _ := strconv.ParseInt(props.GetProperty(
		GALAXY_TALOS_CONSUMER_CHECK_PARTITION_INTERVAL,
		strconv.Itoa(GALAXY_TALOS_CONSUMER_CHECK_PARTITION_INTERVAL_DEFAULT)), 10, 64)
	workerInfoCheckInterval, _ := strconv.ParseInt(props.GetProperty(
		GALAXY_TALOS_CONSUMER_CHECK_WORKER_INFO_INTERVAL,
		strconv.Itoa(GALAXY_TALOS_CONSUMER_CHECK_WORKER_INFO_INTERVAL_DEFAULT)), 10, 64)
	renewCheckInterval, _ := strconv.ParseInt(props.GetProperty(
		GALAXY_TALOS_CONSUMER_RENEW_INTERVAL,
		strconv.Itoa(GALAXY_TALOS_CONSUMER_RENEW_INTERVAL_DEFAULT)), 10, 64)
	renewMaxRetry, _ := strconv.ParseInt(props.GetProperty(
		GALAXY_TALOS_CONSUMER_RENEW_MAX_RETRY,
		strconv.Itoa(GALAXY_TALOS_CONSUMER_RENEW_MAX_RETRY_DEFAULT)), 10, 64)
	maxFetchRecords, _ := strconv.ParseInt(props.GetProperty(
		GALAXY_TALOS_CONSUMER_MAX_FETCH_RECORDS,
		strconv.Itoa(GALAXY_TALOS_CONSUMER_MAX_FETCH_RECORDS_DEFAULT)), 10, 64)
	selfRegisterMaxRetry, _ := strconv.ParseInt(props.GetProperty(
		GALAXY_TALOS_CONSUMER_REGISTER_MAX_RETRY,
		strconv.Itoa(GALAXY_TALOS_CONSUMER_REGISTER_MAX_RETRY_DEFAULT)), 10, 64)
	commitOffsetThreshold, _ := strconv.ParseInt(props.GetProperty(
		GALAXY_TALOS_CONSUMER_COMMIT_OFFSET_THRESHOLD,
		strconv.Itoa(GALAXY_TALOS_CONSUMER_COMMIT_OFFSET_THRESHOLD_DEFAULT)), 10, 64)
	commitOffsetInterval, _ := strconv.ParseInt(props.GetProperty(
		GALAXY_TALOS_CONSUMER_COMMIT_OFFSET_INTERVAL,
		strconv.Itoa(GALAXY_TALOS_CONSUMER_COMMIT_OFFSET_INTERVAL_DEFAULT)), 10, 64)
	fetchMessageInterval, _ := strconv.ParseInt(props.GetProperty(
		GALAXY_TALOS_CONSUMER_FETCH_INTERVAL,
		strconv.Itoa(GALAXY_TALOS_CONSUMER_FETCH_INTERVAL_DEFAULT)), 10, 64)
	checkLastCommitOffset, _ := strconv.ParseBool(props.GetProperty(
		GALAXY_TALOS_CONSUMER_CHECK_LAST_COMMIT_OFFSET_SWITCH,
		strconv.FormatBool(GALAXY_TALOS_CONSUMER_CHECK_LAST_COMMIT_OFFSET_SWITCH_DEFAULT)))
	waitPartitionWorkingTime, _ := strconv.ParseInt(props.GetProperty(
		GALAXY_TALOS_CONSUMER_WAIT_PARTITION_WORKING_TIME,
		strconv.Itoa(GALAXY_TALOS_CONSUMER_WAIT_PARTITION_WORKING_TIME_DEFAULT)), 10, 64)
	resetLatestOffsetWhenOutOfRange, _ := strconv.ParseBool(props.GetProperty(
		GALAXY_TALOS_CONSUMER_OUT_OF_RANGE_RESET_LATEST_OFFSET,
		strconv.FormatBool(GALAXY_TALOS_CONSUMER_OUT_OF_RANGE_RESET_LATEST_OFFSET_DEFAULT)))
	checkpointAutoCommit, _ := strconv.ParseBool(props.GetProperty(
		GALAXY_TALOS_CONSUMER_CHECKPOINT_AUTO_COMMIT,
		strconv.FormatBool(GALAXY_TALOS_CONSUMER_CHECKPOINT_AUTO_COMMIT_DEFAULT)))
	resetOffsetWhenStart, _ := strconv.ParseBool(props.GetProperty(
		GALAXY_TALOS_CONSUMER_START_WHETHER_RESET_OFFSET,
		strconv.FormatBool(GALAXY_TALOS_CONSUMER_START_WHETHER_RESET_OFFSET_DEFAULT)))
	resetOffsetValueWhenStart, _ := strconv.ParseInt(props.GetProperty(
		GALAXY_TALOS_CONSUMER_START_RESET_OFFSET_VALUE,
		strconv.Itoa(GALAXY_TALOS_CONSUMER_START_RESET_OFFSET_AS_START)), 10, 64)

	return &TalosConsumerConfig{
		partitionCheckInterval:          partitionCheckInterval,
		workerInfoCheckInterval:         workerInfoCheckInterval,
		renewCheckInterval:              renewCheckInterval,
		renewMaxRetry:                   renewMaxRetry,
		maxFetchRecords:                 maxFetchRecords,
		selfRegisterMaxRetry:            selfRegisterMaxRetry,
		commitOffsetThreshold:           commitOffsetThreshold,
		commitOffsetInterval:            commitOffsetInterval,
		fetchMessageInterval:            fetchMessageInterval,
		checkLastCommitOffset:           checkLastCommitOffset,
		waitPartitionWorkingTime:        waitPartitionWorkingTime,
		resetLatestOffsetWhenOutOfRange: resetLatestOffsetWhenOutOfRange,
		checkpointAutoCommit:            checkpointAutoCommit,
		resetOffsetWhenStart:            resetOffsetWhenStart,
		resetOffsetValueWhenStart:       resetOffsetValueWhenStart,
		TalosClientConfig:               talosClientConfig,
	}
}

func (c *TalosConsumerConfig) CheckParameter() error {
	err := utils.CheckParameterRange(GALAXY_TALOS_CONSUMER_CHECK_PARTITION_INTERVAL,
		c.partitionCheckInterval,
		GALAXY_TALOS_CONSUMER_CHECK_PARTITION_INTERVAL_MINIMUM,
		GALAXY_TALOS_CONSUMER_CHECK_PARTITION_INTERVAL_MAXIMUM)
	if err != nil {
		return err
	}

	err = utils.CheckParameterRange(GALAXY_TALOS_CONSUMER_CHECK_WORKER_INFO_INTERVAL,
		c.workerInfoCheckInterval,
		GALAXY_TALOS_CONSUMER_CHECK_WORKER_INFO_INTERVAL_MINIMUM,
		GALAXY_TALOS_CONSUMER_CHECK_WORKER_INFO_INTERVAL_MAXIMUM)
	if err != nil {
		return err
	}

	err = utils.CheckParameterRange(GALAXY_TALOS_CONSUMER_RENEW_INTERVAL,
		c.renewCheckInterval,
		GALAXY_TALOS_CONSUMER_RENEW_INTERVAL_MINIMUM,
		GALAXY_TALOS_CONSUMER_RENEW_INTERVAL_MAXIMUM)
	if err != nil {
		return err
	}

	err = utils.CheckParameterRange(GALAXY_TALOS_CONSUMER_RENEW_MAX_RETRY,
		c.renewMaxRetry,
		GALAXY_TALOS_CONSUMER_RENEW_MAX_RETRY_MINIMUM,
		GALAXY_TALOS_CONSUMER_RENEW_MAX_RETRY_MAXIMUM)
	if err != nil {
		return err
	}

	err = utils.CheckParameterRange(GALAXY_TALOS_CONSUMER_MAX_FETCH_RECORDS,
		c.maxFetchRecords,
		GALAXY_TALOS_CONSUMER_MAX_FETCH_RECORDS_MINIMUM,
		GALAXY_TALOS_CONSUMER_MAX_FETCH_RECORDS_MAXIMUM)
	if err != nil {
		return err
	}

	err = utils.CheckParameterRange(GALAXY_TALOS_CONSUMER_COMMIT_OFFSET_THRESHOLD,
		c.commitOffsetThreshold,
		GALAXY_TALOS_CONSUMER_COMMIT_OFFSET_THRESHOLD_MINIMUM,
		GALAXY_TALOS_CONSUMER_COMMIT_OFFSET_THRESHOLD_MAXIMUM)
	if err != nil {
		return err
	}

	err = utils.CheckParameterRange(GALAXY_TALOS_CONSUMER_COMMIT_OFFSET_INTERVAL,
		c.commitOffsetInterval,
		GALAXY_TALOS_CONSUMER_COMMIT_OFFSET_INTERVAL_MINIMUM,
		GALAXY_TALOS_CONSUMER_COMMIT_OFFSET_INTERVAL_MAXIMUM)
	if err != nil {
		return err
	}

	err = utils.CheckParameterRange(GALAXY_TALOS_CONSUMER_FETCH_INTERVAL,
		c.fetchMessageInterval,
		GALAXY_TALOS_CONSUMER_FETCH_INTERVAL_MINIMUM,
		GALAXY_TALOS_CONSUMER_FETCH_INTERVAL_MAXIMUM)
	if err != nil {
		return err
	}

	err = utils.CheckParameterRange(GALAXY_TALOS_CONSUMER_START_RESET_OFFSET_VALUE,
		c.resetOffsetValueWhenStart,
		GALAXY_TALOS_CONSUMER_START_RESET_OFFSET_AS_END,
		GALAXY_TALOS_CONSUMER_START_RESET_OFFSET_AS_START)
	if err != nil {
		return err
	}
	return nil
}

func (c *TalosConsumerConfig) GetPartitionCheckInterval() int64 {
	return c.partitionCheckInterval
}
func (c *TalosConsumerConfig) GetWorkerInfoCheckInterval() int64 {
	return c.workerInfoCheckInterval
}
func (c *TalosConsumerConfig) GetRenewCheckInterval() int64 {
	return c.renewCheckInterval
}
func (c *TalosConsumerConfig) GetRenewMaxRetry() int64 {
	return c.renewMaxRetry
}
func (c *TalosConsumerConfig) GetMaxFetchRecords() int64 {
	return c.maxFetchRecords
}
func (c *TalosConsumerConfig) GetSelfRegisterMaxRetry() int64 {
	return c.selfRegisterMaxRetry
}
func (c *TalosConsumerConfig) GetCommitOffsetThreshold() int64 {
	return c.commitOffsetThreshold
}
func (c *TalosConsumerConfig) GetCommitOffsetInterval() int64 {
	return c.commitOffsetInterval
}
func (c *TalosConsumerConfig) GetFetchMessageInterval() int64 {
	return c.fetchMessageInterval
}
func (c *TalosConsumerConfig) GetCheckLastCommitOffset() bool {
	return c.checkLastCommitOffset
}
func (c *TalosConsumerConfig) GetWaitPartitionWorkingTime() int64 {
	return c.waitPartitionWorkingTime
}
func (c *TalosConsumerConfig) GetResetLatestOffsetWhenOutOfRange() bool {
	return c.resetLatestOffsetWhenOutOfRange
}
func (c *TalosConsumerConfig) GetCheckpointAutoCommit() bool {
	return c.checkpointAutoCommit
}
func (c *TalosConsumerConfig) GetResetOffsetWhenStart() bool {
	return c.resetOffsetWhenStart
}
func (c *TalosConsumerConfig) GetResetOffsetValueWhenStart() int64 {
	return c.resetOffsetValueWhenStart
}

func (c *TalosConsumerConfig) SetPartitionCheckInterval(partitionCheckInterval int64) {
	err := utils.CheckParameterRange(GALAXY_TALOS_CONSUMER_CHECK_PARTITION_INTERVAL,
		c.partitionCheckInterval,
		GALAXY_TALOS_CONSUMER_CHECK_PARTITION_INTERVAL_MINIMUM,
		GALAXY_TALOS_CONSUMER_CHECK_PARTITION_INTERVAL_MAXIMUM)
	if err != nil {
		return
	}
	c.partitionCheckInterval = partitionCheckInterval
}

func (c *TalosConsumerConfig) SetWorkerInfoCheckInterval(workerInfoCheckInterval int64) {
	err := utils.CheckParameterRange(GALAXY_TALOS_CONSUMER_CHECK_WORKER_INFO_INTERVAL,
		c.workerInfoCheckInterval,
		GALAXY_TALOS_CONSUMER_CHECK_WORKER_INFO_INTERVAL_MINIMUM,
		GALAXY_TALOS_CONSUMER_CHECK_WORKER_INFO_INTERVAL_MAXIMUM)
	if err != nil {
		return
	}
	c.workerInfoCheckInterval = workerInfoCheckInterval
}

func (c *TalosConsumerConfig) SetRenewCheckInterval(renewCheckInterval int64) {
	err := utils.CheckParameterRange(GALAXY_TALOS_CONSUMER_RENEW_INTERVAL,
		c.renewCheckInterval,
		GALAXY_TALOS_CONSUMER_RENEW_INTERVAL_MINIMUM,
		GALAXY_TALOS_CONSUMER_RENEW_INTERVAL_MAXIMUM)
	if err != nil {
		return
	}
	c.renewCheckInterval = renewCheckInterval
}

func (c *TalosConsumerConfig) SetRenewMaxRetry(renewMaxRetry int64) {
	err := utils.CheckParameterRange(GALAXY_TALOS_CONSUMER_RENEW_MAX_RETRY,
		c.renewMaxRetry,
		GALAXY_TALOS_CONSUMER_RENEW_MAX_RETRY_MINIMUM,
		GALAXY_TALOS_CONSUMER_RENEW_MAX_RETRY_MAXIMUM)
	if err != nil {
		return
	}
	c.renewMaxRetry = renewMaxRetry
}

func (c *TalosConsumerConfig) SetMaxFetchRecords(maxFetchRecords int64) {
	err := utils.CheckParameterRange(GALAXY_TALOS_CONSUMER_MAX_FETCH_RECORDS,
		c.maxFetchRecords,
		GALAXY_TALOS_CONSUMER_MAX_FETCH_RECORDS_MINIMUM,
		GALAXY_TALOS_CONSUMER_MAX_FETCH_RECORDS_MAXIMUM)
	if err != nil {
		return
	}
	c.maxFetchRecords = maxFetchRecords
}

func (c *TalosConsumerConfig) SetSelfRegisterMaxRetry(selfRegisterMaxRetry int64) {
	c.selfRegisterMaxRetry = selfRegisterMaxRetry
}

func (c *TalosConsumerConfig) SetCommitOffsetThreshold(commitOffsetThreshold int64) {
	err := utils.CheckParameterRange(GALAXY_TALOS_CONSUMER_COMMIT_OFFSET_THRESHOLD,
		c.commitOffsetThreshold,
		GALAXY_TALOS_CONSUMER_COMMIT_OFFSET_THRESHOLD_MINIMUM,
		GALAXY_TALOS_CONSUMER_COMMIT_OFFSET_THRESHOLD_MAXIMUM)
	if err != nil {
		return
	}
	c.commitOffsetThreshold = commitOffsetThreshold
}

func (c *TalosConsumerConfig) SetCommitOffsetInterval(commitOffsetInterval int64) {
	err := utils.CheckParameterRange(GALAXY_TALOS_CONSUMER_COMMIT_OFFSET_INTERVAL,
		c.commitOffsetInterval,
		GALAXY_TALOS_CONSUMER_COMMIT_OFFSET_INTERVAL_MINIMUM,
		GALAXY_TALOS_CONSUMER_COMMIT_OFFSET_INTERVAL_MAXIMUM)
	if err != nil {
		return
	}
	c.commitOffsetInterval = commitOffsetInterval
}

func (c *TalosConsumerConfig) SetFetchMessageInterval(fetchMessageInterval int64) {
	err := utils.CheckParameterRange(GALAXY_TALOS_CONSUMER_FETCH_INTERVAL,
		c.fetchMessageInterval,
		GALAXY_TALOS_CONSUMER_FETCH_INTERVAL_MINIMUM,
		GALAXY_TALOS_CONSUMER_FETCH_INTERVAL_MAXIMUM)
	if err != nil {
		return
	}
	c.fetchMessageInterval = fetchMessageInterval
}

func (c *TalosConsumerConfig) SetCheckLastCommitOffset(checkLastCommitOffset bool) {
	c.checkLastCommitOffset = checkLastCommitOffset
}

func (c *TalosConsumerConfig) SetWaitPartitionWorkingTime(waitPartitionWorkingTime int64) {
	c.waitPartitionWorkingTime = waitPartitionWorkingTime
}

func (c *TalosConsumerConfig) SetResetLatestOffsetWhenOutOfRange(resetLatestOffsetWhenOutOfRange bool) {
	c.resetLatestOffsetWhenOutOfRange = resetLatestOffsetWhenOutOfRange
}

func (c *TalosConsumerConfig) SetCheckpointAutoCommit(checkpointAutoCommit bool) {
	c.checkpointAutoCommit = checkpointAutoCommit
}

func (c *TalosConsumerConfig) SetResetOffsetWhenStart(resetOffsetWhenStart bool) {
	c.resetOffsetWhenStart = resetOffsetWhenStart
}

func (c *TalosConsumerConfig) SetResetOffsetValueWhenStart(resetOffsetValueWhenStart int64) {
	err := utils.CheckParameterRange(GALAXY_TALOS_CONSUMER_START_RESET_OFFSET_VALUE,
		c.resetOffsetValueWhenStart,
		GALAXY_TALOS_CONSUMER_START_RESET_OFFSET_AS_END,
		GALAXY_TALOS_CONSUMER_START_RESET_OFFSET_AS_START)
	if err != nil {
		return
	}
	c.resetOffsetValueWhenStart = resetOffsetValueWhenStart
}
