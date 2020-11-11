/**
 * Copyright 2019, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package utils

const (
	GALAXY_TALOS_DEFAULT_SERVICE_ENDPOINT                = "http://talos.api.xiaomi.com"
	GALAXY_TALOS_DEFAULT_SECURE_SERVICE_ENDPOINT         = "https://talos.api.xiaomi.com"
	GALAXY_TALOS_CLIENT_TIMEOUT_MILLI_SECS_DEFAULT       = 10000
	GALAXY_TALOS_CLIENT_ADMIN_TIMEOUT_MILLI_SECS_DEFAULT = 30000
	GALAXY_TALOS_CLIENT_CONN_TIMECOUT_MILLI_SECS_DEFAULT = 5000
	TALOS_API_ROOT_PATH                                  = "/v1/api"
	TALOS_TOPIC_SERVICE_PATH                             = "/v1/api/topic"
	TALOS_MESSAGE_SERVICE_PATH                           = "/v1/api/message"
	TALOS_QUOTA_SERVICE_PATH                             = "/v1/api/quota"
	TALOS_CONSUMER_SERVICE_PATH                          = "/v1/api/consumer"
	TALOS_METRIC_SERVICE_PATH                            = "/v1/api/metric"
	TALOS_IDENTIFIER_DELIMITER                           = "#"
	TALOS_NAME_REGEX                                     = "^(?!_)(?!-)(?!.*?_$)[a-zA-Z0-9_-]+$"
	TALOS_NAME_BODY_REGEX                                = "^[a-zA-Z0-9_-]+$"
	TALOS_NAME_HEAD_REGEX                                = "^[-_]+|_$"
	TALOS_SINGLE_MESSAGE_BYTES_MINIMAL                   = 1
	TALOS_SINGLE_MESSAGE_BYTES_MAXIMAL                   = 10485760
	TALOS_MESSAGE_BLOCK_BYTES_MAXIMAL                    = 20971520
	TALOS_PARTITION_KEY_LENGTH_MINIMAL                   = 1
	TALOS_PARTITION_KEY_LENGTH_MAXIMAL                   = 256
	TALOS_CLOUD_TOPIC_NAME_DELIMITER                     = "/"
	TALOS_CLOUD_ORG_PREFIX                               = "CL"
	TALOS_CLOUD_TEAM_PREFIX                              = "CI"
	TALOS_CLOUD_AK_PREFIX                                = "AK"
	TALOS_GALAXY_AK_PREFIX                               = "EAK"
	/**
	 * Constants for consumer metrics
	 */
	FETCH_MESSAGE_TIMES                                  = "fetchMessage.60sRate"
	FETCH_MESSAGE_FAILED_TIMES                           = "fetchMessageFailed.60sRate"
	FETCH_MESSAGE_TIME                                   = "fetchMessageTime.gauge"
	MAX_FETCH_MESSAGE_TIME                               = "fetchMessageTime.max"
	MIN_FETCH_MESSAGE_TIME                               = "fetchMessageTime.min"
	FETCH_MESSAGE_INTERVAL                               = "fetchMessageInterval.gauge"
	PROCESS_MESSAGE_TIME                                 = "processMessageTime.gauge"
	MAX_PROCESS_MESSAGE_TIME                             = "processMessageTime.max"
	MIN_PROCESS_MESSAGE_TIME                             = "processMessageTime.min"
	SCHEMA_INCOMPATIBLE_MESSAGE_COUNT                    = "schemaIncompatibleMessageNumber"
	/**
	* Constants for producer metrics
	*/
	PUT_MESSAGE_TIMES                                    = "putMessage.60sRate"
	PUT_MESSAGE_FAILED_TIMES                             = "putMessageFailed.60sRate"
	PUT_MESSAGE_TIME                                     = "putMessageTime.gauge"
	MAX_PUT_MESSAGE_TIME                                 = "putMessageTime.max"
	MIN_PUT_MESSAGE_TIME                                 = "putMessageTime.min"
)

type StopSign int

const (
	Shutdown StopSign = iota // 0
	Running
)

type LockState int

const (
	WAIT   LockState = 0
	NOTIFY LockState = 1
)

type ProducerState int32

const (
	ACTIVE ProducerState = iota
	DISABLED
	SHUTDOWN
)
