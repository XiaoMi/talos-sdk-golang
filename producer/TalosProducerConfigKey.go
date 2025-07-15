/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package producer

const (
	/**
	 * The producer buffered message number for one partition
	 * if the total number exceed it, not allowed to addMessage for user
	 */
	GALAXY_TALOS_PRODUCER_MAX_BUFFERED_MESSAGE_NUMBER         = "galaxy.talos.producer.max.buffered.message.number"
	GALAXY_TALOS_PRODUCER_MAX_BUFFERED_MESSAGE_NUMBER_DEFAULT = 1000000

	/**
	 * The producer buffered message bytes for one partition
	 * if the total bytes exceed it, not allowed to addMessage for user
	 */
	GALAXY_TALOS_PRODUCER_MAX_BUFFERED_MESSAGE_BYTES         = "galaxy.talos.producer.max.buffered.message.bytes"
	GALAXY_TALOS_PRODUCER_MAX_BUFFERED_MESSAGE_BYTES_DEFAULT = 500 * 1024 * 1024

	/**
	 * The producer buffered message time for one partition
	 * if the oldest buffered message always exceed the time,
	 * putMessage will always be called
	 */
	GALAXY_TALOS_PRODUCER_MAX_BUFFERED_MILLI_SECS         = "galaxy.talos.producer.max.buffered.milli.secs"
	GALAXY_TALOS_PRODUCER_MAX_BUFFERED_MILLI_SECS_DEFAULT = 200

	/**
	 * The producer max number of message in each putMessage batch
	 */
	GALAXY_TALOS_PRODUCER_MAX_PUT_MESSAGE_NUMBER         = "galaxy.talos.producer.max.put.message.number"
	GALAXY_TALOS_PRODUCER_MAX_PUT_MESSAGE_NUMBER_DEFAULT = 2000
	GALAXY_TALOS_PRODUCER_MAX_PUT_MESSAGE_NUMBER_MINIMUM = 1
	GALAXY_TALOS_PRODUCER_MAX_PUT_MESSAGE_NUMBER_MAXIMUM = 5000

	/**
	 * The producer max bytes of message in each putMessage batch
	 */
	GALAXY_TALOS_PRODUCER_MAX_PUT_MESSAGE_BYTES         = "galaxy.talos.producer.max.put.message.bytes"
	GALAXY_TALOS_PRODUCER_MAX_PUT_MESSAGE_BYTES_DEFAULT = 1024 * 1024
	GALAXY_TALOS_PRODUCER_MAX_PUT_MESSAGE_BYTES_MINIMUM = 1
	GALAXY_TALOS_PRODUCER_MAX_PUT_MESSAGE_BYTES_MAXIMUM = 10 * 1024 * 1024

	/**
	 * The producer max bytes of a single message
	 */
	GALAXY_TALOS_PRODUCER_MAX_SINGLE_MESSAGE_BYTES         = "galaxy.talos.producer.max.single.message.bytes"
	GALAXY_TALOS_PRODUCER_MAX_SINGLE_MESSAGE_BYTES_DEFAULT = 10 * 1024 * 1024
	GALAXY_TALOS_PRODUCER_MAX_SINGLE_MESSAGE_BYTES_MINIMUM = 10 * 1024 * 1024
	GALAXY_TALOS_PRODUCER_MAX_SINGLE_MESSAGE_BYTES_MAXIMUM = 100 * 1024 * 1024

	/**
	 * The producer thread pool number
	 */
	GALAXY_TALOS_PRODUCER_THREAD_POOL_SIZE         = "galaxy.talos.producer.thread.pool.size"
	GALAXY_TALOS_PRODUCER_THREAD_POOL_SIZE_DEFAULT = 16

	/**
	 * The producer scan/update partition number interval
	 */
	GALAXY_TALOS_PRODUCER_CHECK_PARTITION_INTERVAL         = "galaxy.talos.producer.check.partition.interval"
	GALAXY_TALOS_PRODUCER_CHECK_PARTITION_INTERVAL_DEFAULT = 1000 * 60 * 3
	GALAXY_TALOS_PRODUCER_CHECK_PARTITION_INTERVAL_MINIMUM = 1000 * 60 * 1
	GALAXY_TALOS_PRODUCER_CHECK_PARTITION_INTERVAL_MAXIMUM = 1000 * 60 * 5

	/**
	 * The producer update partitionId time interval when calling addMessage
	 * 100 million secs by default
	 */
	GALAXY_TALOS_PRODUCER_UPDATE_PARTITIONID_INTERVAL         = "galaxy.talos.producer.update.partition.id.interval.milli"
	GALAXY_TALOS_PRODUCER_UPDATE_PARTITIONID_INTERVAL_DEFAULT = 100
	GALAXY_TALOS_PRODUCER_UPDATE_PARTITIONID_INTERVAL_MINIMUM = 1
	GALAXY_TALOS_PRODUCER_UPDATE_PARTITIONID_INTERVAL_MAXIMUM = 500

	/**
	 * The producer update partitionId
	 * when message number added one partition enough large
	 */
	GALAXY_TALOS_PRODUCER_UPDATE_PARTITION_MSGNUMBER         = "galaxy.talos.producer.update.partition.msgnumber"
	GALAXY_TALOS_PRODUCER_UPDATE_PARTITION_MSGNUMBER_DEFAULT = 1000

	/**
	 * The producer partitionSender sleep/delay time when partitionNotServing
	 */
	GALAXY_TALOS_PRODUCER_WAIT_PARTITION_WORKING_TIME         = "galaxy.talos.producer.wait.partition.working.time.milli"
	GALAXY_TALOS_PRODUCER_WAIT_PARTITION_WORKING_TIME_DEFAULT = 200

	/**
	 * The producer compression type, right now suport "NONE", "SNAPPY", "GZIP", "ZSTD" and "LZ4";
	 * default is "SNAPPY";
	 */
	GALAXY_TALOS_PRODUCER_COMPRESSION_TYPE         = "galaxy.talos.producer.compression.type"
	GALAXY_TALOS_PRODUCER_COMPRESSION_TYPE_DEFAULT = "SNAPPY"
)
