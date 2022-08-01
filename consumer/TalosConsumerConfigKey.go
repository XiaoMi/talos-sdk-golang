/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package consumer

const (
	/**
	 * The consumer scan/update partition number interval(milli secs)
	 */
	GALAXY_TALOS_CONSUMER_CHECK_PARTITION_INTERVAL         = "galaxy.talos.consumer.check.partition.interval"
	GALAXY_TALOS_CONSUMER_CHECK_PARTITION_INTERVAL_DEFAULT = 60 * 1000
	GALAXY_TALOS_CONSUMER_CHECK_PARTITION_INTERVAL_MINIMUM = 1000 * 60
	GALAXY_TALOS_CONSUMER_CHECK_PARTITION_INTERVAL_MAXIMUM = 1000 * 60 * 3

	/**
	 * The consumer check all authorized topics that match given pattern
	 */
	GALAXY_TALOS_CONSUMER_CHECK_TOPIC_PATTERN_INTERVAL         = "galaxy.talos.consumer.check.topic.pattern.interval"
	GALAXY_TALOS_CONSUMER_CHECK_TOPIC_PATTERN_INTERVAL_DEFAULT = 5 * 60 * 1000
	GALAXY_TALOS_CONSUMER_CHECK_TOPIC_PATTERN_INTERVAL_MINIMUM = 3 * 60 * 1000
	GALAXY_TALOS_CONSUMER_CHECK_TOPIC_PATTERN_INTERVAL_MAXIMUM = 10 * 60 * 1000

	/**
	 * The consumer check alive worker info and their serving partitions interval
	 */
	GALAXY_TALOS_CONSUMER_CHECK_WORKER_INFO_INTERVAL         = "galaxy.talos.consumer.check.worker.info.interval"
	GALAXY_TALOS_CONSUMER_CHECK_WORKER_INFO_INTERVAL_DEFAULT = 1000 * 10
	GALAXY_TALOS_CONSUMER_CHECK_WORKER_INFO_INTERVAL_MINIMUM = 1000 * 10
	GALAXY_TALOS_CONSUMER_CHECK_WORKER_INFO_INTERVAL_MAXIMUM = 1000 * 30

	/**
	 * The consumer renew interval for both heartbeat and renew serving partitions
	 * the worker column family ttl is 30s by default
	 */
	GALAXY_TALOS_CONSUMER_RENEW_INTERVAL         = "galaxy.talos.consumer.renew.interval"
	GALAXY_TALOS_CONSUMER_RENEW_INTERVAL_DEFAULT = 1000 * 7
	GALAXY_TALOS_CONSUMER_RENEW_INTERVAL_MINIMUM = 1000 * 7
	GALAXY_TALOS_CONSUMER_RENEW_INTERVAL_MAXIMUM = 1000 * 10

	/**
	 * The consumer renew max retry
	 */
	GALAXY_TALOS_CONSUMER_RENEW_MAX_RETRY         = "galaxy.talos.consumer.renew.max.retry"
	GALAXY_TALOS_CONSUMER_RENEW_MAX_RETRY_DEFAULT = 1
	GALAXY_TALOS_CONSUMER_RENEW_MAX_RETRY_MINIMUM = 1
	GALAXY_TALOS_CONSUMER_RENEW_MAX_RETRY_MAXIMUM = 3

	/**
	 * The consumer getRecords max fetch message number
	 */
	GALAXY_TALOS_CONSUMER_MAX_FETCH_RECORDS         = "galaxy.talos.consumer.max.fetch.records"
	GALAXY_TALOS_CONSUMER_MAX_FETCH_RECORDS_DEFAULT = 1000
	GALAXY_TALOS_CONSUMER_MAX_FETCH_RECORDS_MINIMUM = 1
	GALAXY_TALOS_CONSUMER_MAX_FETCH_RECORDS_MAXIMUM = 2000

	/**
	 * The consumer getRecords max fetch message bytes
	 */
	GALAXY_TALOS_CONSUMER_MAX_FETCH_BYTES         = "galaxy.talos.consumer.max.fetch.bytes"
	GALAXY_TALOS_CONSUMER_MAX_FETCH_BYTES_DEFAULT = 2 * 1024 * 1024
	GALAXY_TALOS_CONSUMER_MAX_FETCH_BYTES_MINIMUM = 1
	GALAXY_TALOS_CONSUMER_MAX_FETCH_BYTES_MAXIMUM = 10 * 1024 * 1024

	/**
	* The consumer fetch message operation interval
	* Note server GetRecords qps is [1,5], the minimal interval is 200ms
	 */
	GALAXY_TALOS_CONSUMER_FETCH_INTERVAL         = "galaxy.talos.consumer.fetch.interval.ms"
	GALAXY_TALOS_CONSUMER_FETCH_INTERVAL_DEFAULT = 200
	GALAXY_TALOS_CONSUMER_FETCH_INTERVAL_MINIMUM = 50
	GALAXY_TALOS_CONSUMER_FETCH_INTERVAL_MAXIMUM = 800

	/**
	 * The consumer worker register self max retry times
	 */
	GALAXY_TALOS_CONSUMER_REGISTER_MAX_RETRY         = "galaxy.talos.consumer.register.max.retry"
	GALAXY_TALOS_CONSUMER_REGISTER_MAX_RETRY_DEFAULT = 1

	/**
	 * The consumer commit offset fetched records number threshold
	 */
	GALAXY_TALOS_CONSUMER_COMMIT_OFFSET_THRESHOLD         = "galaxy.talos.consumer.commit.offset.record.fetched.num"
	GALAXY_TALOS_CONSUMER_COMMIT_OFFSET_THRESHOLD_DEFAULT = 10000
	GALAXY_TALOS_CONSUMER_COMMIT_OFFSET_THRESHOLD_MINIMUM = 5000
	GALAXY_TALOS_CONSUMER_COMMIT_OFFSET_THRESHOLD_MAXIMUM = 20000

	/**
	 * The consumer commit offset time interval threshold, milli secs
	 */
	GALAXY_TALOS_CONSUMER_COMMIT_OFFSET_INTERVAL         = "galaxy.talos.consumer.commit.offset.interval.milli"
	GALAXY_TALOS_CONSUMER_COMMIT_OFFSET_INTERVAL_DEFAULT = 5000
	GALAXY_TALOS_CONSUMER_COMMIT_OFFSET_INTERVAL_MINIMUM = 3000
	GALAXY_TALOS_CONSUMER_COMMIT_OFFSET_INTERVAL_MAXIMUM = 8000

	/**
	 * The consumer switch for whether checking lastCommitOffset or not
	 * when commit offset
	 */
	GALAXY_TALOS_CONSUMER_CHECK_LAST_COMMIT_OFFSET_SWITCH         = "galaxy.talos.consumer.check.last.commit.offset.switch"
	GALAXY_TALOS_CONSUMER_CHECK_LAST_COMMIT_OFFSET_SWITCH_DEFAULT = false

	/**
	 * The consumer partitionFetcher sleep/delay time when partitionNotServing
	 */
	GALAXY_TALOS_CONSUMER_WAIT_PARTITION_WORKING_TIME         = "galaxy.talos.consumer.wait.partition.working.time.milli"
	GALAXY_TALOS_CONSUMER_WAIT_PARTITION_WORKING_TIME_DEFAULT = 200

	/**
	 * The consumer reset offset by the latest offset when out of range
	 */
	GALAXY_TALOS_CONSUMER_OUT_OF_RANGE_RESET_LATEST_OFFSET         = "galaxy.talos.consumer.out.of.range.reset.latest.offset"
	GALAXY_TALOS_CONSUMER_OUT_OF_RANGE_RESET_LATEST_OFFSET_DEFAULT = false

	/**
	 * The consumer checkpoint auto commit;
	 */
	GALAXY_TALOS_CONSUMER_CHECKPOINT_AUTO_COMMIT         = "galaxy.talos.consumer.checkpoint.auto.commit"
	GALAXY_TALOS_CONSUMER_CHECKPOINT_AUTO_COMMIT_DEFAULT = true

	/**
	 * When the consumer starts up (including first start and restart),
	 * this configuration indicates whether reset the offset which from reading.
	 * The default value is false which means reading messages from the
	 * 'last commit offset'(restart) or 'MessageOffset.START_OFFSET'(firstly start).
	 */
	GALAXY_TALOS_CONSUMER_START_WHETHER_RESET_OFFSET         = "galaxy.talos.consumer.start.whether.reset.offset"
	GALAXY_TALOS_CONSUMER_START_WHETHER_RESET_OFFSET_DEFAULT = false

	/**
	 * In the following conditions, this configuration will be effective:
	 * 1) 'galaxy.talos.consumer.restart.whether.reset.offset' is 'true';
	 * 2) the consumer is firstly starting which means there is not historical committed offset
	 * Either condition above will lead to the consumer reading messages from the 'reset offset'.
	 *
	 * The value of 'reset offset' has two kinds: -1, -2.
	 * '-1' represents reading message from 'MessageOffset.START_OFFSET'
	 * '-2' represents reading message from 'MessageOffset.LATEST_OFFSET'
	 */
	GALAXY_TALOS_CONSUMER_START_RESET_OFFSET_VALUE    = "galaxy.talos.consumer.start.reset.offset.value"
	GALAXY_TALOS_CONSUMER_START_RESET_OFFSET_AS_START = -1
	GALAXY_TALOS_CONSUMER_START_RESET_OFFSET_AS_END   = -2
)
