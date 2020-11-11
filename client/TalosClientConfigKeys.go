/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package client

const (
	/**
	 * The client max retry times before throw exception
	 */
	GALAXY_TALOS_CLIENT_MAX_RETRY         = "galaxy.talos.client.max.retry"
	GALAXY_TALOS_CLIENT_MAX_RETRY_DEFAULT = 1

	/**
	 * The client timeout milli secs when write/read
	 */
	GALAXY_TALOS_CLIENT_TIMEOUT_MILLI_SECS         = "galaxy.talos.client.timeout.milli.secs"
	GALAXY_TALOS_CLIENT_TIMEOUT_MILLI_SECS_DEFAULT = 10000

	/**
	 * The client connection timeout
	 */
	GALAXY_TALOS_CLIENT_CONN_TIMECOUT_MILLI_SECS         = "galaxy.talos.client.conn.timeout.milli.secs"
	GALAXY_TALOS_CLIENT_CONN_TIMECOUT_MILLI_SECS_DEFAULT = 5000

	/**
	 * The client DDL operation timeout
	 */
	GALAXY_TALOS_CLIENT_ADMIN_TIMEOUT_MILLI_SECS         = "galaxy.talos.client.admin.timeout.milli.secs"
	GALAXY_TALOS_CLIENT_ADMIN_TIMEOUT_MILLI_SECS_DEFAULT = 30000

	/**
	 * Constants for http/https rpc address
	 */
	GALAXY_TALOS_SERVICE_ENDPOINT         = "galaxy.talos.service.endpoint"
	GALAXY_TALOS_DEFAULT_SERVICE_ENDPOINT = "http://talos.api.xiaomi.com"

	GALAXY_TALOS_SECURE_SERVICE_ENDPOINT         = "galaxy.talos.secure.service.endpoint"
	GALAXY_TALOS_DEFAULT_SECURE_SERVICE_ENDPOINT = "https://talos.api.xiaomi.com"

	/**
	 * The http client connection params
	 */
	GALAXY_TALOS_HTTP_MAX_TOTAL_CONNECTION         = "galaxy.talos.http.max.total.connection"
	GALAXY_TALOS_HTTP_MAX_TOTAL_CONNECTION_DEFAULT = 160

	GALAXY_TALOS_HTTP_MAX_TOTAL_CONNECTION_PER_ROUTE         = "galaxy.talos.http.max.total.connection.per.route"
	GALAXY_TALOS_HTTP_MAX_TOTAL_CONNECTION_PER_ROUTE_DEFAULT = 160

	/**
	 * The client whether to retry
	 */
	GALAXY_TALOS_CLIENT_IS_RETRY         = "galaxy.talos.client.is.retry"
	GALAXY_TALOS_CLIENT_IS_RETRY_DEFAULT = false

	/**
	 * The client whether auto location the serving restserver
	 */
	GALAXY_TALOS_CLIENT_IS_AUTO_LOCATION         = "galaxy.talos.client.is.auto.location"
	GALAXY_TALOS_CLIENT_IS_AUTO_LOCATION_DEFAULT = true

	/**
	 * The client get schedule info max retry
	 */
	GALAXY_TALOS_CLIENT_SCHEDULE_INFO_MAX_RETRY         = "galaxy.talos.client.schedule.info.max.retry"
	GALAXY_TALOS_CLIENT_SCHEDULE_INFO_MAX_RETRY_DEFAULT = 1

	/**
	 * interval for client update it's scheduleinfo cache
	 */
	GALAXY_TALOS_CLIENT_SCHEDULE_INFO_INTERVAL         = "galaxy.talos.client.schedule.info.interval"
	GALAXY_TALOS_CLIENT_SCHEDULE_INFO_INTERVAL_DEFAULT = 1000 * 60 * 10

	/**
	 * The TALOS client falcon monitor related
	 */
	GALAXY_TALOS_METRIC_FALCON_URL         = "galaxy.talos.metric.falcon.url"
    GALAXY_TALOS_METRIC_FALCON_URL_DEFAULT = "http://127.0.0.1:1988/v1/push"

    GALAXY_TALOS_REPORT_METRIC_INTERVAL         = "galaxy.talos.client.report.metric.interval"
    GALAXY_TALOS_REPORT_METRIC_INTERVAL_DEFAULT = 60

    GALAXY_TALOS_CLIENT_FALCON_STEP         = "galaxy.talos.sdk.falcon.step"
    GALAXY_TALOS_CLIENT_FALCON_STEP_DEFAULT = 60

    GALAXY_TALOS_CLIENT_ALERT_TYPE         = "galaxy.talos.client.alert.type"
    GALAXY_TALOS_CLIENT_ALERT_TYPE_DEFAULT = "srv"

    GALAXY_TALOS_CONSUMER_METRIC_FALCON_ENDPOINT         = "galaxy.talos.consumer.metric.falcon.endpoint"
    GALAXY_TALOS_CONSUMER_METRIC_FALCON_ENDPOINT_DEFAULT = "talos.consumer."

    GALAXY_TALOS_PRODUCER_METRIC_FALCON_ENDPOINT         = "galaxy.talos.producer.metric.falcon.endpoint"
    GALAXY_TALOS_PRODUCER_METRIC_FALCON_ENDPOINT_DEFAULT = "talos.producer."

    GALAXY_TALOS_CLIENT_FALCON_MONITOR_SWITCH         = "galaxy.talos.client.falcon.monitor.switch"
    GALAXY_TALOS_CLIENT_FALCON_MONITOR_SWITCH_DEFAULT = true
)
