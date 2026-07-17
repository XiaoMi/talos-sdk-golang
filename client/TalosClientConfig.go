/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package client

import (
	"strconv"

	"github.com/XiaoMi/talos-sdk-golang/utils"
)

type TalosClientConfig struct {
	maxRetry                     int64
	clientTimeout                int64
	clientConnTimeout            int64
	adminOperationTimeout        int64
	httpProxyURL                 string
	httpProxyHost                string
	httpProxyPort                int64
	httpProxyUsername            string
	httpProxyPassword            string
	httpProxyPerHostTransport    bool
	serviceEndpoint              string
	maxIdleConns                 int64
	maxIdleConnsPerHost          int64
	maxConnsPerHost              int64
	dnsCacheSwitch               bool
	isRetry                      bool
	isAutoLocation               bool
	scheduleInfoMaxRetry         int64
	scheduleInfoInterval         int64
	clusterName                  string
	falconUrl                    string
	reportMetricInterval         int64
	consumerMetricFalconEndpoint string
	producerMetricFalconEndpoint string
	metricFalconStep             int64
	alertType                    string
	clientIp                     string
	clientMonitorSwitch          bool
	properties                   *utils.Properties
}

func NewTalosClientConfigByDefault() *TalosClientConfig {
	return InitClientConfig(utils.NewProperties())
}

func NewTalosClientConfigByFilename(filename string) *TalosClientConfig {
	return InitClientConfig(utils.LoadProperties(filename))
}

func NewTalosClientConfigByProperties(prop *utils.Properties) *TalosClientConfig {
	return InitClientConfig(prop)
}

func InitClientConfig(prop *utils.Properties) *TalosClientConfig {
	maxRetry, _ := strconv.ParseInt(prop.GetProperty(
		GALAXY_TALOS_CLIENT_MAX_RETRY,
		strconv.Itoa(GALAXY_TALOS_CLIENT_MAX_RETRY_DEFAULT)), 10, 64)
	clientTimeout, _ := strconv.ParseInt(prop.GetProperty(
		GALAXY_TALOS_CLIENT_TIMEOUT_MILLI_SECS,
		strconv.Itoa(GALAXY_TALOS_CLIENT_TIMEOUT_MILLI_SECS_DEFAULT)), 10, 64)
	clientConnTimeout, _ := strconv.ParseInt(prop.GetProperty(
		GALAXY_TALOS_CLIENT_CONN_TIMECOUT_MILLI_SECS,
		strconv.Itoa(GALAXY_TALOS_CLIENT_CONN_TIMECOUT_MILLI_SECS_DEFAULT)), 10, 64)
	httpProxyURL := prop.GetProperty(
		GALAXY_TALOS_HTTP_PROXY_URL, GALAXY_TALOS_HTTP_PROXY_URL_DEFAULT)
	httpProxyHost := prop.GetProperty(
		GALAXY_TALOS_HTTP_PROXY_HOST, GALAXY_TALOS_HTTP_PROXY_HOST_DEFAULT)
	httpProxyPort, _ := strconv.ParseInt(prop.GetProperty(
		GALAXY_TALOS_HTTP_PROXY_PORT,
		strconv.Itoa(GALAXY_TALOS_HTTP_PROXY_PORT_DEFAULT)), 10, 64)
	httpProxyUsername := prop.GetProperty(
		GALAXY_TALOS_HTTP_PROXY_USERNAME, GALAXY_TALOS_HTTP_PROXY_USERNAME_DEFAULT)
	httpProxyPassword := prop.GetProperty(
		GALAXY_TALOS_HTTP_PROXY_PASSWORD, GALAXY_TALOS_HTTP_PROXY_PASSWORD_DEFAULT)
	adminOperationTimeout, _ := strconv.ParseInt(prop.GetProperty(
		GALAXY_TALOS_CLIENT_ADMIN_TIMEOUT_MILLI_SECS,
		strconv.Itoa(GALAXY_TALOS_CLIENT_ADMIN_TIMEOUT_MILLI_SECS_DEFAULT)), 10, 64)
	serviceEndpoint := prop.GetProperty(
		GALAXY_TALOS_SERVICE_ENDPOINT, GALAXY_TALOS_DEFAULT_SERVICE_ENDPOINT)
	maxIdleConns, _ := strconv.ParseInt(prop.GetProperty(
		GALAXY_TALOS_HTTP_MAX_IDLE_CONNS,
		strconv.Itoa(GALAXY_TALOS_HTTP_MAX_IDLE_CONNS_DEFAULT)), 10, 64)
	maxIdleConnsPerHost, _ := strconv.ParseInt(prop.GetProperty(
		GALAXY_TALOS_HTTP_MAX_IDLE_CONNS_PER_HOST,
		strconv.Itoa(GALAXY_TALOS_HTTP_MAX_IDLE_CONNS_PER_HOST_DEFAULT)), 10, 64)
	maxConnsPerHost, _ := strconv.ParseInt(prop.GetProperty(
		GALAXY_TALOS_HTTP_MAX_CONNS_PER_HOST,
		strconv.Itoa(GALAXY_TALOS_HTTP_MAX_CONNS_PER_HOST_DEFAULT)), 10, 64)
	dnsCacheSwitch, _ := strconv.ParseBool(prop.GetProperty(
		GALAXY_TALOS_HTTP_DNS_CACHE_SWITCH,
		strconv.FormatBool(GALAXY_TALOS_HTTP_DNS_CACHE_SWITCH_DEFAULT)))
	httpProxyPerHostTransport, _ := strconv.ParseBool(prop.GetProperty(
		GALAXY_TALOS_HTTP_PROXY_PER_HOST_TRANSPORT_ENABLED,
		strconv.FormatBool(GALAXY_TALOS_HTTP_PROXY_PER_HOST_TRANSPORT_ENABLED_DEFAULT)))
	isRetry, _ := strconv.ParseBool(prop.GetProperty(
		GALAXY_TALOS_CLIENT_IS_RETRY,
		strconv.FormatBool(GALAXY_TALOS_CLIENT_IS_RETRY_DEFAULT)))
	isAutoLocation, _ := strconv.ParseBool(prop.GetProperty(
		GALAXY_TALOS_CLIENT_IS_AUTO_LOCATION,
		strconv.FormatBool(GALAXY_TALOS_CLIENT_IS_AUTO_LOCATION_DEFAULT)))
	scheduleInfoMaxRetry, _ := strconv.ParseInt(prop.GetProperty(
		GALAXY_TALOS_CLIENT_SCHEDULE_INFO_MAX_RETRY,
		strconv.Itoa(GALAXY_TALOS_CLIENT_SCHEDULE_INFO_MAX_RETRY_DEFAULT)), 10, 64)
	scheduleInfoInterval, _ := strconv.ParseInt(prop.GetProperty(
		GALAXY_TALOS_CLIENT_SCHEDULE_INFO_INTERVAL,
		strconv.Itoa(GALAXY_TALOS_CLIENT_SCHEDULE_INFO_INTERVAL_DEFAULT)), 10, 64)
	clusterName := utils.GetClusterFromEndPoint(serviceEndpoint)
	clientIp := utils.GetClientIP()
	falconUrl := prop.GetProperty(
		GALAXY_TALOS_METRIC_FALCON_URL, GALAXY_TALOS_METRIC_FALCON_URL_DEFAULT)
	reportMetricInterval, _ := strconv.ParseInt(prop.GetProperty(
		GALAXY_TALOS_REPORT_METRIC_INTERVAL,
		strconv.Itoa(GALAXY_TALOS_REPORT_METRIC_INTERVAL_DEFAULT)), 10, 64)
	consumerMetricFalconEndpoint := prop.GetProperty(
		GALAXY_TALOS_CONSUMER_METRIC_FALCON_ENDPOINT,
		GALAXY_TALOS_CONSUMER_METRIC_FALCON_ENDPOINT_DEFAULT)
	producerMetricFalconEndpoint := prop.GetProperty(
		GALAXY_TALOS_PRODUCER_METRIC_FALCON_ENDPOINT,
		GALAXY_TALOS_PRODUCER_METRIC_FALCON_ENDPOINT_DEFAULT)
	metricFalconStep, _ := strconv.ParseInt(prop.GetProperty(
		GALAXY_TALOS_CLIENT_FALCON_STEP,
		strconv.Itoa(GALAXY_TALOS_CLIENT_FALCON_STEP_DEFAULT)), 10, 64)
	alertType := prop.GetProperty(
		GALAXY_TALOS_CLIENT_ALERT_TYPE, GALAXY_TALOS_CLIENT_ALERT_TYPE_DEFAULT)
	clientMonitorSwitch, _ := strconv.ParseBool(prop.GetProperty(
		GALAXY_TALOS_CLIENT_FALCON_MONITOR_SWITCH,
		strconv.FormatBool(GALAXY_TALOS_CLIENT_FALCON_MONITOR_SWITCH_DEFAULT)))

	return &TalosClientConfig{
		maxRetry:                     maxRetry,
		clientTimeout:                clientTimeout,
		clientConnTimeout:            clientConnTimeout,
		adminOperationTimeout:        adminOperationTimeout,
		httpProxyURL:                 httpProxyURL,
		httpProxyHost:                httpProxyHost,
		httpProxyPort:                httpProxyPort,
		httpProxyUsername:            httpProxyUsername,
		httpProxyPassword:            httpProxyPassword,
		httpProxyPerHostTransport:    httpProxyPerHostTransport,
		serviceEndpoint:              serviceEndpoint,
		maxIdleConns:                 maxIdleConns,
		maxIdleConnsPerHost:          maxIdleConnsPerHost,
		maxConnsPerHost:              maxConnsPerHost,
		dnsCacheSwitch:               dnsCacheSwitch,
		isRetry:                      isRetry,
		isAutoLocation:               isAutoLocation,
		scheduleInfoMaxRetry:         scheduleInfoMaxRetry,
		scheduleInfoInterval:         scheduleInfoInterval,
		clusterName:                  clusterName,
		falconUrl:                    falconUrl,
		reportMetricInterval:         reportMetricInterval,
		consumerMetricFalconEndpoint: consumerMetricFalconEndpoint,
		producerMetricFalconEndpoint: producerMetricFalconEndpoint,
		metricFalconStep:             metricFalconStep,
		alertType:                    alertType,
		clientIp:                     clientIp,
		clientMonitorSwitch:          clientMonitorSwitch,
		properties:                   prop,
	}
}

func (c *TalosClientConfig) Properties() *utils.Properties {
	return c.properties
}

func (c *TalosClientConfig) FalconUrl() string {
	return c.falconUrl
}

func (c *TalosClientConfig) ClusterName() string {
	return c.clusterName
}

func (c *TalosClientConfig) ReportMetricInterval() int64 {
	return c.reportMetricInterval
}

func (c *TalosClientConfig) ConsumerMetricFalconEndpoint() string {
	return c.consumerMetricFalconEndpoint
}

func (c *TalosClientConfig) ProducerMetricFalconEndpoint() string {
	return c.producerMetricFalconEndpoint
}

func (c *TalosClientConfig) MetricFalconStep() int64 {
	return c.metricFalconStep
}

func (c *TalosClientConfig) AlertType() string {
	return c.alertType
}

func (c *TalosClientConfig) ClientIp() string {
	return c.clientIp
}

func (c *TalosClientConfig) ClientMonitorSwitch() bool {
	return c.clientMonitorSwitch
}

func (c *TalosClientConfig) MaxRetry() int64 {
	return c.maxRetry
}

func (c *TalosClientConfig) ClientTimeout() int64 {
	return c.clientTimeout
}

func (c *TalosClientConfig) ClientConnTimeout() int64 {
	return c.clientConnTimeout
}

func (c *TalosClientConfig) HttpProxyURL() string {
	return c.httpProxyURL
}

func (c *TalosClientConfig) HttpProxyHost() string {
	return c.httpProxyHost
}

func (c *TalosClientConfig) HttpProxyPort() int64 {
	return c.httpProxyPort
}

func (c *TalosClientConfig) HttpProxyUsername() string {
	return c.httpProxyUsername
}

func (c *TalosClientConfig) HttpProxyPassword() string {
	return c.httpProxyPassword
}

func (c *TalosClientConfig) HttpProxyPerHostTransport() bool {
	return c.httpProxyPerHostTransport
}

func (c *TalosClientConfig) AdminOperationTimeout() int64 {
	return c.adminOperationTimeout
}

func (c *TalosClientConfig) ServiceEndpoint() string {
	return c.serviceEndpoint
}

func (c *TalosClientConfig) MaxIdleConns() int64 {
	return c.maxIdleConns
}

func (c *TalosClientConfig) MaxIdleConnsPerHost() int64 {
	return c.maxIdleConnsPerHost
}

func (c *TalosClientConfig) MaxConnsPerHost() int64 {
	return c.maxConnsPerHost
}

func (c *TalosClientConfig) DNSCacheSwitch() bool {
	return c.dnsCacheSwitch
}

func (c *TalosClientConfig) IsRetry() bool {
	return c.isRetry
}

func (c *TalosClientConfig) IsAutoLocation() bool {
	return c.isAutoLocation
}

func (c *TalosClientConfig) ScheduleInfoMaxRetry() int64 {
	return c.scheduleInfoMaxRetry
}

func (c *TalosClientConfig) ScheduleInfoInterval() int64 {
	return c.scheduleInfoInterval
}

func (c *TalosClientConfig) SetMaxRetry(maxRetry int64) {
	c.maxRetry = maxRetry
}

func (c *TalosClientConfig) SetClientTimeout(clientTimeout int64) {
	c.clientTimeout = clientTimeout
}

func (c *TalosClientConfig) SetClientConnTimeout(clientConnTimeout int64) {
	c.clientConnTimeout = clientConnTimeout
}

func (c *TalosClientConfig) SetHttpProxyURL(httpProxyURL string) {
	c.httpProxyURL = httpProxyURL
}

func (c *TalosClientConfig) SetHttpProxyHost(httpProxyHost string) {
	c.httpProxyHost = httpProxyHost
}

func (c *TalosClientConfig) SetHttpProxyPort(httpProxyPort int64) {
	c.httpProxyPort = httpProxyPort
}

func (c *TalosClientConfig) SetHttpProxyUsername(httpProxyUsername string) {
	c.httpProxyUsername = httpProxyUsername
}

func (c *TalosClientConfig) SetHttpProxyPassword(httpProxyPassword string) {
	c.httpProxyPassword = httpProxyPassword
}

func (c *TalosClientConfig) SetHttpProxyPerHostTransport(httpProxyPerHostTransport bool) {
	c.httpProxyPerHostTransport = httpProxyPerHostTransport
}

func (c *TalosClientConfig) SetAdminOperationTimeout(adminOperationTimeout int64) {
	c.adminOperationTimeout = adminOperationTimeout
}

func (c *TalosClientConfig) SetServiceEndpoint(serviceEndpoint string) {
	c.serviceEndpoint = serviceEndpoint
}

func (c *TalosClientConfig) SetMaxIdleConns(maxIdleConns int64) {
	c.maxIdleConns = maxIdleConns
}

func (c *TalosClientConfig) SetMaxIdleConnsPerHost(maxIdleConnsPerHost int64) {
	c.maxIdleConnsPerHost = maxIdleConnsPerHost
}

func (c *TalosClientConfig) SetMaxConnsPerHost(maxConnsPerHost int64) {
	c.maxConnsPerHost = maxConnsPerHost
}

func (c *TalosClientConfig) SetDNSCacheSwitch(dnsCacheSwitch bool) {
	c.dnsCacheSwitch = dnsCacheSwitch
}

func (c *TalosClientConfig) SetIsRetry(isRetry bool) {
	c.isRetry = isRetry
}

func (c *TalosClientConfig) SetIsAutoLocation(isAutoLocation bool) {
	c.isAutoLocation = isAutoLocation
}

func (c *TalosClientConfig) SetScheduleInfoMaxRetry(scheduleInfoMaxRetry int64) {
	c.scheduleInfoMaxRetry = scheduleInfoMaxRetry
}

func (c *TalosClientConfig) SetScheduleInfoInterval(scheduleInfoInterval int64) {
	c.scheduleInfoInterval = scheduleInfoInterval
}
