/**
 * Copyright 2020, Xiaomi.
 * All rights reserved.
 * Author: fangchengjin@xiaomi.com
 */

package utils

import (
	"bytes"
	"encoding/json"
	_ "fmt"
	"net/http"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
)

type FalconMetric struct {
	Endpoint  string      `json:"endpoint"`
	Metric    string      `json:"metric"`
	Value     interface{} `json:"value"`
	Step      int64       `json:"step"`
	Type      string      `json:"counterType"`
	Tags      string      `json:"tags"`
	Timestamp int64       `json:"timestamp"`
}

type FalconWriter struct {
	falconUrl string
	log       *logrus.Logger
}

func NewFalconWriter(falconUrl string, log *logrus.Logger) *FalconWriter {
	return &FalconWriter{
		falconUrl: falconUrl,
		log:       log,
	}
}

func NewFalconMetric(endpoint, name string, step int64, value interface{}, tags *Tags) *FalconMetric {
	return &FalconMetric{
		Endpoint:  endpoint,
		Metric:    name,
		Value:     value,
		Step:      step,
		Type:      "GAUGE",
		Tags:      tags.ToStr(),
		Timestamp: time.Now().Unix(),
	}
}

func (f *FalconWriter) PushToFalcon(ms []*FalconMetric) error {
	mb, err := json.Marshal(ms)
	if err != nil {
		f.log.Errorf("Convert metric data to json failed: %s", err.Error())
		return err
	}
	bodyReader := bytes.NewBuffer(mb)
	resp, err := http.Post(f.falconUrl, "application/json", bodyReader)
	if err != nil {
		f.log.Errorf("Post data to falcon failed: %s", err.Error())
		return err
	}
	defer resp.Body.Close()
	return nil
}

type Tags struct {
	tags map[string]string
}

func NewTags() *Tags {
	return &Tags{
		tags: make(map[string]string),
	}
}

func (t *Tags) SetTag(k, v string) {
	t.tags[k] = v
}

func (t *Tags) ToStr() string {
	var ts []string
	for k, v := range t.tags {
		ts = append(ts, k+"="+v)
	}
	return strings.Join(ts, ",")
}
