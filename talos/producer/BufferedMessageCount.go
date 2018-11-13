/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package producer

import "sync"

var mutex sync.Mutex

type BufferedMessageCount struct {
	maxBufferedMsgNumber int64
	maxBufferedMsgBytes  int64
	bufferedMsgNumber    int64
	bufferedMsgBytes     int64
}

func NewBufferedMessageCount(maxBufferedMsgNumber, maxBufferedMsgBytes int64) *BufferedMessageCount {
	return &BufferedMessageCount{
		maxBufferedMsgNumber: maxBufferedMsgNumber,
		maxBufferedMsgBytes:  maxBufferedMsgBytes,
		bufferedMsgNumber:    0,
		bufferedMsgBytes:     0,
	}
}

func (c *BufferedMessageCount) GetBufferedMsgNumber() int64 {
	return c.bufferedMsgNumber
}

func (c *BufferedMessageCount) GetBufferedMsgBytes() int64 {
	return c.bufferedMsgBytes
}

func (c *BufferedMessageCount) Increase(diffBufferedMsgNumber, diffBufferedMsgBytes int64) {
	mutex.Lock()
	c.bufferedMsgNumber += diffBufferedMsgNumber
	c.bufferedMsgBytes += diffBufferedMsgBytes
	mutex.Unlock()
}

func (c *BufferedMessageCount) Decrease(diffBufferedMsgNumber, diffBufferedMsgBytes int64) {
	mutex.Lock()
	c.bufferedMsgNumber -= diffBufferedMsgNumber
	c.bufferedMsgBytes -= diffBufferedMsgBytes
	mutex.Unlock()
}

func (c *BufferedMessageCount) IsEmpty() bool {
	mutex.Lock()
	empty := c.bufferedMsgNumber == 0
	mutex.Unlock()
	return empty
}

func (c *BufferedMessageCount) IsFull() bool {
	mutex.Lock()
	full := c.bufferedMsgNumber >= c.maxBufferedMsgNumber ||
		c.bufferedMsgBytes >= c.maxBufferedMsgBytes
	mutex.Unlock()
	return full
}
