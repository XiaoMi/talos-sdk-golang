/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package producer

import "sync"

type BufferedMessageCount struct {
	maxBufferedMsgNumber int64
	maxBufferedMsgBytes  int64
	bufferedMsgNumber    int64
	bufferedMsgBytes     int64
	mutex                sync.Mutex
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
	c.mutex.Lock()
	c.bufferedMsgNumber += diffBufferedMsgNumber
	c.bufferedMsgBytes += diffBufferedMsgBytes
	c.mutex.Unlock()
}

func (c *BufferedMessageCount) Decrease(diffBufferedMsgNumber, diffBufferedMsgBytes int64) {
	c.mutex.Lock()
	c.bufferedMsgNumber -= diffBufferedMsgNumber
	c.bufferedMsgBytes -= diffBufferedMsgBytes
	c.mutex.Unlock()
}

func (c *BufferedMessageCount) IsEmpty() bool {
	c.mutex.Lock()
	empty := c.bufferedMsgNumber == 0
	c.mutex.Unlock()
	return empty
}

func (c *BufferedMessageCount) IsFull() bool {
	c.mutex.Lock()
	full := c.bufferedMsgNumber >= c.maxBufferedMsgNumber ||
		c.bufferedMsgBytes >= c.maxBufferedMsgBytes
	c.mutex.Unlock()
	return full
}
