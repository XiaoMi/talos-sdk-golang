/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package consumer

import (
	"github.com/sirupsen/logrus"
)

type TalosMessageReaderFactory struct {
}

func NewTalosMessageReaderFactory() *TalosMessageReaderFactory {
	return &TalosMessageReaderFactory{}
}

func (f *TalosMessageReaderFactory) CreateMessageReader(config *TalosConsumerConfig, logger *logrus.Logger) *TalosMessageReader {
	return NewTalosMessageReader(config, logger)
}
