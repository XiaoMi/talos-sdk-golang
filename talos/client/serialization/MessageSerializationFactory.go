/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package serialization

import "fmt"

type MessageSerializationFactory struct {
}

func NewMessageSerializationFactory() *MessageSerializationFactory {
	return &MessageSerializationFactory{}
}

func (f *MessageSerializationFactory) GetMessageSerializer(version MessageVersion) (MessageSerializer, error) {
	switch version {
	case V1:
		return NewMessageSerializerV1(), nil
	case V2:
		return NewMessageSerializerV2(), nil
	case V3:
		return NewMessageSerializerV3(), nil
	default:
		return nil, fmt.Errorf("unsupported message version: %d", version)
	}
}

func GetDefaultMessageVersion() MessageVersion {
	return V3
}
