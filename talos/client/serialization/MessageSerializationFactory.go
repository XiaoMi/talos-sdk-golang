/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package serialization

import "fmt"

type MessageVersion int

const (
	V1 MessageVersion = iota + 1 //1
	V2
	V3
)

type MessageSerializationFactory struct {
}

func NewMessageSerializationFactory() *MessageSerializationFactory {
	return &MessageSerializationFactory{}
}

func (f *MessageSerializationFactory) GetMessageSerializer(version MessageVersion) (MessageSerialer, error) {
	switch version {
	//case V1:
	//  return
	//case V2:
	//  return
	case V3:
		return MessageSerialer{version: V3}, nil
	default:
		err := fmt.Errorf("unsupported message version: %d", version)
		return MessageSerialer{}, err
	}
}
