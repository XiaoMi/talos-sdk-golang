/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package serialization

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/XiaoMi/talos-sdk-golang/thrift/message"
)

type MessageSerializer interface {

	/**
	 * TalosProducer serialize
	 */
	Serialize(message *message.Message, buf *bytes.Buffer) error

	/**
	 * TalosConsumer deserialize
	 */
	Deserialize(header []byte, buf *bytes.Buffer) (*message.Message, error)

	/**
	 * GetMessageSize
	 */
	GetMessageSize(message *message.Message) (int, error)
}

func DecodeMessageVersionNumber(header []byte) (MessageVersion, error) {
	//tansfer []byte to int
	if header[0] != 'V' {
		return V1, nil
	} else {
		var mVersion MessageVersion
		var err error
		version := binary.BigEndian.Uint16(header[1:3])
		switch version {
		case 1:
			mVersion = V1
		case 2:
			mVersion = V2
		case 3:
			mVersion = V3
		default:
			err = fmt.Errorf("unsupport message version: %v", string(header[2]))
		}
		return mVersion, err
	}
}

func WriteMessageVersion(version MessageVersion, buf *bytes.Buffer) {
	buf.WriteByte('V')
	//tansfer int to []byte
	versionBuffer := make([]byte, 2)
	binary.BigEndian.PutUint16(versionBuffer, uint16(version))
	buf.Write(versionBuffer)
	buf.WriteByte(0)
}
