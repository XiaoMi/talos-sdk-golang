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

	"github.com/XiaoMi/talos-sdk-golang/talos/thrift/message"
	"github.com/XiaoMi/talos-sdk-golang/thrift"

	log "github.com/alecthomas/log4go"
)

const (
	MESSAGE_HEADER_BYTES  int = 8
	VERSION_NUMBER_LENGTH int = 4
)

type MessageSerialer struct {
	version MessageVersion
}

func DecodeMessageVersionNumber(header []byte) (MessageVersion, error) {
	//tansfer []byte to int
	version := binary.BigEndian.Uint16(header[1:3])
	if header[0] != 'V' {
		return V1, nil
	} else {
		var mVersion MessageVersion
		var err error
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

func (s *MessageSerialer) Serialize(message *message.Message, buf *bytes.Buffer) error {
	// write version number;
	WriteMessageVersion(V3, buf)

	// serialize message;
	transport := thrift.NewTMemoryBufferLen(1024)
	protocol := thrift.NewTCompactProtocolFactory().GetProtocol(transport)
	serializer := &thrift.TSerializer{Transport: transport, Protocol: protocol}
	messageData, err := serializer.Write(message)
	if err != nil {
		return err
	}
	//write message size
	sizeBuffer := make([]byte, 4)
	binary.BigEndian.PutUint32(sizeBuffer, uint32(len(messageData)))
	buf.Write(sizeBuffer)
	//write message
	buf.Write(messageData)
	return nil
}

func (s *MessageSerialer) Deserialize(header []byte, buf *bytes.Buffer) (*message.Message, error) {
	//read message size
	messageSizeBuffer := make([]byte, 4)
	n, err := buf.Read(messageSizeBuffer)
	if n != 4 || err != nil {
		log.Error("read message size error: %s", err.Error())
		return nil, err
	}
	messageSize := binary.BigEndian.Uint32(messageSizeBuffer)
	//read message
	var messageData = make([]byte, messageSize)
	buf.Read(messageData)
	transport := thrift.NewTMemoryBufferLen(1024)
	protocol := thrift.NewTCompactProtocolFactory().GetProtocol(transport)
	deserializer := &thrift.TDeserializer{Transport: transport, Protocol: protocol}
	var msg = message.NewMessage()
	e := deserializer.Read(msg, messageData)
	if e != nil {
		log.Error("read message data error: %s", e.Error())
		return nil, e
	}
	return msg, nil
}

func (s *MessageSerialer) GetMessageSize(message *message.Message) (int, error) {
	transport := thrift.NewTMemoryBufferLen(1024)
	protocol := thrift.NewTCompactProtocolFactory().GetProtocol(transport)
	serializer := &thrift.TSerializer{Transport: transport, Protocol: protocol}
	messageData, err := serializer.Write(message)
	if err != nil {
		log.Error("serialize messageData error: %s", err.Error())
		return 0, err
	}
	return MESSAGE_HEADER_BYTES + len(messageData), nil
}
