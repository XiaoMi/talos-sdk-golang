/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package client

import (
	"github.com/XiaoMi/talos-sdk-golang/talos/client/compression"
	"github.com/XiaoMi/talos-sdk-golang/talos/thrift/message"
	"github.com/XiaoMi/talos-sdk-golang/talos/utils"
	"reflect"
	"strconv"
	"testing"
	"time"
)

func setUp() []*message.Message {
	var messageList = make([]*message.Message, 0, 100)

	for i := 0; i < 100; i++ {
		msg := message.NewMessage()
		curTime := utils.CurrentTimeMills()
		time.Sleep(10 * time.Millisecond)
		partitionKey := strconv.FormatInt(int64(i), 10)
		mType := message.MessageType_BINARY
		msg.CreateTimestamp = &curTime
		msg.PartitionKey = &partitionKey
		msg.MessageType = &mType

		//set sequence number
		if i%2 == 0 {
			sequenceNumber := strconv.FormatInt(int64(i), 10)
			msg.SequenceNumber = &sequenceNumber
		} else {
			sequenceNumber := strconv.FormatInt(int64(i*200), 10)
			msg.SequenceNumber = &sequenceNumber
		}

		//set message data
		data := make([]byte, i)
		for ind := 0; ind < i; ind++ {
			data[ind] = byte(i)
		}
		msg.Message = data
		messageList = append(messageList, msg)
	}
	return messageList
}

func TestNonCompression(t *testing.T) {
	messageList := setUp()
	messageBlock, err := compression.Compress(messageList, message.MessageCompressionType_NONE)
	if err != nil || messageBlock.GetCompressionType() != message.MessageCompressionType_NONE ||
		int32(len(messageList)) != messageBlock.GetMessageNumber() {
		t.Errorf("compress type error")
	}

	startOffset := int64(1234)
	appendTimestamp := int64(1110000)
	unHandledMessageNumber := int64(117)

	messageBlock.StartMessageOffset = startOffset
	messageBlock.AppendTimestamp = &appendTimestamp
	t.Logf("CompressionType: None, message BlockSize: %v", messageBlock.GetMessageBlockSize())

	//verify message right
	verifyMessageList, err := compression.DoDecompress(messageBlock, unHandledMessageNumber)
	if len(verifyMessageList) != len(messageList) || err != nil {
		t.Errorf("decompress error: wrong size or unKnow")
	}

	for i := 0; i < len(messageList); i++ {
		msg := messageList[i]
		verifyMsg := verifyMessageList[i].GetMessage()
		if msg.GetPartitionKey() != verifyMsg.GetPartitionKey() ||
			msg.GetCreateTimestamp() != verifyMsg.GetCreateTimestamp() ||
			appendTimestamp != verifyMsg.GetAppendTimestamp() ||
			msg.GetSequenceNumber() != verifyMsg.GetSequenceNumber() ||
			!reflect.DeepEqual(msg.GetMessage(), verifyMsg.GetMessage()) ||
			(startOffset+int64(i)) != verifyMessageList[i].GetMessageOffset() ||
			verifyMessageList[i].GetUnHandledMessageNumber() != (unHandledMessageNumber+int64(len(messageList))-1-int64(i)) {
			t.Errorf("decompress message not equal to source message")
		}
	}
}

func TestSnappy(t *testing.T) {
	messageList := setUp()
	messageBlock, err := compression.Compress(messageList, message.MessageCompressionType_SNAPPY)
	if message.MessageCompressionType_SNAPPY != messageBlock.GetCompressionType() || err != nil {
		t.Errorf("compression type error")
	}
	if int32(len(messageList)) != messageBlock.GetMessageNumber() {
		t.Errorf("message number error")
	}

	startOffset := int64(1234)
	appendTimestamp := int64(1110000)
	unHandledMessageNumber := int64(117)

	messageBlock.StartMessageOffset = startOffset
	messageBlock.AppendTimestamp = &appendTimestamp
	t.Logf("CompressionType: Snappy, message BlockSize: %v", len(messageBlock.GetMessageBlock()))

	verifyMessageList, err := compression.DoDecompress(messageBlock, unHandledMessageNumber)
	if len(verifyMessageList) != len(messageList) || err != nil {
		t.Errorf("decompress error: wrong size or unKnow")
	}
	for i := 0; i < len(messageList); i++ {
		msg := messageList[i]
		verifyMsg := verifyMessageList[i].GetMessage()
		if msg.GetPartitionKey() != verifyMsg.GetPartitionKey() ||
			msg.GetCreateTimestamp() != verifyMsg.GetCreateTimestamp() ||
			appendTimestamp != verifyMsg.GetAppendTimestamp() ||
			msg.GetSequenceNumber() != verifyMsg.GetSequenceNumber() ||
			!reflect.DeepEqual(msg.GetMessage(), verifyMsg.GetMessage()) ||
			(startOffset+int64(i)) != verifyMessageList[i].GetMessageOffset() ||
			verifyMessageList[i].GetUnHandledMessageNumber() != (unHandledMessageNumber+int64(len(messageList))-1-int64(i)) {
			t.Errorf("decompress message not equal to source message")
		}
	}
}

func TestGzip(t *testing.T) {
	messageList := setUp()
	messageBlock, err := compression.Compress(messageList, message.MessageCompressionType_GZIP)
	if message.MessageCompressionType_GZIP != messageBlock.GetCompressionType() || err != nil {
		t.Errorf("compression type error")
	}
	if int32(len(messageList)) != messageBlock.GetMessageNumber() {
		t.Errorf("message number error")
	}

	startOffset := int64(1234)
	appendTimestamp := int64(1110000)
	unHandledMessageNumber := int64(117)

	messageBlock.StartMessageOffset = startOffset
	messageBlock.AppendTimestamp = &appendTimestamp
	t.Logf("CompressionType: Gzip, message BlockSize: %v", len(messageBlock.GetMessageBlock()))

	verifyMessageList, err := compression.DoDecompress(messageBlock, unHandledMessageNumber)
	if len(verifyMessageList) != len(messageList) || err != nil {
		t.Errorf("decompress error: wrong size or unKnow")
	}
	for i := 0; i < len(messageList); i++ {
		msg := messageList[i]
		verifyMsg := verifyMessageList[i].GetMessage()
		if msg.GetPartitionKey() != verifyMsg.GetPartitionKey() ||
			msg.GetCreateTimestamp() != verifyMsg.GetCreateTimestamp() ||
			appendTimestamp != verifyMsg.GetAppendTimestamp() ||
			msg.GetSequenceNumber() != verifyMsg.GetSequenceNumber() ||
			!reflect.DeepEqual(msg.GetMessage(), verifyMsg.GetMessage()) ||
			(startOffset+int64(i)) != verifyMessageList[i].GetMessageOffset() ||
			verifyMessageList[i].GetUnHandledMessageNumber() != (unHandledMessageNumber+int64(len(messageList))-1-int64(i)) {
			t.Errorf("decompress message not equal to source message")
		}
	}

}
