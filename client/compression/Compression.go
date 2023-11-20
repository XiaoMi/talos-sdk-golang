/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package compression

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io/ioutil"

	"github.com/XiaoMi/talos-sdk-golang/client/serialization"
	"github.com/XiaoMi/talos-sdk-golang/thrift/message"
	"github.com/XiaoMi/talos-sdk-golang/utils"

	"github.com/DataDog/zstd"
	"github.com/bkaradzic/go-lz4"
	xSnappy "github.com/eapache/go-xerial-snappy"
)

func Compress(messageList []*message.Message,
	compressionType message.MessageCompressionType) (*message.MessageBlock, error) {
	return DoCompress(messageList, compressionType,
		serialization.GetDefaultMessageVersion())
}

func DoCompress(messageList []*message.Message,
	compressionType message.MessageCompressionType,
	messageVersion serialization.MessageVersion) (*message.MessageBlock, error) {

	messageBlock := message.NewMessageBlock()
	messageBlock.CompressionType = compressionType
	messageBlock.MessageNumber = int32(len(messageList))

	createTime := utils.CurrentTimeMills()
	if len(messageList) > 0 {
		createTime = messageList[0].GetCreateTimestamp()
	}

	//assume the message bytes size is 256KB
	messageBlockData := make([]byte, 0, 256*1024)
	messageByteSlice := make([]byte, 0, 256*1024)
	messageSerializedBuffer := bytes.NewBuffer(messageByteSlice)
	for index, msg := range messageList {
		err := serialization.NewMessageSerialization().SerializeMessage(msg,
			messageSerializedBuffer, messageVersion)
		if err != nil {
			return nil, err
		}
		createTime += (msg.GetCreateTimestamp() - createTime) / int64(index+1)
	}
	switch compressionType {
	case message.MessageCompressionType_NONE:
		messageBlockData = append(messageBlockData, messageSerializedBuffer.Bytes()...)
	case message.MessageCompressionType_SNAPPY:
		compressedMessage := xSnappy.EncodeStream(nil, messageSerializedBuffer.Bytes())
		messageBlockData = append(messageBlockData, compressedMessage...)
	case message.MessageCompressionType_GZIP:
		gzipBuf := bytes.NewBuffer(make([]byte, 0))
		writer := gzip.NewWriter(gzipBuf)
		writer.Write(messageSerializedBuffer.Bytes())
		writer.Close()
		messageBlockData = append(messageBlockData, gzipBuf.Bytes()...)
	case message.MessageCompressionType_ZSTD:
		result, err := zstd.Compress(nil, messageSerializedBuffer.Bytes())
		if err != nil {
			return nil, err
		}
		messageBlockData = append(messageBlockData, result...)
	case message.MessageCompressionType_LZ4:
		compressed := make([]byte, 0)
		compressed, err := lz4.Encode(nil, messageSerializedBuffer.Bytes())
		if err != nil {
			return nil, err
		}
		messageBlockData = append(messageBlockData, compressed...)
	default:
		err := fmt.Errorf("unsupport compression type")
		return nil, err
	}

	if len(messageBlockData) > utils.TALOS_MESSAGE_BLOCK_BYTES_MAXIMAL {
		err := fmt.Errorf("MessageBlock must be less than %d bytes, got bytes: %d",
			utils.TALOS_MESSAGE_BLOCK_BYTES_MAXIMAL, len(messageBlockData))
		return nil, err
	}
	size := int32(len(messageBlockData))
	messageBlock.CreateTimestamp = &createTime
	messageBlock.MessageBlock = messageBlockData
	messageBlock.MessageBlockSize = &size

	return messageBlock, nil
}

func Decompress(messageBlockList []*message.MessageBlock,
	unhandledMessageNumber int64) ([]*message.MessageAndOffset, error) {

	messageAndOffsetList := make([]*message.MessageAndOffset, 0)
	unhandledNumber := unhandledMessageNumber
	for i := len(messageBlockList) - 1; i >= 0; i-- {
		list, err := DoDecompress(messageBlockList[i], unhandledNumber)
		if err != nil {
			return nil, err
		}
		unhandledNumber += int64(len(list))
		messageAndOffsetList = append(list, messageAndOffsetList...)
	}
	return messageAndOffsetList, nil
}

func DoDecompress(messageBlock *message.MessageBlock,
	unhandledNumber int64) ([]*message.MessageAndOffset, error) {

	messageNumber := messageBlock.GetMessageNumber()
	messageAndOffsetList := make([]*message.MessageAndOffset, 0, messageNumber)

	var messageBlockData *bytes.Buffer
	switch messageBlock.CompressionType {
	case message.MessageCompressionType_NONE:
		messageBlockData = bytes.NewBuffer(messageBlock.GetMessageBlock())
	case message.MessageCompressionType_SNAPPY:
		messageByteSlice, err := xSnappy.DecodeInto(nil, messageBlock.GetMessageBlock())
		if err != nil {
			return nil, err
		}
		messageBlockData = bytes.NewBuffer(messageByteSlice)
	case message.MessageCompressionType_GZIP:
		reader, _ := gzip.NewReader(bytes.NewBuffer(messageBlock.GetMessageBlock()))
		messageByteSlice, err := ioutil.ReadAll(reader)
		defer reader.Close()
		if err != nil {
			return nil, err
		}
		messageBlockData = bytes.NewBuffer(messageByteSlice)
	case message.MessageCompressionType_ZSTD:
		result, err := zstd.Decompress(nil, messageBlock.GetMessageBlock())
		if err != nil {
			return nil, err
		}
		messageBlockData = bytes.NewBuffer(result)
	case message.MessageCompressionType_LZ4:
		decompressed := make([]byte, 0)
		decompressed, err := lz4.Decode(nil, messageBlock.GetMessageBlock())
		if err != nil {
			return nil, err
		}
		messageBlockData = bytes.NewBuffer(decompressed)
	}

	for i := int32(0); i < messageNumber; i++ {
		messageAndOffset := message.NewMessageAndOffset()
		messageAndOffset.MessageOffset = messageBlock.GetStartMessageOffset() + int64(i)
		msg, err := serialization.NewMessageSerialization().
			DeserializeMessage(messageBlockData)
		if err != nil {
			return nil, err
		}
		if messageBlock.IsSetAppendTimestamp() {
			time := messageBlock.GetAppendTimestamp()
			msg.AppendTimestamp = &time
		}
		messageAndOffset.Message = msg
		num := unhandledNumber + int64(messageNumber) - 1 - int64(i)
		messageAndOffset.UnHandledMessageNumber = &num

		messageAndOffsetList = append(messageAndOffsetList, messageAndOffset)
	}

	return messageAndOffsetList, nil
}
