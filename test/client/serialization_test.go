/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package client

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"reflect"

	"github.com/XiaoMi/talos-sdk-golang/thrift/message"

	"github.com/XiaoMi/talos-sdk-golang/utils"

	"github.com/XiaoMi/talos-sdk-golang/client/serialization"

	"github.com/stretchr/testify/assert"
)

func TestSerializeV3(t *testing.T) {
	buf := make([]byte, 0)
	buffer := bytes.NewBuffer(buf)
	messageType := message.MessageType_BINARY

	time1 := utils.CurrentTimeMills()
	partitionKey1 := "P1"
	sequenceNumber1 := "S1"
	msg1 := []byte("M1")
	message1 := &message.Message{CreateTimestamp: &time1, MessageType: &messageType,
		PartitionKey: &partitionKey1, SequenceNumber: &sequenceNumber1, Message: msg1}

	time2 := utils.CurrentTimeMills()
	partitionKey2 := "P2"
	msg2 := []byte("M2")
	message2 := &message.Message{CreateTimestamp: &time2, MessageType: &messageType,
		PartitionKey: &partitionKey2, Message: msg2}

	time3 := utils.CurrentTimeMills()
	sequenceNumber3 := "S3"
	msg3 := []byte("M3")
	message3 := &message.Message{CreateTimestamp: &time3, MessageType: &messageType,
		SequenceNumber: &sequenceNumber3, Message: msg3}

	time4 := utils.CurrentTimeMills()
	schemaFinagerPrint4 := "S4"
	msg4 := []byte("M4")
	message4 := &message.Message{CreateTimestamp: &time4, MessageType: &messageType,
		SchemaFingerprint: &schemaFinagerPrint4, Message: msg4}

	//wait for make sure CurrenTimeMills not equals
	time.Sleep(1 * time.Second)
	time5 := utils.CurrentTimeMills()
	schemaFinagerPrint5 := "S4"
	msg5 := []byte("M4")
	message5 := &message.Message{CreateTimestamp: &time5, MessageType: &messageType,
		SchemaFingerprint: &schemaFinagerPrint5, Message: msg5}

	serializer, _ := serialization.NewMessageSerializationFactory().GetMessageSerializer(serialization.V3)
	serializer.Serialize(message1, buffer)
	t.Logf("buffer size: %d\n", buffer.Len())

	serializer.Serialize(message2, buffer)
	t.Logf("buffer size: %d\n", buffer.Len())

	serializer.Serialize(message3, buffer)
	t.Logf("buffer size: %d\n", buffer.Len())

	serialization.NewMessageSerialization().SerializeMessage(message4, buffer, serialization.V3)
	t.Logf("buffer size: %d\n", buffer.Len())

	verifyMessage1, err := serialization.NewMessageSerialization().DeserializeMessage(buffer)
	verifyMessage2, err := serialization.NewMessageSerialization().DeserializeMessage(buffer)
	verifyMessage3, err := serialization.NewMessageSerialization().DeserializeMessage(buffer)
	verifyMessage4, err := serialization.NewMessageSerialization().DeserializeMessage(buffer)

	if err != nil {
		fmt.Println(err)
	}

	if !reflect.DeepEqual(verifyMessage1, message1) {
		t.Errorf("verify failed")
	}

	if !reflect.DeepEqual(verifyMessage2, message2) {
		t.Errorf("verify failed")
	}

	if !reflect.DeepEqual(verifyMessage3, message3) {
		t.Errorf("verify failed")
	}

	if !reflect.DeepEqual(verifyMessage4, message4) {
		t.Errorf("verify failed")
	}

	if reflect.DeepEqual(verifyMessage4, message5) {
		t.Errorf("verify failed")
	}

}

func TestSerializeV2(t *testing.T) {

	buf := make([]byte, 0)
	buffer := bytes.NewBuffer(buf)
	messageType := message.MessageType_BINARY

	time1 := utils.CurrentTimeMills()
	partitionKey1 := "P1"
	sequenceNumber1 := "S1"
	msg1 := []byte("M1")
	message1 := &message.Message{CreateTimestamp: &time1, MessageType: &messageType,
		PartitionKey: &partitionKey1, SequenceNumber: &sequenceNumber1, Message: msg1}

	time2 := utils.CurrentTimeMills()
	partitionKey2 := "P2"
	msg2 := []byte("M2")
	message2 := &message.Message{CreateTimestamp: &time2, MessageType: &messageType,
		PartitionKey: &partitionKey2, Message: msg2}

	time3 := utils.CurrentTimeMills()
	sequenceNumber3 := "S3"
	msg3 := []byte("M3")
	message3 := &message.Message{CreateTimestamp: &time3, MessageType: &messageType,
		SequenceNumber: &sequenceNumber3, Message: msg3}

	time4 := utils.CurrentTimeMills()
	schemaFinagerPrint4 := "S4"
	msg4 := []byte("M4")
	message4 := &message.Message{CreateTimestamp: &time4, MessageType: &messageType,
		SchemaFingerprint: &schemaFinagerPrint4, Message: msg4}

	serializer, _ := serialization.NewMessageSerializationFactory().GetMessageSerializer(serialization.V2)
	serializer.Serialize(message1, buffer)
	t.Logf("buffer size: %d\n", buffer.Len())

	serializer.Serialize(message2, buffer)
	t.Logf("buffer size: %d\n", buffer.Len())

	serializer.Serialize(message3, buffer)
	t.Logf("buffer size: %d\n", buffer.Len())

	serialization.NewMessageSerialization().SerializeMessage(message4, buffer, serialization.V2)
	t.Logf("buffer size: %d\n", buffer.Len())

	verifyMessage1, err := serialization.NewMessageSerialization().DeserializeMessage(buffer)
	verifyMessage2, err := serialization.NewMessageSerialization().DeserializeMessage(buffer)
	verifyMessage3, err := serialization.NewMessageSerialization().DeserializeMessage(buffer)
	verifyMessage4, err := serialization.NewMessageSerialization().DeserializeMessage(buffer)

	if err != nil {
		fmt.Println(err)
	}

	assert.False(t, verifyMessage1.IsSetPartitionKey())
	assert.Equal(t, message1.GetCreateTimestamp(), verifyMessage1.GetCreateTimestamp())
	assert.Equal(t, message.MessageType_BINARY, verifyMessage1.GetMessageType())
	assert.Equal(t, message1.GetSequenceNumber(), verifyMessage1.GetSequenceNumber())
	assert.Equal(t, message1.GetMessage(), verifyMessage1.GetMessage())

	assert.False(t, verifyMessage2.IsSetPartitionKey())
	assert.False(t, verifyMessage2.IsSetSequenceNumber())
	assert.Equal(t, message2.GetCreateTimestamp(), verifyMessage2.GetCreateTimestamp())
	assert.Equal(t, message.MessageType_BINARY, verifyMessage2.GetMessageType())
	assert.Equal(t, message2.GetMessage(), verifyMessage2.GetMessage())

	assert.False(t, verifyMessage3.IsSetPartitionKey())
	assert.Equal(t, message3.GetCreateTimestamp(), verifyMessage3.GetCreateTimestamp())
	assert.Equal(t, message.MessageType_BINARY, verifyMessage3.GetMessageType())
	assert.Equal(t, message3.GetSequenceNumber(), verifyMessage3.GetSequenceNumber())
	assert.Equal(t, message3.GetMessage(), verifyMessage3.GetMessage())

	assert.False(t, verifyMessage4.IsSetPartitionKey())
	assert.False(t, verifyMessage4.IsSetSequenceNumber())
	assert.Equal(t, message4.GetCreateTimestamp(), verifyMessage4.GetCreateTimestamp())
	assert.Equal(t, message.MessageType_BINARY, verifyMessage4.GetMessageType())
	assert.Equal(t, message4.GetMessage(), verifyMessage4.GetMessage())
}

func TestSerializeV1(t *testing.T) {
	buf := make([]byte, 0)
	buffer := bytes.NewBuffer(buf)
	messageType := message.MessageType_BINARY

	time1 := utils.CurrentTimeMills()
	partitionKey1 := "P1"
	sequenceNumber1 := "S1"
	msg1 := []byte("M1")
	message1 := &message.Message{CreateTimestamp: &time1, MessageType: &messageType,
		PartitionKey: &partitionKey1, SequenceNumber: &sequenceNumber1, Message: msg1}

	time2 := utils.CurrentTimeMills()
	partitionKey2 := "P2"
	msg2 := []byte("M2")
	message2 := &message.Message{CreateTimestamp: &time2, MessageType: &messageType,
		PartitionKey: &partitionKey2, Message: msg2}

	time3 := utils.CurrentTimeMills()
	sequenceNumber3 := "S3"
	msg3 := []byte("M3")
	message3 := &message.Message{CreateTimestamp: &time3, MessageType: &messageType,
		SequenceNumber: &sequenceNumber3, Message: msg3}

	time4 := utils.CurrentTimeMills()
	schemaFinagerPrint4 := "S4"
	msg4 := []byte("M4")
	message4 := &message.Message{CreateTimestamp: &time4, MessageType: &messageType,
		SchemaFingerprint: &schemaFinagerPrint4, Message: msg4}

	serializer, _ := serialization.NewMessageSerializationFactory().GetMessageSerializer(serialization.V1)
	serializer.Serialize(message1, buffer)
	t.Logf("buffer size: %d\n", buffer.Len())

	serializer.Serialize(message2, buffer)
	t.Logf("buffer size: %d\n", buffer.Len())

	serializer.Serialize(message3, buffer)
	t.Logf("buffer size: %d\n", buffer.Len())

	serialization.NewMessageSerialization().SerializeMessage(message4, buffer, serialization.V1)
	t.Logf("buffer size: %d\n", buffer.Len())

	verifyMessage1, err := serialization.NewMessageSerialization().DeserializeMessage(buffer)
	verifyMessage2, err := serialization.NewMessageSerialization().DeserializeMessage(buffer)
	verifyMessage3, err := serialization.NewMessageSerialization().DeserializeMessage(buffer)
	verifyMessage4, err := serialization.NewMessageSerialization().DeserializeMessage(buffer)

	if err != nil {
		fmt.Println(err)
	}

	assert.False(t, verifyMessage1.IsSetPartitionKey())
	assert.False(t, verifyMessage1.IsSetCreateTimestamp())
	assert.Equal(t, message1.GetSequenceNumber(), verifyMessage1.GetSequenceNumber())
	assert.Equal(t, message1.GetMessage(), verifyMessage1.GetMessage())

	assert.False(t, verifyMessage2.IsSetPartitionKey())
	assert.False(t, verifyMessage2.IsSetCreateTimestamp())
	assert.False(t, verifyMessage2.IsSetSequenceNumber())
	assert.Equal(t, message2.GetMessage(), verifyMessage2.GetMessage())

	assert.False(t, verifyMessage3.IsSetPartitionKey())
	assert.False(t, verifyMessage3.IsSetCreateTimestamp())
	assert.Equal(t, message3.GetSequenceNumber(), verifyMessage3.GetSequenceNumber())
	assert.Equal(t, message3.GetMessage(), verifyMessage3.GetMessage())

	assert.False(t, verifyMessage4.IsSetPartitionKey())
	assert.False(t, verifyMessage4.IsSetCreateTimestamp())
	assert.False(t, verifyMessage4.IsSetSequenceNumber())
	assert.Equal(t, message4.GetMessage(), verifyMessage4.GetMessage())
}
