/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package utils

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"os"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	"github.com/XiaoMi/talos-sdk-golang/thrift/auth"
	"github.com/XiaoMi/talos-sdk-golang/thrift/common"
	"github.com/XiaoMi/talos-sdk-golang/thrift/message"
	"github.com/XiaoMi/talos-sdk-golang/thrift/thrift"
	"github.com/XiaoMi/talos-sdk-golang/thrift/topic"
	"github.com/gofrs/uuid"
	"github.com/sirupsen/logrus"
)

/**
 * read properties for client / consumer / producer
 */

type Properties struct {
	props map[string]string
}

func NewProperties() *Properties {
	properties := Properties{props: make(map[string]string)}
	return &properties
}

func LoadProperties(filename string) *Properties {
	file, err := os.Open(filename)
	if err != nil {
		return nil
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	maps := make(map[string]string)
	for scanner.Scan() {
		reg := regexp.MustCompile("\\s*#.*")
		line := scanner.Text()
		if !reg.MatchString(line) {
			reg = regexp.MustCompile("\\s*\\w+\\s*=.*")
			if reg.MatchString(line) {
				arr := strings.SplitN(line, "=", 2)
				key := strings.Trim(arr[0], " ")
				value := strings.Trim(arr[1], " ")
				maps[key] = value
			}
		}
	}
	return &Properties{props: maps}
}

func (p *Properties) Get(key string) string {
	return p.props[key]
}

func (p *Properties) GetProperty(key string, defaultValue string) string {
	value, ok := p.props[key]
	if !ok {
		return defaultValue
	}
	return value
}

func (p *Properties) SetProperty(key, value string) *Properties {
	p.props[key] = value
	return p
}

/**
 * The format of valid resource name is: ownerId#topicName#UUID
 * Note the 'ownerId' may contains the symbol '#',
 * so return topicName parsing from the tail of resourceName.
 */
func GetTopicNameByResourceName(topicTalosResourceName string) (string, error) {
	itemList := strings.Split(topicTalosResourceName, "#")
	len := len(itemList)
	if len < 3 {
		err := fmt.Errorf("%s is not a valid topicTalosResourceName",
			topicTalosResourceName)
		return "", err
	}
	return itemList[len-2], nil
}

func CheckArgument(expr bool) error {
	if !expr {
		return fmt.Errorf("Illegal Argument! ")
	}
	return nil
}

func CheckArgumentWithErrorMsg(expr bool, errorMsg string) error {
	if !expr {
		return fmt.Errorf(errorMsg)
	}
	return nil
}

func CheckParameterRange(parameter string, value int64, min int64, max int64) error {
	if value < min || value > max {
		err := fmt.Errorf("%s should be in range [%d,%d], got: %d ",
			parameter, min, max, value)
		return err
	}
	return nil
}

func CheckTopicAndPartition(topicAndPartition *topic.TopicAndPartition) error {
	if strings.Contains(topicAndPartition.GetTopicName(),
		TALOS_CLOUD_TOPIC_NAME_DELIMITER) {
		return errors.New(
			"The topic name format in TopicAndPartition should not be: orgId/topicName. ")
	}
	return nil
}

func CheckNameValidity(str string) error {
	if str == "" || len(str) <= 0 {
		return nil
	}
	matchBody, _ := regexp.MatchString(TALOS_NAME_BODY_REGEX, str)
	matchWrongHead, _ := regexp.MatchString(TALOS_NAME_HEAD_REGEX, str)
	if !(matchBody && !matchWrongHead) || len(str) > 80 {
		return fmt.Errorf("invalid string: %s. Its only with regex set:"+
			" [a-zA-Z0-9_-] and cannot start with '_' or '-',"+
			" and must be less than 80 ", str)
	}
	return nil
}

func GenerateClientId() string {
	uuid, _ := uuid.NewV4()
	return fmt.Sprintf("%d%s", CurrentTimeMills(), uuid.String()[0:8])
}

func ClientIP4() ([]byte, error) {
	ipList, err := net.InterfaceAddrs()
	if err != nil {
		return nil, errors.New("unexpected IP address")
	}
	for _, addr := range ipList {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ip4 := ipnet.IP.To4(); ip4 != nil {
				return ip4, nil
			}
		}
	}
	return nil, errors.New("unknown IP address")
}

// CheckAndGenerateClientId example: 10_189_32_151-go172163819113877e88b3d
func CheckAndGenerateClientId(prefix string) (string, error) {
	err := CheckNameValidity(prefix)
	if err != nil {
		return "", err
	}
	ip, err := ClientIP4()
	if err != nil {
		return "", err
	}
	localIp := fmt.Sprintf("%d_%d_%d_%d", ip[0], ip[1], ip[2], ip[3])
	return localIp + "-" + prefix + GenerateClientId(), nil
}

func CheckStartOffsetValidity(startOffset int64) error {
	if startOffset >= 0 || startOffset == int64(message.MessageOffset_START_OFFSET) ||
		startOffset == int64(message.MessageOffset_LATEST_OFFSET) {
		return nil
	} else {
		//errCode := common.ErrorCode_UNEXPECTED_MESSAGE_OFFSET
		//err := fmt.Errorf("invalid startOffset: %d. It must be greater than "+
		//	"or equal to 0, or equal to Message_START_OFFSET/LATEST_OFFSET", startOffset)
		//return NewTalosRuntimeError(errCode, err)
		return fmt.Errorf("invalid startOffset: %d. It must be greater than "+
			"or equal to 0, or equal to Message_START_OFFSET/LATEST_OFFSET", startOffset)
	}
}

func GenerateRequestSequenceId(clientId string, requestId atomic.Value) (string, error) {
	err := CheckNameValidity(clientId)
	if err != nil {
		return "", err
	}
	sequenceId := fmt.Sprintf("%s%s%d", clientId,
		TALOS_IDENTIFIER_DELIMITER, requestId.Load().(int64))
	requestId.Store(requestId.Load().(int64) + 1)
	return sequenceId, nil
}

func CurrentTimeMills() int64 {
	return time.Now().UnixNano() / 1000000
}

func Serialize(msg *message.Message) ([]byte, error) {
	transport := thrift.NewTMemoryBufferLen(1024)
	protocol := thrift.NewTCompactProtocolFactory().GetProtocol(transport)
	serializer := &thrift.TSerializer{Transport: transport, Protocol: protocol}
	return serializer.Write(msg)
}

func Deserialize(bytes []byte) (*message.Message, error) {
	transport := thrift.NewTMemoryBufferLen(1024)
	protocol := thrift.NewTCompactProtocolFactory().GetProtocol(transport)
	deserializer := &thrift.TDeserializer{Transport: transport, Protocol: protocol}
	var msg = message.NewMessage()
	e := deserializer.Read(msg, bytes)
	return msg, e
}

func HashCode(value []rune) int {
	h := 0
	if len(value) > 0 {
		val := value
		for i := 0; i < len(value); i++ {
			h = 31*h + int(val[i])
		}
	}
	return h
}

func IsTopicNotExist(err error) bool {
	if talosError, ok := err.(*common.GalaxyTalosException); ok {
		return talosError.GetErrorCode() == common.ErrorCode_TOPIC_NOT_EXIST
	}
	return false
}

func IsPartitionNotServing(err error) bool {
	return strings.Contains(err.Error(), "partition not serving error")
}

func IsOffsetOutOfRange(err error) bool {
	return strings.Contains(err.Error(), "Message out range")
}

func UpdateMessage(msg *message.Message, messageType message.MessageType) {
	if !msg.IsSetCreateTimestamp() {
		msg.CreateTimestamp = thrift.Int64Ptr(CurrentTimeMills())
	}

	if !msg.IsSetMessageType() {
		msg.MessageType = &messageType
	}
}

func CheckMessagesValidity(msgList []*message.Message, maxSingleMsgBytes int64) error {
	totalSize := int64(0)
	for _, msg := range msgList {
		if err := CheckMessageValidity(msg, maxSingleMsgBytes); err != nil {
			return err
		}
		totalSize += int64(len(msg.GetMessage()))
	}

	if totalSize > maxSingleMsgBytes*2 {
		return fmt.Errorf("Total Messages byte must less than %v ",
			maxSingleMsgBytes*2)
	}
	return nil
}

func CheckMessageValidity(msg *message.Message, maxSingleMsgBytes int64) error {
	if err := CheckMessageLenValidity(msg, maxSingleMsgBytes); err != nil {
		return fmt.Errorf("Check MessageLength validity error: %s ", err.Error())
	}
	if err := CheckMessageSequenceNumberValidity(msg); err != nil {
		return fmt.Errorf("Check MessageSequenceNumber Validity error: %s ", err.Error())
	}
	if err := CheckMessageTypeValidity(msg); err != nil {
		return fmt.Errorf("Check MessageType Validity error: %s ", err.Error())
	}
	return nil
}

func CheckMessageLenValidity(msg *message.Message, maxSingleMsgBytes int64) error {
	if len(msg.Message) == 0 {
		return fmt.Errorf("Field \"message\" must be set. ")
	}
	data := msg.GetMessage()
	if int64(len(data)) > maxSingleMsgBytes ||
		len(data) < TALOS_SINGLE_MESSAGE_BYTES_MINIMAL {
		return fmt.Errorf("Data must be less than or equal to %v bytes, got bytes: %v ",
			maxSingleMsgBytes, len(data))
	}
	return nil
}

func CheckMessageSequenceNumberValidity(msg *message.Message) error {
	if !msg.IsSetSequenceNumber() {
		return nil
	}
	sequenceNumber := msg.GetSequenceNumber()
	if len(sequenceNumber) < TALOS_PARTITION_KEY_LENGTH_MINIMAL ||
		len(sequenceNumber) > TALOS_PARTITION_KEY_LENGTH_MAXIMAL {
		return fmt.Errorf("Invalid sequenceNumber which length must be at least %d "+
			"and at most %d, got %d ", TALOS_PARTITION_KEY_LENGTH_MINIMAL,
			TALOS_PARTITION_KEY_LENGTH_MAXIMAL, len(sequenceNumber))
	}
	return nil
}

func CheckMessageTypeValidity(msg *message.Message) error {
	if !msg.IsSetMessageType() {
		return fmt.Errorf("Filed \"messageType\" must be set. ")
	}
	return nil
}

func CheckAddSubResourceNameRequest(credential *auth.Credential,
	request *topic.AddSubResourceNameRequest) error {
	// check principal
	if strings.HasPrefix(credential.GetSecretKeyId(), TALOS_CLOUD_AK_PREFIX) {
		return fmt.Errorf("Only Developer principal can add subResourceName. ")
	}

	// check topic
	if strings.HasPrefix(request.GetTopicTalosResourceName().
		GetTopicTalosResourceName(), TALOS_CLOUD_ORG_PREFIX) {
		return fmt.Errorf(
			"The topic created by cloud-manager role can not add subResourceName. ")
	}

	// check orgId
	if !strings.HasPrefix(request.GetOrgId(), TALOS_CLOUD_ORG_PREFIX) {
		return fmt.Errorf("The orgId must starts with 'CL'. ")
	}

	// check teamId
	if !strings.HasPrefix(request.GetAdminTeamId(), TALOS_CLOUD_TEAM_PREFIX) {
		return fmt.Errorf("The teamId must starts with 'CI'. ")
	}
	return nil
}

func CheckNotNull(T interface{}) error {
	if T == nil {
		return fmt.Errorf("NullPointerException")
	}
	return nil
}

// The format of cloud topicName is: orgId/topicName
func CheckCloudTopicNameValidity(topicName string) error {
	if topicName == "" || len(topicName) == 0 {
		return fmt.Errorf("Illegal Argument! ")
	}
	items := strings.Split(topicName, TALOS_CLOUD_TOPIC_NAME_DELIMITER)
	//either 'xxx/xxx/'(split 2), '/xxx'(split 2) or 'xx//xx'(split 3) are invalid
	if len(items) != 2 || strings.HasSuffix(topicName,
		TALOS_CLOUD_TOPIC_NAME_DELIMITER) ||
		!strings.HasPrefix(topicName, TALOS_CLOUD_ORG_PREFIX) {
		return fmt.Errorf(
			"The format of topicName used by cloud-manager must be: orgId/topicName. ")
	}
	return nil
}

func CheckTopicName(topicName string) error {
	if strings.Contains(topicName, TALOS_CLOUD_TOPIC_NAME_DELIMITER) {
		return fmt.Errorf("The topicname format should not be: orgId/topicname! ")
	}
	return nil
}

func InitLogger() *logrus.Logger {
	return &logrus.Logger{
		Out: os.Stdout,
		Formatter: &logrus.TextFormatter{
			TimestampFormat: "2006-01-02 15:04:05.000",
			FullTimestamp:   true,
		},
		Hooks:        make(logrus.LevelHooks),
		Level:        logrus.InfoLevel,
		ExitFunc:     os.Exit,
		ReportCaller: false,
	}
}

func NewTDeserializer() *thrift.TDeserializer {
	transport := thrift.NewTMemoryBufferLen(1024)
	procotol := thrift.NewTCompactProtocol(transport)
	return &thrift.TDeserializer{
		Transport: transport,
		Protocol:  procotol,
	}
}

func GetClientIP() string {
	ifaces, err := net.Interfaces()
	if err != nil {
		return "127.0.0.1"
	}
	for _, iface := range ifaces {
		// interface down
		if iface.Flags & net.FlagUp == 0 {
			continue
		}
		// loopback interface
		if iface.Flags & net.FlagLoopback != 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			return "127.0.0.1"
		}
		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip == nil || ip.IsLoopback() {
				continue
			}
			ip = ip.To4()
			// not an ipv4 address
			if ip == nil {
				continue
			}
			return ip.String()
		}
	}
	return "127.0.0.1"
}

func GetClusterFromEndPoint(endPoint string) string {
	cluster := strings.ReplaceAll(endPoint, "http://", "")
	cluster = strings.ReplaceAll(cluster, "https://", "")
	cluster = strings.ReplaceAll(cluster, ".api.xiaomi.net", "")
	cluster = strings.ReplaceAll(cluster, ".api.xiaomi.com", "")
	return cluster
}
