// Autogenerated by Thrift Compiler (0.9.2)
// DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING

package main

import (
	"flag"
	"fmt"
	"math"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"thrift/consumer"

	"github.com/XiaoMi/talos-sdk-golang/thrift/thrift"
)

func Usage() {
	fmt.Fprintln(os.Stderr, "Usage of ", os.Args[0], " [-h host:port] [-u url] [-f[ramed]] function [arg1 [arg2...]]:")
	flag.PrintDefaults()
	fmt.Fprintln(os.Stderr, "\nFunctions:")
	fmt.Fprintln(os.Stderr, "  LockPartitionResponse lockPartition(LockPartitionRequest request)")
	fmt.Fprintln(os.Stderr, "  LockWorkerResponse lockWorker(LockWorkerRequest request)")
	fmt.Fprintln(os.Stderr, "  TopicsLockWorkerResponse lockWorkerForTopics(TopicsLockWorkerRequest request)")
	fmt.Fprintln(os.Stderr, "  CheckRegisterResponse checkRegister(CheckRegisterRequest request)")
	fmt.Fprintln(os.Stderr, "  void unlockPartition(UnlockPartitionRequest request)")
	fmt.Fprintln(os.Stderr, "  RenewResponse renew(RenewRequest request)")
	fmt.Fprintln(os.Stderr, "  TopicsRenewResponse renewForTopics(TopicsRenewRequest request)")
	fmt.Fprintln(os.Stderr, "  UpdateOffsetResponse updateOffset(UpdateOffsetRequest request)")
	fmt.Fprintln(os.Stderr, "  QueryOffsetResponse queryOffset(QueryOffsetRequest request)")
	fmt.Fprintln(os.Stderr, "  QueryWorkerResponse queryWorker(QueryWorkerRequest request)")
	fmt.Fprintln(os.Stderr, "  TopicsQueryWorkerResponse queryWorkerForTopics(TopicsQueryWorkerRequest request)")
	fmt.Fprintln(os.Stderr, "  QueryOrgOffsetResponse queryOrgOffset(QueryOrgOffsetRequest request)")
	fmt.Fprintln(os.Stderr, "  GetWorkerIdResponse getWorkerId(GetWorkerIdRequest request)")
	fmt.Fprintln(os.Stderr, "  void deleteConsumerGroup(DeleteConsumerGroupRequest request)")
	fmt.Fprintln(os.Stderr, "  Version getServiceVersion()")
	fmt.Fprintln(os.Stderr, "  void validClientVersion(Version clientVersion)")
	fmt.Fprintln(os.Stderr)
	os.Exit(0)
}

func main() {
	flag.Usage = Usage
	var host string
	var port int
	var protocol string
	var urlString string
	var framed bool
	var useHttp bool
	var parsedUrl url.URL
	var trans thrift.TTransport
	_ = strconv.Atoi
	_ = math.Abs
	flag.Usage = Usage
	flag.StringVar(&host, "h", "localhost", "Specify host and port")
	flag.IntVar(&port, "p", 9090, "Specify port")
	flag.StringVar(&protocol, "P", "binary", "Specify the protocol (binary, compact, simplejson, json)")
	flag.StringVar(&urlString, "u", "", "Specify the url")
	flag.BoolVar(&framed, "framed", false, "Use framed transport")
	flag.BoolVar(&useHttp, "http", false, "Use http")
	flag.Parse()

	if len(urlString) > 0 {
		parsedUrl, err := url.Parse(urlString)
		if err != nil {
			fmt.Fprintln(os.Stderr, "Error parsing URL: ", err)
			flag.Usage()
		}
		host = parsedUrl.Host
		useHttp = len(parsedUrl.Scheme) <= 0 || parsedUrl.Scheme == "http"
	} else if useHttp {
		_, err := url.Parse(fmt.Sprint("http://", host, ":", port))
		if err != nil {
			fmt.Fprintln(os.Stderr, "Error parsing URL: ", err)
			flag.Usage()
		}
	}

	cmd := flag.Arg(0)
	var err error
	if useHttp {
		trans, err = thrift.NewTHttpClient(parsedUrl.String())
	} else {
		portStr := fmt.Sprint(port)
		if strings.Contains(host, ":") {
			host, portStr, err = net.SplitHostPort(host)
			if err != nil {
				fmt.Fprintln(os.Stderr, "error with host:", err)
				os.Exit(1)
			}
		}
		trans, err = thrift.NewTSocket(net.JoinHostPort(host, portStr))
		if err != nil {
			fmt.Fprintln(os.Stderr, "error resolving address:", err)
			os.Exit(1)
		}
		if framed {
			trans = thrift.NewTFramedTransport(trans)
		}
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error creating transport", err)
		os.Exit(1)
	}
	defer trans.Close()
	var protocolFactory thrift.TProtocolFactory
	switch protocol {
	case "compact":
		protocolFactory = thrift.NewTCompactProtocolFactory()
		break
	case "simplejson":
		protocolFactory = thrift.NewTSimpleJSONProtocolFactory()
		break
	case "json":
		protocolFactory = thrift.NewTJSONProtocolFactory()
		break
	case "binary", "":
		protocolFactory = thrift.NewTBinaryProtocolFactoryDefault()
		break
	default:
		fmt.Fprintln(os.Stderr, "Invalid protocol specified: ", protocol)
		Usage()
		os.Exit(1)
	}
	client := consumer.NewConsumerServiceClientFactory(trans, protocolFactory)
	if err := trans.Open(); err != nil {
		fmt.Fprintln(os.Stderr, "Error opening socket to ", host, ":", port, " ", err)
		os.Exit(1)
	}

	switch cmd {
	case "lockPartition":
		if flag.NArg()-1 != 1 {
			fmt.Fprintln(os.Stderr, "LockPartition requires 1 args")
			flag.Usage()
		}
		arg47 := flag.Arg(1)
		mbTrans48 := thrift.NewTMemoryBufferLen(len(arg47))
		defer mbTrans48.Close()
		_, err49 := mbTrans48.WriteString(arg47)
		if err49 != nil {
			Usage()
			return
		}
		factory50 := thrift.NewTSimpleJSONProtocolFactory()
		jsProt51 := factory50.GetProtocol(mbTrans48)
		argvalue0 := consumer.NewLockPartitionRequest()
		err52 := argvalue0.Read(jsProt51)
		if err52 != nil {
			Usage()
			return
		}
		value0 := argvalue0
		fmt.Print(client.LockPartition(value0))
		fmt.Print("\n")
		break
	case "lockWorker":
		if flag.NArg()-1 != 1 {
			fmt.Fprintln(os.Stderr, "LockWorker requires 1 args")
			flag.Usage()
		}
		arg53 := flag.Arg(1)
		mbTrans54 := thrift.NewTMemoryBufferLen(len(arg53))
		defer mbTrans54.Close()
		_, err55 := mbTrans54.WriteString(arg53)
		if err55 != nil {
			Usage()
			return
		}
		factory56 := thrift.NewTSimpleJSONProtocolFactory()
		jsProt57 := factory56.GetProtocol(mbTrans54)
		argvalue0 := consumer.NewLockWorkerRequest()
		err58 := argvalue0.Read(jsProt57)
		if err58 != nil {
			Usage()
			return
		}
		value0 := argvalue0
		fmt.Print(client.LockWorker(value0))
		fmt.Print("\n")
		break
	case "lockWorkerForTopics":
		if flag.NArg()-1 != 1 {
			fmt.Fprintln(os.Stderr, "LockWorkerForTopics requires 1 args")
			flag.Usage()
		}
		arg59 := flag.Arg(1)
		mbTrans60 := thrift.NewTMemoryBufferLen(len(arg59))
		defer mbTrans60.Close()
		_, err61 := mbTrans60.WriteString(arg59)
		if err61 != nil {
			Usage()
			return
		}
		factory62 := thrift.NewTSimpleJSONProtocolFactory()
		jsProt63 := factory62.GetProtocol(mbTrans60)
		argvalue0 := consumer.NewTopicsLockWorkerRequest()
		err64 := argvalue0.Read(jsProt63)
		if err64 != nil {
			Usage()
			return
		}
		value0 := argvalue0
		fmt.Print(client.LockWorkerForTopics(value0))
		fmt.Print("\n")
		break
	case "checkRegister":
		if flag.NArg()-1 != 1 {
			fmt.Fprintln(os.Stderr, "CheckRegister requires 1 args")
			flag.Usage()
		}
		arg65 := flag.Arg(1)
		mbTrans66 := thrift.NewTMemoryBufferLen(len(arg65))
		defer mbTrans66.Close()
		_, err67 := mbTrans66.WriteString(arg65)
		if err67 != nil {
			Usage()
			return
		}
		factory68 := thrift.NewTSimpleJSONProtocolFactory()
		jsProt69 := factory68.GetProtocol(mbTrans66)
		argvalue0 := consumer.NewCheckRegisterRequest()
		err70 := argvalue0.Read(jsProt69)
		if err70 != nil {
			Usage()
			return
		}
		value0 := argvalue0
		fmt.Print(client.CheckRegister(value0))
		fmt.Print("\n")
		break
	case "unlockPartition":
		if flag.NArg()-1 != 1 {
			fmt.Fprintln(os.Stderr, "UnlockPartition requires 1 args")
			flag.Usage()
		}
		arg71 := flag.Arg(1)
		mbTrans72 := thrift.NewTMemoryBufferLen(len(arg71))
		defer mbTrans72.Close()
		_, err73 := mbTrans72.WriteString(arg71)
		if err73 != nil {
			Usage()
			return
		}
		factory74 := thrift.NewTSimpleJSONProtocolFactory()
		jsProt75 := factory74.GetProtocol(mbTrans72)
		argvalue0 := consumer.NewUnlockPartitionRequest()
		err76 := argvalue0.Read(jsProt75)
		if err76 != nil {
			Usage()
			return
		}
		value0 := argvalue0
		fmt.Print(client.UnlockPartition(value0))
		fmt.Print("\n")
		break
	case "renew":
		if flag.NArg()-1 != 1 {
			fmt.Fprintln(os.Stderr, "Renew requires 1 args")
			flag.Usage()
		}
		arg77 := flag.Arg(1)
		mbTrans78 := thrift.NewTMemoryBufferLen(len(arg77))
		defer mbTrans78.Close()
		_, err79 := mbTrans78.WriteString(arg77)
		if err79 != nil {
			Usage()
			return
		}
		factory80 := thrift.NewTSimpleJSONProtocolFactory()
		jsProt81 := factory80.GetProtocol(mbTrans78)
		argvalue0 := consumer.NewRenewRequest()
		err82 := argvalue0.Read(jsProt81)
		if err82 != nil {
			Usage()
			return
		}
		value0 := argvalue0
		fmt.Print(client.Renew(value0))
		fmt.Print("\n")
		break
	case "renewForTopics":
		if flag.NArg()-1 != 1 {
			fmt.Fprintln(os.Stderr, "RenewForTopics requires 1 args")
			flag.Usage()
		}
		arg83 := flag.Arg(1)
		mbTrans84 := thrift.NewTMemoryBufferLen(len(arg83))
		defer mbTrans84.Close()
		_, err85 := mbTrans84.WriteString(arg83)
		if err85 != nil {
			Usage()
			return
		}
		factory86 := thrift.NewTSimpleJSONProtocolFactory()
		jsProt87 := factory86.GetProtocol(mbTrans84)
		argvalue0 := consumer.NewTopicsRenewRequest()
		err88 := argvalue0.Read(jsProt87)
		if err88 != nil {
			Usage()
			return
		}
		value0 := argvalue0
		fmt.Print(client.RenewForTopics(value0))
		fmt.Print("\n")
		break
	case "updateOffset":
		if flag.NArg()-1 != 1 {
			fmt.Fprintln(os.Stderr, "UpdateOffset requires 1 args")
			flag.Usage()
		}
		arg89 := flag.Arg(1)
		mbTrans90 := thrift.NewTMemoryBufferLen(len(arg89))
		defer mbTrans90.Close()
		_, err91 := mbTrans90.WriteString(arg89)
		if err91 != nil {
			Usage()
			return
		}
		factory92 := thrift.NewTSimpleJSONProtocolFactory()
		jsProt93 := factory92.GetProtocol(mbTrans90)
		argvalue0 := consumer.NewUpdateOffsetRequest()
		err94 := argvalue0.Read(jsProt93)
		if err94 != nil {
			Usage()
			return
		}
		value0 := argvalue0
		fmt.Print(client.UpdateOffset(value0))
		fmt.Print("\n")
		break
	case "queryOffset":
		if flag.NArg()-1 != 1 {
			fmt.Fprintln(os.Stderr, "QueryOffset requires 1 args")
			flag.Usage()
		}
		arg95 := flag.Arg(1)
		mbTrans96 := thrift.NewTMemoryBufferLen(len(arg95))
		defer mbTrans96.Close()
		_, err97 := mbTrans96.WriteString(arg95)
		if err97 != nil {
			Usage()
			return
		}
		factory98 := thrift.NewTSimpleJSONProtocolFactory()
		jsProt99 := factory98.GetProtocol(mbTrans96)
		argvalue0 := consumer.NewQueryOffsetRequest()
		err100 := argvalue0.Read(jsProt99)
		if err100 != nil {
			Usage()
			return
		}
		value0 := argvalue0
		fmt.Print(client.QueryOffset(value0))
		fmt.Print("\n")
		break
	case "queryWorker":
		if flag.NArg()-1 != 1 {
			fmt.Fprintln(os.Stderr, "QueryWorker requires 1 args")
			flag.Usage()
		}
		arg101 := flag.Arg(1)
		mbTrans102 := thrift.NewTMemoryBufferLen(len(arg101))
		defer mbTrans102.Close()
		_, err103 := mbTrans102.WriteString(arg101)
		if err103 != nil {
			Usage()
			return
		}
		factory104 := thrift.NewTSimpleJSONProtocolFactory()
		jsProt105 := factory104.GetProtocol(mbTrans102)
		argvalue0 := consumer.NewQueryWorkerRequest()
		err106 := argvalue0.Read(jsProt105)
		if err106 != nil {
			Usage()
			return
		}
		value0 := argvalue0
		fmt.Print(client.QueryWorker(value0))
		fmt.Print("\n")
		break
	case "queryWorkerForTopics":
		if flag.NArg()-1 != 1 {
			fmt.Fprintln(os.Stderr, "QueryWorkerForTopics requires 1 args")
			flag.Usage()
		}
		arg107 := flag.Arg(1)
		mbTrans108 := thrift.NewTMemoryBufferLen(len(arg107))
		defer mbTrans108.Close()
		_, err109 := mbTrans108.WriteString(arg107)
		if err109 != nil {
			Usage()
			return
		}
		factory110 := thrift.NewTSimpleJSONProtocolFactory()
		jsProt111 := factory110.GetProtocol(mbTrans108)
		argvalue0 := consumer.NewTopicsQueryWorkerRequest()
		err112 := argvalue0.Read(jsProt111)
		if err112 != nil {
			Usage()
			return
		}
		value0 := argvalue0
		fmt.Print(client.QueryWorkerForTopics(value0))
		fmt.Print("\n")
		break
	case "queryOrgOffset":
		if flag.NArg()-1 != 1 {
			fmt.Fprintln(os.Stderr, "QueryOrgOffset requires 1 args")
			flag.Usage()
		}
		arg113 := flag.Arg(1)
		mbTrans114 := thrift.NewTMemoryBufferLen(len(arg113))
		defer mbTrans114.Close()
		_, err115 := mbTrans114.WriteString(arg113)
		if err115 != nil {
			Usage()
			return
		}
		factory116 := thrift.NewTSimpleJSONProtocolFactory()
		jsProt117 := factory116.GetProtocol(mbTrans114)
		argvalue0 := consumer.NewQueryOrgOffsetRequest()
		err118 := argvalue0.Read(jsProt117)
		if err118 != nil {
			Usage()
			return
		}
		value0 := argvalue0
		fmt.Print(client.QueryOrgOffset(value0))
		fmt.Print("\n")
		break
	case "getWorkerId":
		if flag.NArg()-1 != 1 {
			fmt.Fprintln(os.Stderr, "GetWorkerId requires 1 args")
			flag.Usage()
		}
		arg119 := flag.Arg(1)
		mbTrans120 := thrift.NewTMemoryBufferLen(len(arg119))
		defer mbTrans120.Close()
		_, err121 := mbTrans120.WriteString(arg119)
		if err121 != nil {
			Usage()
			return
		}
		factory122 := thrift.NewTSimpleJSONProtocolFactory()
		jsProt123 := factory122.GetProtocol(mbTrans120)
		argvalue0 := consumer.NewGetWorkerIdRequest()
		err124 := argvalue0.Read(jsProt123)
		if err124 != nil {
			Usage()
			return
		}
		value0 := argvalue0
		fmt.Print(client.GetWorkerId(value0))
		fmt.Print("\n")
		break
	case "deleteConsumerGroup":
		if flag.NArg()-1 != 1 {
			fmt.Fprintln(os.Stderr, "DeleteConsumerGroup requires 1 args")
			flag.Usage()
		}
		arg125 := flag.Arg(1)
		mbTrans126 := thrift.NewTMemoryBufferLen(len(arg125))
		defer mbTrans126.Close()
		_, err127 := mbTrans126.WriteString(arg125)
		if err127 != nil {
			Usage()
			return
		}
		factory128 := thrift.NewTSimpleJSONProtocolFactory()
		jsProt129 := factory128.GetProtocol(mbTrans126)
		argvalue0 := consumer.NewDeleteConsumerGroupRequest()
		err130 := argvalue0.Read(jsProt129)
		if err130 != nil {
			Usage()
			return
		}
		value0 := argvalue0
		fmt.Print(client.DeleteConsumerGroup(value0))
		fmt.Print("\n")
		break
	case "getServiceVersion":
		if flag.NArg()-1 != 0 {
			fmt.Fprintln(os.Stderr, "GetServiceVersion requires 0 args")
			flag.Usage()
		}
		fmt.Print(client.GetServiceVersion())
		fmt.Print("\n")
		break
	case "validClientVersion":
		if flag.NArg()-1 != 1 {
			fmt.Fprintln(os.Stderr, "ValidClientVersion requires 1 args")
			flag.Usage()
		}
		arg131 := flag.Arg(1)
		mbTrans132 := thrift.NewTMemoryBufferLen(len(arg131))
		defer mbTrans132.Close()
		_, err133 := mbTrans132.WriteString(arg131)
		if err133 != nil {
			Usage()
			return
		}
		factory134 := thrift.NewTSimpleJSONProtocolFactory()
		jsProt135 := factory134.GetProtocol(mbTrans132)
		argvalue0 := consumer.NewVersion()
		err136 := argvalue0.Read(jsProt135)
		if err136 != nil {
			Usage()
			return
		}
		value0 := argvalue0
		fmt.Print(client.ValidClientVersion(value0))
		fmt.Print("\n")
		break
	case "":
		Usage()
		break
	default:
		fmt.Fprintln(os.Stderr, "Invalid function ", cmd)
	}
}
