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
	"thrift/metric"

	"github.com/XiaoMi/talos-sdk-golang/thrift/thrift"
)

func Usage() {
	fmt.Fprintln(os.Stderr, "Usage of ", os.Args[0], " [-h host:port] [-u url] [-f[ramed]] function [arg1 [arg2...]]:")
	flag.PrintDefaults()
	fmt.Fprintln(os.Stderr, "\nFunctions:")
	fmt.Fprintln(os.Stderr, "  ListTopicsInfoResponse listTopics()")
	fmt.Fprintln(os.Stderr, "  ListTopicsInfoResponse listTopicsByOrgId(string orgId)")
	fmt.Fprintln(os.Stderr, "  QueryConsumerGroupResponse queryConsumerGroup(QueryConsumerGroupRequest request)")
	fmt.Fprintln(os.Stderr, "  GetTopicConsumeUnitResponse getTopicConsumeUnit(GetTopicConsumeUnitRequest request)")
	fmt.Fprintln(os.Stderr, "  QueryTopicConsumeUnitResponse queryTopicConsumeUnit(QueryTopicConsumeUnitRequest request)")
	fmt.Fprintln(os.Stderr, "  GetDataPreviewResponse getDataPreview(GetDataPreviewRequest request)")
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
	client := metric.NewMetricServiceClientFactory(trans, protocolFactory)
	if err := trans.Open(); err != nil {
		fmt.Fprintln(os.Stderr, "Error opening socket to ", host, ":", port, " ", err)
		os.Exit(1)
	}

	switch cmd {
	case "listTopics":
		if flag.NArg()-1 != 0 {
			fmt.Fprintln(os.Stderr, "ListTopics requires 0 args")
			flag.Usage()
		}
		fmt.Print(client.ListTopics())
		fmt.Print("\n")
		break
	case "listTopicsByOrgId":
		if flag.NArg()-1 != 1 {
			fmt.Fprintln(os.Stderr, "ListTopicsByOrgId requires 1 args")
			flag.Usage()
		}
		argvalue0 := flag.Arg(1)
		value0 := argvalue0
		fmt.Print(client.ListTopicsByOrgId(value0))
		fmt.Print("\n")
		break
	case "queryConsumerGroup":
		if flag.NArg()-1 != 1 {
			fmt.Fprintln(os.Stderr, "QueryConsumerGroup requires 1 args")
			flag.Usage()
		}
		arg23 := flag.Arg(1)
		mbTrans24 := thrift.NewTMemoryBufferLen(len(arg23))
		defer mbTrans24.Close()
		_, err25 := mbTrans24.WriteString(arg23)
		if err25 != nil {
			Usage()
			return
		}
		factory26 := thrift.NewTSimpleJSONProtocolFactory()
		jsProt27 := factory26.GetProtocol(mbTrans24)
		argvalue0 := metric.NewQueryConsumerGroupRequest()
		err28 := argvalue0.Read(jsProt27)
		if err28 != nil {
			Usage()
			return
		}
		value0 := argvalue0
		fmt.Print(client.QueryConsumerGroup(value0))
		fmt.Print("\n")
		break
	case "getTopicConsumeUnit":
		if flag.NArg()-1 != 1 {
			fmt.Fprintln(os.Stderr, "GetTopicConsumeUnit requires 1 args")
			flag.Usage()
		}
		arg29 := flag.Arg(1)
		mbTrans30 := thrift.NewTMemoryBufferLen(len(arg29))
		defer mbTrans30.Close()
		_, err31 := mbTrans30.WriteString(arg29)
		if err31 != nil {
			Usage()
			return
		}
		factory32 := thrift.NewTSimpleJSONProtocolFactory()
		jsProt33 := factory32.GetProtocol(mbTrans30)
		argvalue0 := metric.NewGetTopicConsumeUnitRequest()
		err34 := argvalue0.Read(jsProt33)
		if err34 != nil {
			Usage()
			return
		}
		value0 := argvalue0
		fmt.Print(client.GetTopicConsumeUnit(value0))
		fmt.Print("\n")
		break
	case "queryTopicConsumeUnit":
		if flag.NArg()-1 != 1 {
			fmt.Fprintln(os.Stderr, "QueryTopicConsumeUnit requires 1 args")
			flag.Usage()
		}
		arg35 := flag.Arg(1)
		mbTrans36 := thrift.NewTMemoryBufferLen(len(arg35))
		defer mbTrans36.Close()
		_, err37 := mbTrans36.WriteString(arg35)
		if err37 != nil {
			Usage()
			return
		}
		factory38 := thrift.NewTSimpleJSONProtocolFactory()
		jsProt39 := factory38.GetProtocol(mbTrans36)
		argvalue0 := metric.NewQueryTopicConsumeUnitRequest()
		err40 := argvalue0.Read(jsProt39)
		if err40 != nil {
			Usage()
			return
		}
		value0 := argvalue0
		fmt.Print(client.QueryTopicConsumeUnit(value0))
		fmt.Print("\n")
		break
	case "getDataPreview":
		if flag.NArg()-1 != 1 {
			fmt.Fprintln(os.Stderr, "GetDataPreview requires 1 args")
			flag.Usage()
		}
		arg41 := flag.Arg(1)
		mbTrans42 := thrift.NewTMemoryBufferLen(len(arg41))
		defer mbTrans42.Close()
		_, err43 := mbTrans42.WriteString(arg41)
		if err43 != nil {
			Usage()
			return
		}
		factory44 := thrift.NewTSimpleJSONProtocolFactory()
		jsProt45 := factory44.GetProtocol(mbTrans42)
		argvalue0 := metric.NewGetDataPreviewRequest()
		err46 := argvalue0.Read(jsProt45)
		if err46 != nil {
			Usage()
			return
		}
		value0 := argvalue0
		fmt.Print(client.GetDataPreview(value0))
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
		argvalue0 := metric.NewVersion()
		err52 := argvalue0.Read(jsProt51)
		if err52 != nil {
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