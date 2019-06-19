# Installing

Install your specific service package with the following go get command.
You can install the entire SDK by installing the root package:

`$ go get github.com/XiaoMi/talos-sdk-golang`

# Configuring

Before using the Talos SDK, ensure that you've configured credential and base information.
You can get the necessary information by registering on [Xiaomi fusion cloud UI]().
To configure information, you may use codes like this:

```
topicName := "MY-TOPIC"
appKey := "MY-APP-KEY"
appSecret := "MY-SECRET-KEY"
userType := auth.UserType_APP_SECRET
credential := auth.Credential{&userType, &appKey, &appSecret}
```

Configure your log format and its level, log will output to current path by default.

# Usages

To use SDK, you can import like:

```
import (
	"github.com/XiaoMi/talos-sdk-golang/talos/client"
	"github.com/XiaoMi/talos-sdk-golang/talos/consumer"
	"github.com/XiaoMi/talos-sdk-golang/talos/thrift"
	"github.com/XiaoMi/talos-sdk-golang/talos/utils"
	"github.com/XiaoMi/talos-sdk-golang/thrift"
)

```

## Run producerDemo

We have an demo in example/simple_producer/TalosSimpleProducerDemo.go and example/talos_producer/TalosProducerDemo.go, users can run this demo.

* Configured:

simpleProducer.conf -> TalosSimpleProducerDemo.go

talosProducer.conf -> TalosProducerDemo.go

* Coding

```
    var propertyFilename string
	flag.StringVar(&propertyFilename, "conf", "talosProducer.conf", "conf: talosConsumer.conf'")
	flag.Parse()
    talosProducer, err := producer.NewTalosProducerByFilename(propertyFilename,
    		client.NewSimpleTopicAbnormalCallback(), new(MyMessageCallback))

    toPutMsgNumber := 8
    messageList := make([]*message.Message, 0)
    for i := 0; i < toPutMsgNumber; i++ {
    	messageStr := fmt.Sprintf("This message is a text string. messageId: %d", i)
    	msg := &message.Message{Message: []byte(messageStr)}
    	messageList = append(messageList, msg)
    }
    talosProducer.AddUserMessage(messageList)
```

* Then:

```
$ cd example/simple_producer [example/talos_producer]
$ go get
$ go build TalosSimpleProducerDemo.go [TalosConsumerDemo.go]
$ ./TalosSimpleProducerDemo [./TalosConsumerDemo]
```

## Run consumerDemo

We have an demo in example/simple_consumer/TalosSimpleConsumerDemo.go and example/talos_consumer/TalosConsumerDemo.go, users can run this demo.

* Configured:

simpleConsumer.conf -> TalosSimpleConsumerDemo.go

talosConsumer.conf -> TalosConsumerDemo.go

* Then:

```
$ cd example/simple_consumer [example/talos_consumer]
$ go get
$ go build TalosSimpleConsumerDemo.go [TalosConsumerDemo.go]
$ ./TalosSimpleConsumerDemo [TalosConsumerDemo]
```

# Talos Book

  [Talos Wiki](http://docs.api.xiaomi.com/talos/index.html)