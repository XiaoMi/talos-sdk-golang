# Installing

Install your specific service package with the following command.

1. GoPath

You can install the entire SDK to GoPath by go get:

`$ go get github.com/XiaoMi/talos-sdk-golang`

2. Go modules

Or add the module to your go.mod:

`go mod edit -require=github.com/XiaoMi/talos-sdk-golang@v0.2.12`

# Tutorial

We have an Tutorial Demo in package example/, here we choose talos_consumer to explain the use process

### import

To use SDK, import like this:

```
import (
	"github.com/XiaoMi/talos-sdk-golang/consumer"
	"github.com/XiaoMi/talos-sdk-golang/utils"
	log "github.com/sirupsen/logrus"
)

```

### Configuration profile

Reference talosConsumer.conf, you can configure like this:

```
galaxy.talos.service.endpoint=ENDPOINT

galaxy.talos.topic.name=MY-TOPIC

galaxy.talos.access.key=MY-APP-KEY

galaxy.talos.access.secret=MY-SECRET-KEY
```

**Notice**

* Before using the Talos SDK, ensure that you've correct credential and base information
* Other configurations can remain the default

### Coding

```
    // user can customize log format or use default format
    // log.SetOutput(os.Stdout)
    // log.SetLevel(log.InfoLevel)
    utils.InitLog()
    
    // init properties from talosConsumer.conf
    var propertyFilename string
    flag.StringVar(&propertyFilename, "conf", "talosProducer.conf", "conf: talosConsumer.conf'")
    flag.Parse()
    
    // constructor
    talosConsumer, err := consumer.NewTalosConsumerByFilename(propertyFilename, 
        NewMyMessageProcessorFactory(),
        client.NewSimpleTopicAbnormalCallback())
    if err != nil {
        log.Errorf("init talosConsumer failed: %s", err.Error())
        return
    }
    
    // shutdown, graceful exit
    go func() {
        time.Sleep(5 * time.Second)
        talosConsumer.ShutDown()
    }()
    
    // block main function and wait shutdown
    talosConsumer.WaitGroup.Wait()
```

### Go run & Go build

Then user can run this demo to fetch data

```
$ go run $path/TalosConsumerDemo.go [-config] [$path/talosConsumer.conf]
```
or 
```
$ cd example/talos_consumer
$ go build TalosConsumerDemo.go
$ ./TalosConsumerDemo [-config] [$path/talosConsumer.conf]
```

# Talos Book

  [Talos Wiki](http://docs.api.xiaomi.com/talos/index.html)
