# Installing

* Install your specific service package with the following go get command.
You can install the entire SDK by installing the root package:

`$ go get github.com/XiaoMi/talos-sdk-golang`

# Configuring

* Before using the Talos SDK, ensure that you've configured credential and base information.
You can get the necessary information by registering on [Xiaomi fusion cloud UI]().
To configure information, you may use codes like this:

```
topicName := "MY-TOPIC"
appKey := "MY-APP-KEY"
appSecret := "MY-SECRET-KEY"
userType := auth.UserType_APP_SECRET
credential := auth.Credential{&userType, &appKey, &appSecret}

* Configure your log format and its level, log will output to current path by default.

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

## Run consumer

We have an demo in example/TalosSimpleConsumerDemo.go and example/TalosConsumerDemo.go,
users can run this demo after base information configured:

```
$ cd example
$ go get
$ go run TalosConsumerDemo.go
```