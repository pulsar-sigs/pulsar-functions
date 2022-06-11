package main

import (
	"context"
	"flag"
	"log"

	"github.com/apache/pulsar/pulsar-function-go/logutil"
	"github.com/pulsar-sigs/pulsar-functions/pkg/mqtt"
)

var (
	functionConfig string
)

func main() {
	var stopper = make(chan struct{})

	flag.StringVar(&functionConfig, "fun-config", "", "Config file for pulsar function")
	flag.Parse()

	if functionConfig == "" {
		log.Fatalln("config is empty! please use --fun-config set it.")
	}

	logutil.Info("run normal mode")

	go mqtt.RunMQTTFunction(context.TODO(), functionConfig, stopper)

	<-stopper
}
