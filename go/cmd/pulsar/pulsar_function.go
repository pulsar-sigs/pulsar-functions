package main

import (
	"context"
	"flag"

	"github.com/apache/pulsar/pulsar-function-go/logutil"
	"github.com/apache/pulsar/pulsar-function-go/pf"
	"github.com/pulsar-sigs/pulsar-functions/pkg/pulsar"
)

var (
	target         string
	function       bool
	functionConfig string
)

func init() {

	flag.BoolVar(&function, "fun", false, "Is pulsar function")
	flag.StringVar(&functionConfig, "fun-config", "", "Config file for pulsar function")
	flag.Parse()

	target = ""

	if function {
		for k, v := range pf.GetUserConfMap() {
			logutil.Infof("userMap:%s,%s\n", k, v)
		}
	}
}

func main() {

	if function {
		logutil.Info("pf is not impl")
		return
	}

	var stopper = make(chan struct{})

	logutil.Info("run normal mode")

	go pulsar.RunPulsarFunction(context.TODO(), functionConfig, stopper)

	<-stopper

}
