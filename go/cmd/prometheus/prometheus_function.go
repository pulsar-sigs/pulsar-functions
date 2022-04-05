package main

import (
	"context"
	"flag"

	"github.com/apache/pulsar/pulsar-function-go/logutil"
	"github.com/apache/pulsar/pulsar-function-go/pf"
	"github.com/pulsar-sigs/pulsar-functions/pkg/prometheus"
)

func remoteWriteHandler(ctx context.Context, in []byte) error {
	// if fc, ok := pf.FromContext(ctx); ok {
	// 	logutil.Info("function ID is:%s, ", fc.GetFuncID())
	// 	logutil.Info("function version is:%s\n", fc.GetFuncVersion())
	// }

	// todo access remote.request protobuf object
	_, err := prometheus.DoBytesPost(target, in)
	return err
}

var (
	target         string
	function       bool
	functionConfig string
)

func init() {

	flag.BoolVar(&function, "fun", true, "Is pulsar function")
	flag.StringVar(&functionConfig, "fun-config", "", "Config file for pulsar function")
	flag.Parse()

	target = ""

	if function {
		for k, v := range pf.GetUserConfMap() {
			logutil.Infof("userMap:%s,%s\n", k, v)
		}

		if remoteWriteTarget, ok := pf.GetUserConfMap()["target"]; !ok {
			logutil.Fatal("have not prometheus remote write config! please set userConfig for taget")
		} else {
			target = remoteWriteTarget.(string)
		}
	}
}

func main() {

	if function {
		logutil.Info("begin pf.Start")
		pf.Start(remoteWriteHandler)
		return
	}

	var stopper = make(chan struct{})

	logutil.Info("run normal mode")

	go prometheus.RunPrometheusFunction(context.TODO(), functionConfig, stopper)

	<-stopper

}
