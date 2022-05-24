package main

import (
	"context"
	"flag"
	"net/http"

	_ "net/http/pprof"

	"github.com/apache/pulsar/pulsar-function-go/logutil"
	"github.com/apache/pulsar/pulsar-function-go/pf"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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

	prometheusHandle := &prometheus.PrometheusHandle{}

	go func(p *prometheus.PrometheusHandle) {
		http.Handle("/readness", p)
		http.Handle("/metrics", promhttp.Handler())
		err := http.ListenAndServe(":9494", nil)
		if err != nil {
			logutil.Fatal("start http server failed!", err)
		}
	}(prometheusHandle)

	go prometheusHandle.RunPrometheusFunction(context.TODO(), functionConfig, stopper)

	<-stopper

}
