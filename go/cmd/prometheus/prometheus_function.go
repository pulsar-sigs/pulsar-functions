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

var (
	target         string
	function       bool
	functionConfig string
	p              *prometheus.PrometheusHandle
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

	p = &prometheus.PrometheusHandle{
		HTTPClient: http.DefaultClient,
	}
	p.Target = target

}

func main() {

	if function {
		logutil.Info("begin pf.Start")
		pf.Start(p.RemoteWriteHandler)
		return
	}

	var stopper = make(chan struct{})

	logutil.Info("run normal mode")

	go func(p *prometheus.PrometheusHandle) {
		http.Handle("/readness", p)
		http.Handle("/metrics", promhttp.Handler())
		err := http.ListenAndServe(":9494", nil)
		if err != nil {
			logutil.Fatal("start http server failed!", err)
		}
	}(p)

	go p.RunPrometheusFunction(context.TODO(), functionConfig, stopper)

	<-stopper

}
