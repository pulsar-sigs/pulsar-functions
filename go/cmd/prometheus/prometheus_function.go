package main

import (
	"context"
	"log"

	"github.com/apache/pulsar/pulsar-function-go/logutil"
	"github.com/apache/pulsar/pulsar-function-go/pf"
	"github.com/pulsar-sigs/pulsar-functions/pkg/prometheus"

	"github.com/spf13/cobra"
)

func remoteWriteHandler(ctx context.Context, in []byte) error {

	// if fc, ok := pf.FromContext(ctx); ok {
	// 	logutil.Info("function ID is:%s, ", fc.GetFuncID())
	// 	logutil.Info("function version is:%s\n", fc.GetFuncVersion())
	// }

	// todo
	// 2. access not compress byte data
	// 3. access remote.request protobuf object

	go prometheus.DoBytesPost(target, in)
	return nil
}

var target = ""

func init() {

	for k, v := range pf.GetUserConfMap() {
		logutil.Infof("userMap:%s,%s\n", k, v)
	}

	if remoteWriteTarget, ok := pf.GetUserConfMap()["target"]; !ok {
		logutil.Fatal("have not prometheus remote write config! please set userConfig for taget")
	} else {
		target = remoteWriteTarget.(string)
	}
}

var function bool

var rootCmd = &cobra.Command{
	Use:   "pulsar-functions",
	Short: "",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		log.Println("begin pf.Start")
		if function {
			pf.Start(remoteWriteHandler)
			return
		}
		log.Println("run normal mode")
	},
}

func main() {
	rootCmd.Flags().BoolVar(&function, "", true, "Is pulsar function")
	err := rootCmd.Execute()
	if err != nil {
		panic(err)
	}
}
