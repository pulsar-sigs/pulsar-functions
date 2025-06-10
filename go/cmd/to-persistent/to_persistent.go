package main

import (
	"context"

	"github.com/apache/pulsar/pulsar-function-go/logutil"
	"github.com/apache/pulsar/pulsar-function-go/pf"
)

func toPersistentHandler(ctx context.Context, in []byte) error {
	// if fc, ok := pf.FromContext(ctx); ok {
	// 	//TODO implement feature
	// 	topic := fc.GetCurrentRecord().Topic()
	// 	// logutil.Info("function ID is:%s, ", fc.GetFuncID())
	// 	// logutil.Info("function version is:%s\n", fc.GetFuncVersion())
	// }
	return nil
}

func init() {
	for k, v := range pf.GetUserConfMap() {
		logutil.Infof("userMap:%s,%s\n", k, v)
	}
}

func main() {
	pf.Start(toPersistentHandler)
}
