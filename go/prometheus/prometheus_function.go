package main

import (
	"bytes"
	"context"
	"io/ioutil"
	"net/http"

	"github.com/apache/pulsar/pulsar-function-go/logutil"
	"github.com/apache/pulsar/pulsar-function-go/pf"
)

func remoteWriteHandler(ctx context.Context, in []byte) error {

	// if fc, ok := pf.FromContext(ctx); ok {
	// 	logutil.Info("function ID is:%s, ", fc.GetFuncID())
	// 	logutil.Info("function version is:%s\n", fc.GetFuncVersion())
	// }

	// todo
	// 2. access not compress byte data
	// 3. access remote.request protobuf object

	go doBytesPost(target, in)
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

func doBytesPost(url string, data []byte) ([]byte, error) {

	body := bytes.NewReader(data)
	request, err := http.NewRequest("POST", url, body)
	if err != nil {
		logutil.Errorf("http.NewRequest,[err=%s][url=%s]", err, url)
		return []byte(""), err
	}
	request.Header.Set("Connection", "Keep-Alive")
	var resp *http.Response
	resp, err = http.DefaultClient.Do(request)
	if err != nil {
		logutil.Errorf("http.Do failed,[err=%s][url=%s]", err, url)
		return []byte(""), err
	}
	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		logutil.Errorf("http.Do failed,[err=%s][url=%s]", err, url)
	}
	return b, err
}

func main() {
	pf.Start(remoteWriteHandler)
}
