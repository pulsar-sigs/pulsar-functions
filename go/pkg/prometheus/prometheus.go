package prometheus

import (
	"bytes"
	"io/ioutil"
	"net/http"

	"github.com/apache/pulsar/pulsar-function-go/logutil"
)

func DoBytesPost(url string, data []byte) ([]byte, error) {

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
