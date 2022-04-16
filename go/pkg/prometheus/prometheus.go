package prometheus

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/apache/pulsar/pulsar-function-go/logutil"
	"github.com/golang/snappy"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"
)

func DoBytesPost(url string, data []byte) ([]byte, error) {

	encodedata := snappy.Encode(nil, data)
	logutil.Info("encodedata.size:", len(encodedata))

	body := bytes.NewReader(encodedata)
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
	logutil.Info("push.prometheus status code:", resp.StatusCode)
	if resp.StatusCode != 200 && resp.StatusCode != 204 {
		return b, errors.New(string(b))
	}
	return b, err
}

func RunPrometheusFunction(ctx context.Context, functionConfig string, stopchan chan struct{}) {

	conf, err := ReadYamlConfig(functionConfig)
	if err != nil {
		log.Fatal(err)
	}

	logutil.Debug("conf:", conf.Config.Pulsar.Url)

	if !strings.HasPrefix(conf.Config.Pulsar.Url, "pulsar://") {
		logutil.Errorf("unsupported pulsar protocol scheme %s,please use pulsar://", conf.Config.Pulsar.Url)
		stopchan <- struct{}{}
	}

	if !strings.HasPrefix(conf.Config.Prometheus.Url, "http://") && !strings.HasPrefix(conf.Config.Prometheus.Url, "https://") {
		logutil.Errorf("unsupported prometheus url protocol scheme %s,please use http:// or https://", conf.Config.Prometheus.Url)
		stopchan <- struct{}{}
	}

	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:               conf.Config.Pulsar.Url,
		OperationTimeout:  30 * time.Second,
		ConnectionTimeout: 30 * time.Second,
	})
	if err != nil {
		log.Fatalf("Could not instantiate Pulsar client: %v", err)
	}

	defer client.Close()

	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            conf.Config.Pulsar.Topic,
		SubscriptionName: conf.Config.Pulsar.SubscriptionName,
		Type:             pulsar.Shared,
	})
	if err != nil {
		log.Fatalf("Could not create Pulsar consumer: %v", err)
	}
	defer consumer.Close()

	for {
		msg, err := consumer.Receive(context.TODO())
		if err != nil {
			logutil.Error("receive message failed!", err)
			continue
		}
		_, err = DoBytesPost(conf.Config.Prometheus.Url, msg.Payload())
		if err != nil {
			logutil.Error("remote write data to prometheus failed!", err)
			continue
		}
		consumer.Ack(msg)
	}
}

type RootConfig struct {
	Config Config `yaml:"config"`
}

type Config struct {
	Pulsar     Pulsar     `yaml:"pulsar"`
	Prometheus Prometheus `yaml:"prometheus"`
}

type Pulsar struct {
	Url              string `yaml:"url"`
	Topic            string `yaml:"topic"`
	SubscriptionName string `yaml:"subscriptionName"`
}

type Prometheus struct {
	Url string `yaml:"url"`
}

func ReadYamlConfig(path string) (*RootConfig, error) {
	conf := &RootConfig{}
	if f, err := os.Open(path); err != nil {
		return nil, err
	} else {
		yaml.NewDecoder(f).Decode(conf)
	}
	fmt.Println("conf: ", conf)
	return conf, nil
}
