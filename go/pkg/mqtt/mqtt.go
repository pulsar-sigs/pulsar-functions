package mqtt

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"text/template"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/apache/pulsar/pulsar-function-go/logutil"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"gopkg.in/yaml.v2"
)

var (
	templ *template.Template
)

func RunMQTTFunction(ctx context.Context, functionConfig string, stopchan chan struct{}) {
	conf, err := ReadYamlConfig(functionConfig)
	if err != nil {
		log.Fatal(err)
	}
	if !strings.HasPrefix(conf.Config.Pulsar.Url, "pulsar://") {
		logutil.Errorf("unsupported pulsar protocol scheme %s,please use pulsar://", conf.Config.Pulsar.Url)
		stopchan <- struct{}{}
	}

	if conf.Config.Mqtt.TopicTemplate != "" {
		t := template.New("mqtt topic template")
		templ, err = t.Parse(conf.Config.Mqtt.TopicTemplate)
		if err != nil {
			logutil.Errorf("parse mqtt topic template failed", err)
			stopchan <- struct{}{}
			return
		}
	}

	opts := mqtt.NewClientOptions()
	opts.AddBroker(conf.Config.Mqtt.Url)
	opts.SetUsername(conf.Config.Mqtt.Username)
	opts.SetPassword(conf.Config.Mqtt.Password)
	opts.SetClientID(conf.Config.Mqtt.ClientId)
	opts.SetAutoReconnect(true)

	mqttClient := mqtt.NewClient(opts)
	for {
		logutil.Info("mqtt: connecting to broker")
		if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
			logutil.Errorf("mqtt: connecting to broker error, will retry in 2s: %s-->%s", token.Error(), conf.Config.Mqtt.Url)
			time.Sleep(2 * time.Second)
		} else {
			break
		}
	}
	logutil.Info("mqtt: connected to broker")

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

		topic := strings.ReplaceAll(msg.Topic(), "non-persistent://", "")
		topic = strings.ReplaceAll(topic, "persistent://", "")
		if conf.Config.Mqtt.TopicTemplate != "" {
			topicBuf := new(bytes.Buffer)
			err = templ.Execute(topicBuf, msg.Properties())
			if err != nil {
				logutil.Error("template execute topic failed!", err)
				continue
			}
			topic = topicBuf.String()
		}

		token := mqttClient.Publish(topic, 1, false, string(msg.Payload()))
		if token.Error() != nil {
			logutil.Error("push message to mqtt failed!", token.Error())
			continue
		}
		consumer.Ack(msg)
	}

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

type RootConfig struct {
	Config Config `yaml:"config"`
}

type Config struct {
	Pulsar Pulsar `yaml:"pulsar"`
	Mqtt   MQTT   `yaml:"mqtt"`
}

type Pulsar struct {
	Url              string `yaml:"url"`
	Topic            string `yaml:"topic"`
	SubscriptionName string `yaml:"subscriptionName"`
}

type MQTT struct {
	Url           string `yaml:"url"`
	Username      string `yaml:"username"`
	Password      string `yaml:"password"`
	ClientId      string `yaml:"clientId"`
	TopicTemplate string `yaml:"topicTemplate"`
}
