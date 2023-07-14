package pulsar

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/apache/pulsar/pulsar-function-go/logutil"
	"gopkg.in/yaml.v2"
)

func RunPulsarFunction(ctx context.Context, functionConfig string, stopchan chan struct{}) {
	conf, err := ReadYamlConfig(functionConfig)
	if err != nil {
		log.Fatal(err)
	}

	logutil.Debug("input pulsar conf:", conf.Config.InputPulsar.Url)
	logutil.Debug("output pulsar  conf:", conf.Config.OutputPulsar.Url)

	if !strings.HasPrefix(conf.Config.InputPulsar.Url, "pulsar://") || !strings.HasPrefix(conf.Config.OutputPulsar.Url, "pulsar://") {
		logutil.Errorf("unsupported pulsar protocol scheme %s,please use pulsar://", conf.Config.InputPulsar.Url)
		stopchan <- struct{}{}
	}

	intputClient := createClient(conf.Config.InputPulsar.Url)

	defer intputClient.Close()

	consumer, err := intputClient.Subscribe(pulsar.ConsumerOptions{
		Topic:            conf.Config.InputPulsar.Topic,
		SubscriptionName: conf.Config.InputPulsar.SubscriptionName,
		Type:             pulsar.Shared,
	})
	if err != nil {
		log.Fatalf("Could not create input Pulsar consumer: %v", err)
	}
	defer consumer.Close()

	outputClient := createClient(conf.Config.OutputPulsar.Url)

	defer outputClient.Close()

	producer, err := outputClient.CreateProducer(pulsar.ProducerOptions{
		Topic: conf.Config.OutputPulsar.Topic,
	})
	if err != nil {
		log.Fatalf("Could not create output Pulsar producer: %v", err)
	}
	defer producer.Close()

	for {

		msg, err := consumer.Receive(context.TODO())
		if err != nil {
			logutil.Error("receive message failed!", err)
			continue
		}
		_, err = producer.Send(context.TODO(), &pulsar.ProducerMessage{
			Payload:     msg.Payload(),
			Key:         msg.Key(),
			OrderingKey: msg.OrderingKey(),
			Properties:  msg.Properties(),
			EventTime:   msg.EventTime(),
		})
		if err != nil {
			logutil.Error("send message to output pulsar failed!", err)
			continue
		}
		consumer.Ack(msg)
	}

}

func createClient(brokerurl string) pulsar.Client {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:               brokerurl,
		OperationTimeout:  30 * time.Second,
		ConnectionTimeout: 30 * time.Second,
	})
	if err != nil {
		log.Fatalf("Could not instantiate Pulsar client: %v", err)
	}
	return client
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
	InputPulsar  Pulsar `yaml:"inputpulsar"`
	OutputPulsar Pulsar `yaml:"outputpulsar"`
}

type Pulsar struct {
	Url              string `yaml:"url"`
	Topic            string `yaml:"topic"`
	SubscriptionName string `yaml:"subscriptionName"`
}
