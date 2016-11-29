package main

import (
	"fmt"
	"strings"
	"sync"

	"github.com/Shopify/sarama"
)

var (
	kafkaClients = map[string]sarama.Client{}
	kafkaLock    = sync.RWMutex{}
)

func KafkaClient(key string) (sarama.Client, error) {
	brokerURL := kafkaURL(key)

	kafkaLock.RLock()
	kafkaClient := kafkaClients[key]
	kafkaLock.RUnlock()

	if kafkaClient == nil {
		config := sarama.NewConfig()

		config.ClientID = key

		c, err := sarama.NewClient([]string{brokerURL}, config)
		if err != nil {
			return nil, err
		}

		kafkaClient = c
		kafkaLock.Lock()
		kafkaClients[key] = c
		kafkaLock.Unlock()
	}

	return kafkaClient, nil
}

func KafkaSSLSASLClient(user string, password string) (sarama.Client, error) {
	brokerURL := kafkaTLSURL()

	kafkaLock.RLock()
	kafkaClient := kafkaClients[user]
	kafkaLock.RUnlock()

	if kafkaClient == nil {
		config := sarama.NewConfig()

		fmt.Println(brokerURL)
		fmt.Println(user)
		fmt.Println(password)

		config.Net.TLS.Enable = true
		config.Net.SASL.Enable = true
		config.Net.SASL.User = user
		config.Net.SASL.Password = password

		c, err := sarama.NewClient([]string{brokerURL}, config)
		if err != nil {
			return nil, err
		}

		kafkaClient = c
		kafkaLock.Lock()
		kafkaClients[user] = c
		kafkaLock.Unlock()
	}

	return kafkaClient, nil
}

func kafkaURL(key string) string {
	url := conf.Broker
	if key == conf.AdminPassword {
		url = strings.Replace(url, KafkyPort, KafkaPort, -1)
	} else {
		url = strings.Replace(url, KafkaPort, KafkyPort, -1)
	}
	return url
}

func kafkaTLSURL() string {
	url := conf.Broker
	url = strings.Replace(url, KafkyPort, KafkaTLSPort, -1)
	url = strings.Replace(url, KafkaPort, KafkaTLSPort, -1)
	return url
}
