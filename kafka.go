package main

import (
	"fmt"
	"strings"
	"sync"

	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
	"github.com/gin-gonic/gin"
	"github.com/samuel/go-zookeeper/zk"
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

func kafkaURL(key string) string {
	url := conf.Broker
	if key == conf.AdminPassword {
		url = strings.Replace(url, KafkyPort, KafkaPort, -1)
	} else {
		url = strings.Replace(url, KafkaPort, KafkyPort, -1)
	}
	return url
}

func ListTopics(c *gin.Context) {
	client := c.MustGet("kafkaClient").(sarama.Client)

	err := client.RefreshMetadata([]string{}...)
	if handlHTTPErr(c, err) {
		return
	}

	topics, err := client.Topics()
	if handlHTTPErr(c, err) {
		return
	}

	c.JSON(200, topics)
}

type Topic struct {
	Name       string
	Partitions map[string]int64
}

func FullTopic(c *gin.Context) {
	client := c.MustGet("kafkaClient").(sarama.Client)
	zkConn := c.MustGet("zkConn").(*zk.Conn)
	topicName := c.Param("topic")

	chroot, err := ZkChroot(c)
	if handlHTTPErr(c, err) {
		return
	}

	zkTopic, err := getZkTopic(zkConn, chroot, topicName)
	if handlHTTPErr(c, err) {
		return
	}

	topic, err := getTopic(client, topicName)
	if handlHTTPErr(c, err) {
		return
	}

	c.JSON(200, gin.H{
		"Name":       topicName,
		"Partitions": zkTopic.Partitions,
		"Offsets":    topic.Partitions,
	})
}

func GetTopic(c *gin.Context) {
	client := c.MustGet("kafkaClient").(sarama.Client)
	topicName := c.Param("topic")

	topic, err := getTopic(client, topicName)
	if handlHTTPErr(c, err) {
		return
	}

	c.JSON(200, topic)
}

func getTopic(client sarama.Client, topicName string) (*Topic, error) {
	wg := &sync.WaitGroup{}
	mutex := &sync.Mutex{}
	topic := Topic{Name: topicName, Partitions: map[string]int64{}}

	err := client.RefreshMetadata(topicName)
	if err != nil {
		return nil, err
	}

	partitions, err := client.Partitions(topicName)
	if err != nil {
		return nil, err
	}

	wg.Add(len(partitions))
	for _, partitionID := range partitions {
		go func(p int32) {
			defer wg.Done()

			topicOffset, err := client.GetOffset(topicName, p, sarama.OffsetNewest)
			if err != nil {
				log.WithError(err).WithField("topic", topicName).Error("Fail to get offset")
				return
			}

			mutex.Lock()
			topic.Partitions[fmt.Sprintf("%d", p)] = topicOffset
			mutex.Unlock()

		}(partitionID)
	}
	wg.Wait()

	return &topic, nil
}
