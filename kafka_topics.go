package main

import (
	"fmt"
	"sync"

	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
	"github.com/gin-gonic/gin"
)

type KafkaTopic struct {
	Name       string
	Partitions map[string]int64
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

func GetTopicOffsets(c *gin.Context) {
	client := c.MustGet("kafkaClient").(sarama.Client)
	topicName := c.Param("topic")

	topic, err := getTopic(client, topicName)
	if handlHTTPErr(c, err) {
		return
	}

	c.JSON(200, topic)
}

func getTopic(client sarama.Client, topicName string) (*KafkaTopic, error) {
	wg := &sync.WaitGroup{}
	mutex := &sync.Mutex{}
	topic := KafkaTopic{Name: topicName, Partitions: map[string]int64{}}

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
