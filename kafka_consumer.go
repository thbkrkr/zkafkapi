package main

import (
	"sync"

	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
	"github.com/gin-gonic/gin"
)

type ConsumerGroup struct {
	Name   string
	Topics map[string][]int32
}

func KafkaListConsumers(c *gin.Context) {
	if IsNotAdmin(c) {
		return
	}

	c.MustGet("kafkaClient")

	consumersOffsetsMutex.RLock()
	co := ConsumersTopicsOffsets
	consumersOffsetsMutex.RUnlock()

	consumers := []ConsumerGroup{}
	for topicName, consumersOffsets := range co {
		for consumerName, offsets := range consumersOffsets {
			consumer := ConsumerGroup{
				Name:   consumerName,
				Topics: map[string][]int32{},
			}
			partitions := consumer.Topics[topicName]
			if partitions == nil {
				partitions = []int32{}
			}
			for partition, _ := range offsets {
				partitions = append(partitions, partition)
			}
			consumer.Topics[topicName] = partitions
			consumers = append(consumers, consumer)
		}
	}

	c.JSON(200, consumers)
}

func KafkaListConsumersOffsets(c *gin.Context) {
	if IsNotAdmin(c) {
		return
	}

	consumersOffsetsMutex.RLock()
	defer consumersOffsetsMutex.RUnlock()

	c.JSON(200, ConsumersTopicsOffsets)
}

func KafkaListConsumerOffsets(c *gin.Context) {
	client := c.MustGet("kafkaClient").(sarama.Client)
	topic := c.Param("topic")
	consumerGroupID := c.Param("consumer")

	consumersOffsets, err := getConsumerOffsets(client, topic, consumerGroupID)
	if handlHTTPErr(c, err) {
		return
	}

	c.JSON(200, consumersOffsets)
}

func getConsumerOffsets(client sarama.Client, topic string, consumerGroupID string) (map[int32]interface{}, error) {
	consumersOffsets := map[int32]interface{}{}
	wg := &sync.WaitGroup{}
	mutex := &sync.Mutex{}

	offsetManager, err := sarama.NewOffsetManagerFromClient(consumerGroupID, client)
	if err != nil {
		return nil, err
	}

	partitions, err := client.Partitions(topic)
	if err != nil {
		return nil, err
	}

	for _, p := range partitions {
		wg.Add(1)
		go func(partition int32) {
			defer wg.Done()

			brokerPartitionOffsetManager, err := offsetManager.ManagePartition(topic, partition)
			if err != nil {
				log.WithError(err).Fatal("Fail to manage partition")
			}
			defer brokerPartitionOffsetManager.AsyncClose()

			offset, _ := brokerPartitionOffsetManager.NextOffset()

			mutex.Lock()
			consumersOffsets[partition] = offset
			mutex.Unlock()

		}(p)
	}
	wg.Wait()

	return consumersOffsets, nil
}
