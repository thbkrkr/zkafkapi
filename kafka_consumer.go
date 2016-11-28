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

	consumersOffsetsMutex.RLock()

	consumers := []ConsumerGroup{}
	for topicName, consumersOffsets := range ConsumersTopicsOffsets {
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

	consumersOffsetsMutex.RUnlock()

	c.JSON(200, consumers)
}

func KafkaGetConsumer(c *gin.Context) {
	consumerName := c.Param("consumer")

	if IsNotAdmin(c) {
		return
	}

	consumersOffsetsMutex.RLock()

	consumers := []ConsumerGroup{}
	for topicName, consumersOffsets := range ConsumersTopicsOffsets {
		for name, offsets := range consumersOffsets {
			consumer := ConsumerGroup{
				Name:   name,
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

	consumersOffsetsMutex.RUnlock()

	var consumer *ConsumerGroup
	for _, c := range consumers {
		if c.Name == consumerName {
			consumer = &c
			break
		}
	}

	if consumer == nil {
		c.JSON(404, gin.H{"message": "consumer not found"})
		return
	}

	c.JSON(200, consumer)
}

func KafkaListAllConsumersOffsets(c *gin.Context) {
	if IsNotAdmin(c) {
		return
	}

	consumersOffsetsMutex.RLock()
	defer consumersOffsetsMutex.RUnlock()

	c.JSON(200, ConsumersTopicsOffsets)
}

func KafkaListTopicConsumerOffsets(c *gin.Context) {
	topic := c.Param("topic")
	consumerGroupID := c.Param("consumer")

	client := c.MustGet("kafkaClient").(sarama.Client)

	consumersOffsets, err := getKafkaConsumerOffsets(client, topic, consumerGroupID)
	if handlHTTPErr(c, err) {
		return
	}

	c.JSON(200, consumersOffsets)
}

func getKafkaConsumerOffsets(client sarama.Client, topic string, consumerGroupID string) (map[int32]interface{}, error) {
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
