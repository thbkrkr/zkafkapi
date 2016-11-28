package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
	"github.com/gin-gonic/gin"
)

type KafkaTopic struct {
	Name       string
	Partitions map[string]int64
}

func KafkaListTopics(c *gin.Context) {
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

func KafkaGetTopicOffsets(c *gin.Context) {
	topicName := c.Param("topic")

	client := c.MustGet("kafkaClient").(sarama.Client)

	topic, err := getKafkaTopicOffsets(client, topicName)
	if handlHTTPErr(c, err) {
		return
	}

	c.JSON(200, topic)
}

func getKafkaTopicOffsets(client sarama.Client, topicName string) (*KafkaTopic, error) {
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

type PartitionOffset struct {
	Value     int64
	Timestamp int64
}

func KafkaListAllTopicsOffsets(c *gin.Context) {
	if IsNotAdmin(c) {
		return
	}

	client := c.MustGet("kafkaClient").(sarama.Client)

	offsets, err := getAllTopicsOffsets(client)
	if handlHTTPErr(c, err) {
		return
	}

	c.JSON(200, offsets)
}

func getAllTopicsOffsets(client sarama.Client) (map[string]map[int32]PartitionOffset, error) {
	topics, err := client.Topics()
	if err != nil {
		return nil, err
	}

	topicsPartitions := map[string]int{}
	for _, topic := range topics {
		partitions, _ := client.Partitions(topic)
		topicsPartitions[topic] = len(partitions)
	}

	requests := make(map[int32]*sarama.OffsetRequest)
	brokers := make(map[int32]*sarama.Broker)

	// Generate an OffsetRequest for each topic:partition and bucket it to the leader broker
	for topic, partitions := range topicsPartitions {
		for i := 0; i < partitions; i++ {
			broker, err := client.Leader(topic, int32(i))
			if err != nil {
				log.Errorf("Topic leader error on %s:%v: %v", topic, int32(i), err)
				return nil, err
			}
			if _, ok := requests[broker.ID()]; !ok {
				requests[broker.ID()] = &sarama.OffsetRequest{}
			}
			brokers[broker.ID()] = broker
			requests[broker.ID()].AddBlock(topic, int32(i), sarama.OffsetNewest, 1)
		}
	}

	wg := &sync.WaitGroup{}
	mutex := &sync.Mutex{}
	offsets := map[string]map[int32]PartitionOffset{}

	// Send out the OffsetRequest to each broker for all the partitions it is leader for
	// The results go to the offset storage module
	getBrokerTopicsOffsets := func(brokerID int32, request *sarama.OffsetRequest) {
		defer wg.Done()
		response, err := brokers[brokerID].GetAvailableOffsets(request)
		if err != nil {
			log.Errorf("Cannot get topics offsets from broker %v: %v", brokerID, err)
			_ = brokers[brokerID].Close()
			return
		}

		ts := time.Now().Unix() * 1000
		for topicName, partitions := range response.Blocks {
			for partition, offsetResponse := range partitions {
				if offsetResponse.Err != sarama.ErrNoError {
					log.Warnf("Error in OffsetResponse for %s:%v from broker %v: %s", topicName, partition, brokerID, offsetResponse.Err.Error())
					continue
				}
				mutex.Lock()

				topic := offsets[topicName]
				if topic == nil {
					topic = map[int32]PartitionOffset{}
				}

				topic[partition] = PartitionOffset{Value: offsetResponse.Offsets[0], Timestamp: ts}
				offsets[topicName] = topic

				mutex.Unlock()
			}
		}
	}

	for brokerID, request := range requests {
		wg.Add(1)
		go getBrokerTopicsOffsets(brokerID, request)
	}

	wg.Wait()

	return offsets, nil
}
