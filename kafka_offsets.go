package main

import (
	"sync"
	"time"

	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
	"github.com/gin-gonic/gin"
)

type Offset struct {
	Value     int64
	Timestamp int64
}

func KafkaListTopicsOffsets(c *gin.Context) {
	if IsNotAdmin(c) {
		return
	}

	client := c.MustGet("kafkaClient").(sarama.Client)

	offsets, err := getTopicsOffsets(client)
	if handlHTTPErr(c, err) {
		return
	}

	c.JSON(200, offsets)
}

func getTopicsOffsets(client sarama.Client) (map[string]map[int32]Offset, error) {
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
	offsets := map[string]map[int32]Offset{}

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
					topic = map[int32]Offset{}
				}

				topic[partition] = Offset{Value: offsetResponse.Offsets[0], Timestamp: ts}
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
