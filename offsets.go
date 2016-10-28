package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
	"github.com/gin-gonic/gin"
)

func Offsets(c *gin.Context) {
	offsets, err := getOffsets()
	if handlHTTPErr(c, err) {
		return
	}

	c.JSON(200, offsets)
}

type PartitionOffset struct {
	Topic               string
	Partition           int32
	Offset              int64
	Timestamp           int64
	TopicPartitionCount int
}

func getOffsets() (map[string]*PartitionOffset, error) {
	topicMap := map[string]int{}

	topics, err := client.Topics()
	if err != nil {
		return nil, err
	}
	for _, topic := range topics {
		partitions, _ := client.Partitions(topic)
		topicMap[topic] = len(partitions)
	}

	requests := make(map[int32]*sarama.OffsetRequest)
	brokers := make(map[int32]*sarama.Broker)

	// Generate an OffsetRequest for each topic:partition and bucket it to the leader broker
	for topic, partitions := range topicMap {
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

	// Send out the OffsetRequest to each broker for all the partitions it is leader for
	// The results go to the offset storage module
	var wg sync.WaitGroup
	var mutex = sync.Mutex{}
	offsets := map[string]*PartitionOffset{}

	getBrokerOffsets := func(brokerID int32, request *sarama.OffsetRequest) {
		defer wg.Done()
		response, err := brokers[brokerID].GetAvailableOffsets(request)
		if err != nil {
			log.Errorf("Cannot fetch offsets from broker %v: %v", brokerID, err)
			_ = brokers[brokerID].Close()
			return
		}
		ts := time.Now().Unix() * 1000
		for topic, partitions := range response.Blocks {
			for partition, offsetResponse := range partitions {
				if offsetResponse.Err != sarama.ErrNoError {
					log.Warnf("Error in OffsetResponse for %s:%v from broker %v: %s", topic, partition, brokerID, offsetResponse.Err.Error())
					continue
				}
				mutex.Lock()
				offsets[fmt.Sprintf("%s-%d", topic, partition)] = &PartitionOffset{
					Topic:               topic,
					Partition:           partition,
					Offset:              offsetResponse.Offsets[0],
					Timestamp:           ts,
					TopicPartitionCount: topicMap[topic],
				}
				mutex.Unlock()
			}
		}
	}

	for brokerID, request := range requests {
		wg.Add(1)
		go getBrokerOffsets(brokerID, request)
	}

	wg.Wait()

	return offsets, nil
}
