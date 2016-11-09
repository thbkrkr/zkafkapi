package main

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
	"github.com/gin-gonic/gin"
)

func BrokersLogSize(c *gin.Context) {
	offsets, err := getBrokersLogSize(c)
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

func getBrokersLogSize(c *gin.Context) (map[string]*PartitionOffset, error) {
	client := c.MustGet("kafkaClient").(sarama.Client)

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

// --------

type ConsumerOffset struct {
	Topic         string
	Partition     int32
	Offset        int64
	Timestamp     int64
	ConsumerGroup string
}

func ConsumersOffsets(c *gin.Context) {
	chroot, err := ZkChroot(c)
	if err != nil {
		return
	}

	paths, err := childrenRecursiveInternal(zkClient, chroot+"/consumers", "")
	if handlHTTPErr(c, err) {
		return
	}

	offsets := map[string]map[string]map[string]interface{}{}
	var wg sync.WaitGroup
	var mutex = sync.Mutex{}

	for _, path := range paths {

		go func() {

			parts := strings.Split(path, "/")

			// Skip if it's not an 'offset path' (e.g.: "topic/offsets/consumerGroup/partition")
			if parts[1] != "offsets" || len(parts) != 4 {
				continue
			}

			topicID := parts[0]
			consumerGroupID := parts[2]
			partitionSID := parts[3]

			consumerGroup := offsets[consumerGroupID]
			if consumerGroup == nil {
				consumerGroup = map[string]map[string]interface{}{}
			}

			topic := consumerGroup[topicID]
			if topic == nil {
				topic = map[string]interface{}{}
			}

			data, _, err := zkClient.Get(chroot + "/consumers/" + path)
			if handlHTTPErr(c, err) {
				return
			}
			offsetNumber, err := strconv.ParseInt(string(data), 10, 32)
			if handlHTTPErr(c, err) {
				return
			}

			mutex.Lock()
			topic[partitionSID] = int32(offsetNumber)
			consumerGroup[topicID] = topic
			offsets[consumerGroupID] = consumerGroup
			mutex.Unlock()
		}
	}

	c.JSON(200, offsets)
}
