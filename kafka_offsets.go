package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
	"github.com/gin-gonic/gin"
	"github.com/linkedin/Burrow/protocol"
)

func KafkaTopicsOffsets(c *gin.Context) {
	client := c.MustGet("kafkaClient").(sarama.Client)

	topicsOffsets, err := getTopicsOffsets(client)
	if handlHTTPErr(c, err) {
		return
	}

	c.JSON(200, topicsOffsets)
}

//

var (
	topicsOffsets      = map[string]map[int32]int64{}
	topicsOffsetsMutex = &sync.RWMutex{}
)

type BrokerPartitionOffset struct {
	Topic               string
	Partition           int32
	Offset              int64
	Timestamp           int64
	TopicPartitionCount int
}

func getTopicsOffsets(client sarama.Client) (map[string]map[int32]int64, error) {
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
	xtopicsOffsets := map[string]map[int32]int64{}

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

		//ts := time.Now().Unix() * 1000
		for topicName, partitions := range response.Blocks {
			for partition, offsetResponse := range partitions {
				if offsetResponse.Err != sarama.ErrNoError {
					log.Warnf("Error in OffsetResponse for %s:%v from broker %v: %s", topicName, partition, brokerID, offsetResponse.Err.Error())
					continue
				}
				topicsOffsetsMutex.Lock()

				topic := xtopicsOffsets[topicName]
				if topic == nil {
					topic = map[int32]int64{}
				}

				topic[partition] = offsetResponse.Offsets[0]
				xtopicsOffsets[topicName] = topic

				topicsOffsetsMutex.Unlock()
			}
		}
	}

	for brokerID, request := range requests {
		wg.Add(1)
		go getBrokerTopicsOffsets(brokerID, request)
	}

	wg.Wait()

	return xtopicsOffsets, nil
}

// --------------------

func KafkaTopicConsumerOffsets(c *gin.Context) {
	client := c.MustGet("kafkaClient").(sarama.Client)
	topic := c.Param("topic")
	consumerGroupID := c.Param("consumer")

	consumersOffsets, err := getTopicConsumerOffsets(client, topic, consumerGroupID)
	if handlHTTPErr(c, err) {
		return
	}

	c.JSON(200, consumersOffsets)
}

func getTopicConsumerOffsets(client sarama.Client, topic string, consumerGroupID string) (map[int32]interface{}, error) {
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

// --

var (
	offsetsChannel = make(chan *protocol.PartitionOffset)
	messageChannel = make(chan *sarama.ConsumerMessage)
	errorChannel   = make(chan *sarama.ConsumerError)

	consumerTopicsOffsets = map[string]map[string]map[int32]int64{}
	consumersOffsetsMutex = &sync.RWMutex{}
)

func ProcessConsumerOffsetsMessage() {
	client, err := KafkaClient(conf.AdminPassword)
	fatalErr(err)

	// Get topics offset in background every tick

	tick := 30 * time.Second

	topicsOffsets, err = getTopicsOffsets(client)
	if err != nil {
		log.WithError(err).Error("Fail to get topics offsets")
	}

	go func() {
		for range time.Tick(tick) {
			var err error

			topicsOffsets, err = getTopicsOffsets(client)
			if err != nil {
				log.WithError(err).Error("Fail to get topics offsets")
			}
		}
	}()

	// Get offset coonsumers for the consumption topic

	offsetsTopic := "__consumer_offsets"

	partitions, err := client.Partitions(offsetsTopic)
	if err != nil {
		fatalErr(err)
	}

	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		fatalErr(err)
	}

	// Start consumers for each partition with fan in
	log.Infof("Starting consumers for %v partitions of %v", len(partitions), offsetsTopic)
	for _, partition := range partitions {
		pconsumer, err := consumer.ConsumePartition(offsetsTopic, partition, sarama.OffsetNewest)
		if err != nil {
			fatalErr(err)
		}

		go func() {
			for msg := range pconsumer.Messages() {
				messageChannel <- msg
			}
		}()
		go func() {
			for err := range pconsumer.Errors() {
				errorChannel <- err
			}
		}()
	}

	go func() {
		for msg := range messageChannel {
			go processConsumerOffsetsMessage(msg)
		}
	}()

	go func() {
		for err := range errorChannel {
			log.Errorf("Consume error on %s:%v: %v", err.Topic, err.Partition, err.Err)
		}
	}()

	go func() {
		for offset := range offsetsChannel {
			consumersOffsetsMutex.Lock()

			topic := consumerTopicsOffsets[offset.Topic]
			if topic == nil {
				topic = map[string]map[int32]int64{}
			}
			consumer := topic[offset.Group]
			if consumer == nil {
				consumer = map[int32]int64{}
			}
			consumer[offset.Partition] = offset.Offset

			topic[offset.Group] = consumer
			consumerTopicsOffsets[offset.Topic] = topic

			consumersOffsetsMutex.Unlock()
		}
	}()
}

func KafkaTopicConsumersOffsets(c *gin.Context) {
	consumersOffsetsMutex.RLock()
	defer consumersOffsetsMutex.RUnlock()

	c.JSON(200, consumerTopicsOffsets)
}

func processConsumerOffsetsMessage(msg *sarama.ConsumerMessage) {
	var keyver, valver uint16
	var group, topic string
	var partition uint32
	var offset, timestamp uint64

	buf := bytes.NewBuffer(msg.Key)
	err := binary.Read(buf, binary.BigEndian, &keyver)
	switch keyver {
	case 0, 1:
		group, err = readString(buf)
		if err != nil {
			log.Warnf("Failed to decode %s:%v offset %v: group", msg.Topic, msg.Partition, msg.Offset)
			return
		}
		topic, err = readString(buf)
		if err != nil {
			log.Warnf("Failed to decode %s:%v offset %v: topic", msg.Topic, msg.Partition, msg.Offset)
			return
		}
		err = binary.Read(buf, binary.BigEndian, &partition)
		if err != nil {
			log.Warnf("Failed to decode %s:%v offset %v: partition", msg.Topic, msg.Partition, msg.Offset)
			return
		}
	case 2:
		log.Debugf("Discarding group metadata message with key version 2")
		return
	default:
		log.Warnf("Failed to decode %s:%v offset %v: keyver %v", msg.Topic, msg.Partition, msg.Offset, keyver)
		return
	}

	buf = bytes.NewBuffer(msg.Value)
	err = binary.Read(buf, binary.BigEndian, &valver)
	if (err != nil) || ((valver != 0) && (valver != 1)) {
		log.Warnf("Failed to decode %s:%v offset %v: valver %v", msg.Topic, msg.Partition, msg.Offset, valver)
		return
	}
	err = binary.Read(buf, binary.BigEndian, &offset)
	if err != nil {
		log.Warnf("Failed to decode %s:%v offset %v: offset", msg.Topic, msg.Partition, msg.Offset)
		return
	}
	_, err = readString(buf)
	if err != nil {
		log.Warnf("Failed to decode %s:%v offset %v: metadata", msg.Topic, msg.Partition, msg.Offset)
		return
	}
	err = binary.Read(buf, binary.BigEndian, &timestamp)
	if err != nil {
		log.Warnf("Failed to decode %s:%v offset %v: timestamp", msg.Topic, msg.Partition, msg.Offset)
		return
	}

	// fmt.Printf("[%s,%s,%v]::OffsetAndMetadata[%v,%s,%v]\n", group, topic, partition, offset, metadata, timestamp)
	partitionOffset := &protocol.PartitionOffset{
		Topic:     topic,
		Partition: int32(partition),
		Group:     group,
		Timestamp: int64(timestamp),
		Offset:    int64(offset),
	}

	timeoutSendOffset(offsetsChannel, partitionOffset, 1)
	return
}

func readString(buf *bytes.Buffer) (string, error) {
	var strlen uint16
	err := binary.Read(buf, binary.BigEndian, &strlen)
	if err != nil {
		return "", err
	}
	strbytes := make([]byte, strlen)
	n, err := buf.Read(strbytes)
	if (err != nil) || (n != int(strlen)) {
		return "", errors.New("string underflow")
	}
	return string(strbytes), nil
}

// Send the offset on the specified channel, but wait no more than maxTime seconds to do so
func timeoutSendOffset(offsetsChan chan *protocol.PartitionOffset, offset *protocol.PartitionOffset, maxTime int) {
	timeout := time.After(time.Duration(maxTime) * time.Second)
	select {
	case offsetsChan <- offset:
	case <-timeout:
	}
}
