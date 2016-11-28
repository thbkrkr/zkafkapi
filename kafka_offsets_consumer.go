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

var (
	BrokersTopicsOffsetsInterval = 20 * time.Second

	offsetsChannel = make(chan *protocol.PartitionOffset)
	messageChannel = make(chan *sarama.ConsumerMessage)
	errorChannel   = make(chan *sarama.ConsumerError)

	ConsumersTopicsOffsets = map[string]map[string]map[int32]PartitionOffset{}
	consumersOffsetsMutex  = &sync.RWMutex{}

	TopicsOffsets      = map[string]map[int32]PartitionOffset{}
	topicsOffsetsMutex = &sync.RWMutex{}
)

func FetchTopicsOffsetsAndConsumeConsumerOffsets() {
	client, err := KafkaClient(conf.AdminPassword)
	fatalErr(err)

	// Get topics offset in background every tick

	tick := BrokersTopicsOffsetsInterval

	offsets, err := getAllTopicsOffsets(client)
	if err != nil {
		log.WithError(err).Error("Fail to get topics offsets")
	}

	topicsOffsetsMutex.Lock()
	TopicsOffsets = offsets
	topicsOffsetsMutex.Unlock()

	go func() {
		for range time.Tick(tick) {
			var err error

			offs, err := getAllTopicsOffsets(client)
			if err != nil {
				log.WithError(err).Error("Fail to get topics offsets")
			}

			topicsOffsetsMutex.Lock()
			TopicsOffsets = offs
			topicsOffsetsMutex.Unlock()
		}
	}()

	// Get offset consumers for the consumption topic

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

			topic := ConsumersTopicsOffsets[offset.Topic]
			if topic == nil {
				topic = map[string]map[int32]PartitionOffset{}
			}
			consumer := topic[offset.Group]
			if consumer == nil {
				consumer = map[int32]PartitionOffset{}
			}
			consumer[offset.Partition] = PartitionOffset{Value: offset.Offset, Timestamp: offset.Timestamp}

			topic[offset.Group] = consumer
			ConsumersTopicsOffsets[offset.Topic] = topic

			consumersOffsetsMutex.Unlock()
		}
	}()
}

func KafkaTopicConsumersOffsets(c *gin.Context) {
	consumersOffsetsMutex.RLock()
	defer consumersOffsetsMutex.RUnlock()

	c.JSON(200, ConsumersTopicsOffsets)
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
