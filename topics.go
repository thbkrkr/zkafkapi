package main

import (
	"github.com/Shopify/sarama"
	"github.com/gin-gonic/gin"
	"github.com/samuel/go-zookeeper/zk"
)

func GetTopic(c *gin.Context) {
	client := c.MustGet("kafkaClient").(sarama.Client)
	zkConn := c.MustGet("zkConn").(*zk.Conn)
	topicName := c.Param("topic")

	chroot, err := zkChroot(c)
	if handlHTTPErr(c, err) {
		return
	}

	zkTopic, err := getZkTopicPartitions(zkConn, chroot, topicName)
	if handlHTTPErr(c, err) {
		return
	}

	topic, err := getKafkaTopicOffsets(client, topicName)
	if handlHTTPErr(c, err) {
		return
	}

	c.JSON(200, gin.H{
		"Name":       topicName,
		"Partitions": zkTopic.Partitions,
		"Offsets":    topic.Partitions,
	})
}
