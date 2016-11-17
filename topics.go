package main

import (
	"github.com/Shopify/sarama"
	"github.com/gin-gonic/gin"
	"github.com/samuel/go-zookeeper/zk"
)

func FullTopic(c *gin.Context) {
	client := c.MustGet("kafkaClient").(sarama.Client)
	zkConn := c.MustGet("zkConn").(*zk.Conn)
	topicName := c.Param("topic")

	chroot, err := zkChroot(c)
	if handlHTTPErr(c, err) {
		return
	}

	zkTopic, err := getZkTopic(zkConn, chroot, topicName)
	if handlHTTPErr(c, err) {
		return
	}

	topic, err := getTopic(client, topicName)
	if handlHTTPErr(c, err) {
		return
	}

	c.JSON(200, gin.H{
		"Name":       topicName,
		"Partitions": zkTopic.Partitions,
		"Offsets":    topic.Partitions,
	})
}
