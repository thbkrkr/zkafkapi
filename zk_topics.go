package main

import (
	"encoding/json"
	"sync"

	log "github.com/Sirupsen/logrus"
	"github.com/gin-gonic/gin"
	"github.com/samuel/go-zookeeper/zk"
)

type ZkTopic struct {
	Name       string
	Partitions map[string][]int32
}

type ZkTopicPartition struct {
	Leader int32   `json:"leader"`
	Isr    []int32 `json:"isr"`
}

func ZkListTopics(c *gin.Context) {
	zkConn := c.MustGet("zkConn").(*zk.Conn)

	chroot, err := zkChroot(c)
	if err != nil {
		return
	}

	topicNames, _, err := zkConn.Children(chroot + "/brokers/topics")
	if handlHTTPErr(c, err) {
		return
	}

	c.JSON(200, topicNames)
}

func ZkListTopicsPartitions(c *gin.Context) {
	zkConn := c.MustGet("zkConn").(*zk.Conn)

	chroot, err := zkChroot(c)
	if handlHTTPErr(c, err) {
		return
	}

	topicNames, _, err := zkConn.Children(chroot + "/brokers/topics")
	if handlHTTPErr(c, err) {
		return
	}

	wg := &sync.WaitGroup{}
	mutex := &sync.Mutex{}
	topics := []ZkTopic{}

	for _, topicName := range topicNames {
		wg.Add(1)

		go func(name string) {
			defer wg.Done()

			topic, err := getZkTopicPartitions(zkConn, chroot, name)
			if err != nil {
				log.WithField("topic", name).Error("Fail to get zk topics")
				return
			}

			mutex.Lock()
			topics = append(topics, *topic)
			mutex.Unlock()

		}(topicName)
	}
	wg.Wait()

	c.JSON(200, topics)
}

func ZkGetTopicPartitions(c *gin.Context) {
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

	c.JSON(200, zkTopic)
}

func getZkTopicPartitions(zkConn *zk.Conn, chroot string, topicName string) (*ZkTopic, error) {
	data, _, err := zkConn.Get(chroot + "/brokers/topics/" + topicName)
	if err != nil {
		return nil, err
	}

	var zkTopic ZkTopic
	err = json.Unmarshal(data, &zkTopic)
	if err != nil {
		return nil, err
	}
	zkTopic.Name = topicName

	return &zkTopic, nil
}

func ZkDeleteTopic(c *gin.Context) {
	if IsNotAdmin(c) {
		return
	}

	zkConn := c.MustGet("zkConn").(*zk.Conn)
	topic := c.Param("topic")

	res, err := zkConn.Create("/admin/delete_topics/"+topic, []byte(""), 0, zk.WorldACL(zk.PermAll))
	if handlHTTPErr(c, err) {
		return
	}

	c.JSON(200, res)
}

func IsNotAdmin(c *gin.Context) bool {
	key := c.Request.Header.Get("X-Auth")
	isNotAdmin := key != conf.AdminPassword
	if isNotAdmin {
		c.JSON(403, "Restricted area")
	}
	return isNotAdmin
}
