package main

import (
	"encoding/json"
	"strings"
	"sync"

	log "github.com/Sirupsen/logrus"
	"github.com/gin-gonic/gin"
	"github.com/samuel/go-zookeeper/zk"
)

type ZkTopicPartitionsOffsets struct {
	Name       string
	Partitions map[string][]int32
}

type ZkTopicPartitionning struct {
	Leader int32   `json:"leader"`
	Isr    []int32 `json:"isr"`
}

func ZkListTopics(c *gin.Context) {
	zkConn := c.MustGet("zkConn").(*zk.Conn)

	chroot, err := zkChroot(c)
	if err != nil {
		return
	}

	log.Info("zk: ls " + chroot + "/brokers/topics")
	topicNames, _, err := zkConn.Children(chroot + "/brokers/topics")
	if handlHTTPErr(c, err) {
		return
	}

	c.JSON(200, topicNames)
}

func ZkGetTopic(c *gin.Context) {
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

	c.JSON(200, zkTopic)
}

func getZkTopic(zkConn *zk.Conn, chroot string, topicName string) (*ZkTopicPartitionsOffsets, error) {
	data, _, err := zkConn.Get(chroot + "/brokers/topics/" + topicName)
	if err != nil {
		return nil, err
	}

	var zkTopic ZkTopicPartitionsOffsets
	err = json.Unmarshal(data, &zkTopic)
	if err != nil {
		return nil, err
	}
	zkTopic.Name = topicName

	return &zkTopic, nil
}

func ZkListConsumers(c *gin.Context) {
	zkConn := c.MustGet("zkConn").(*zk.Conn)

	chroot, err := zkChroot(c)
	if err != nil {
		return
	}

	paths, _, err := zkConn.Children(chroot + "/consumers")
	if handlHTTPErr(c, err) {
		return
	}

	wg := &sync.WaitGroup{}
	mutex := &sync.Mutex{}

	topics := map[string]map[string]interface{}{}
	for _, p := range paths {
		wg.Add(1)
		go func(path string) {
			defer wg.Done()

			data, _, err := zkConn.Get(chroot + "/consumers/" + path)
			if handlHTTPErr(c, err) {
				return
			}

			var tp ZkTopicPartitionsOffsets
			err = json.Unmarshal(data, &tp)
			if handlHTTPErr(c, err) {
				return
			}

			parts := strings.Split(path, "/")
			topicID := parts[0]
			partitionID := parts[2]

			mutex.Lock()

			topic := topics[topicID]
			if topic == nil {
				topic = map[string]interface{}{}
			}
			topic[partitionID] = tp
			topics[topicID] = topic

			mutex.Unlock()
		}(p)
	}
	wg.Wait()

	c.JSON(200, paths)
}

func ZkDeleteTopic(c *gin.Context) {
	zkConn := c.MustGet("zkConn").(*zk.Conn)

	// TODO: Auth
	_, err := zkChroot(c)
	if err != nil {
		return
	}

	topic := c.Param("topic")

	res, err := zkConn.Create("/admin/delete_topics/"+topic, []byte(""), 0, zk.WorldACL(zk.PermAll))
	if handlHTTPErr(c, err) {
		return
	}

	c.JSON(200, res)
}
