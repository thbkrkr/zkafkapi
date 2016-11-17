package main

import (
	"encoding/json"
	"strconv"
	"strings"
	"sync"

	"github.com/gin-gonic/gin"
	"github.com/samuel/go-zookeeper/zk"
)

func ZkListTopicsOffsets(c *gin.Context) {
	zkConn := c.MustGet("zkConn").(*zk.Conn)

	chroot, err := zkChroot(c)
	if handlHTTPErr(c, err) {
		return
	}

	paths, err := childrenRecursive(zkConn, chroot+"/brokers/topics", "")
	if handlHTTPErr(c, err) {
		return
	}

	wg := &sync.WaitGroup{}
	mutex := &sync.Mutex{}

	nb := 0
	topics := map[string]map[string]interface{}{}
	for _, p := range paths {
		if strings.Contains(p, "/state") {
			wg.Add(1)
			nb++

			go func(path string) {
				defer wg.Done()

				data, _, err := ZookyClient.Get(chroot + path)
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
	}

	wg.Wait()

	c.JSON(200, topics)
}

func ZkListConsumersOffsets(c *gin.Context) {
	zkConn := c.MustGet("zkConn").(*zk.Conn)

	chroot, err := zkChroot(c)
	if err != nil {
		return
	}

	paths, err := childrenRecursive(zkConn, chroot+"/consumers", "")
	if handlHTTPErr(c, err) {
		return
	}

	wg := &sync.WaitGroup{}
	mutex := &sync.Mutex{}
	offsets := map[string]map[string]map[string]interface{}{}

	for _, p := range paths {
		parts := strings.Split(p, "/")

		// Skip if it's not an 'offset path' (e.g.: "topic/offsets/consumerGroup/partition")
		if parts[1] != "offsets" || len(parts) != 4 {
			continue
		}

		wg.Add(1)
		go func(path string) {
			defer wg.Done()

			data, _, err := ZookyClient.Get(chroot + "/consumers/" + path)
			if handlHTTPErr(c, err) {
				return
			}
			offsetNumber, err := strconv.ParseInt(string(data), 10, 32)
			if handlHTTPErr(c, err) {
				return
			}

			topicID := parts[0]
			consumerGroupID := parts[2]
			partitionSID := parts[3]

			mutex.Lock()

			consumerGroup := offsets[consumerGroupID]
			if consumerGroup == nil {
				consumerGroup = map[string]map[string]interface{}{}
			}
			topic := consumerGroup[topicID]
			if topic == nil {
				topic = map[string]interface{}{}
			}

			topic[partitionSID] = int32(offsetNumber)
			consumerGroup[topicID] = topic
			offsets[consumerGroupID] = consumerGroup

			mutex.Unlock()
		}(p)
	}

	c.JSON(200, offsets)
}
