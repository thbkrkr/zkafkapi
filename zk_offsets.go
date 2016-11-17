package main

import (
	"strconv"
	"strings"
	"sync"

	"github.com/gin-gonic/gin"
	"github.com/samuel/go-zookeeper/zk"
)

func ZkConsumersOffsets(c *gin.Context) {
	zkConn := c.MustGet("zkConn").(*zk.Conn)

	chroot, err := ZkChroot(c)
	if err != nil {
		return
	}

	paths, err := childrenRecursiveInternal(zkConn, chroot+"/consumers", "")
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
