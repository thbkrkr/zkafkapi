package main

import (
	"errors"
	"fmt"
	"os/exec"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/gin-gonic/gin"
)

func ListTopics(c *gin.Context) {
	topics, err := client.Topics()
	if handlHTTPErr(c, err) {
		return
	}

	c.JSON(200, topics)
}

type topicInfo struct {
	Name       string
	Partitions map[string]int64
}

func GetTopic(c *gin.Context) {
	var wg sync.WaitGroup
	var mutex = &sync.Mutex{}

	topic := c.Param("topic")
	topicInfo := topicInfo{Name: topic, Partitions: map[string]int64{}}

	partitions, err := client.Partitions(topic)
	if handlHTTPErr(c, err) {
		return
	}

	wg.Add(len(partitions))
	for _, partitionID := range partitions {
		go func(p int32) {
			defer wg.Done()
			logSize, err := client.GetOffset(topic, p, sarama.OffsetNewest)
			if handlHTTPErr(c, err) {
				return
			}

			mutex.Lock()
			topicInfo.Partitions[fmt.Sprintf("%d", p)] = logSize
			mutex.Unlock()
		}(partitionID)
	}
	wg.Wait()

	c.JSON(200, topicInfo)
}

func DeleteTopic(c *gin.Context) {
	topic := c.Param("topic")

	out, err := exec.Command("docker", "run", "--rm", "ovhcom/queue-kafka-topics-tools",
		"--zookeeper", zkURL()+"/"+conf.Key,
		"--delete", "--topic", topic).CombinedOutput()
	if err != nil {
		handlHTTPErr(c, errors.New(string(out)+"(err: "+err.Error()+")"))
		return
	}

	c.JSON(200, gin.H{"message": string(out)})
}

func CreateTopic(c *gin.Context) {
	topic := c.Param("topic")
	partition := c.Query("p")
	replicationFactor := c.Query("r")

	out, err := exec.Command("docker", "run", "--rm", "ovhcom/queue-kafka-topics-tools",
		"--zookeeper", zkURL()+"/"+conf.Key,
		"--create", "--topic", topic, "--partition", partition,
		"--replication-factor", replicationFactor).CombinedOutput()
	if err != nil {
		handlHTTPErr(c, errors.New(string(out)))
		return
	}

	c.JSON(200, gin.H{"message": string(out)})
}
