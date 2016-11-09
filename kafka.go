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
	client := c.MustGet("kafkaClient").(sarama.Client)

	err := client.RefreshMetadata([]string{}...)
	if handlHTTPErr(c, err) {
		return
	}

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
	client := c.MustGet("kafkaClient").(sarama.Client)

	var wg sync.WaitGroup
	var mutex = &sync.Mutex{}

	topic := c.Param("topic")
	topicInfo := topicInfo{Name: topic, Partitions: map[string]int64{}}

	err := client.RefreshMetadata(topic)
	if handlHTTPErr(c, err) {
		return
	}

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

func Key(c *gin.Context) (string, error) {
	key := c.Request.Header.Get("X-Auth")
	if key == "" {
		c.JSON(401, "Empty key")
		return "", errors.New("Invalid key")
	}
	return key, nil
}

func UpdateTopic(c *gin.Context) {
	topic := c.Param("topic")
	partitions := c.Query("p")

	key, err := Key(c)
	if err != nil {
		return
	}

	out, err := exec.Command("docker", "run", "--rm", "ovhcom/queue-kafka-topics-tools",
		"--zookeeper", zkURL()+"/"+key,
		"--alter", "--topic", topic, "--partitions", partitions).CombinedOutput()
	if err != nil {
		handlHTTPErr(c, errors.New(string(out)+"(err: "+err.Error()+")"))
		return
	}

	c.JSON(200, gin.H{"message": string(out)})

}

func DeleteTopic(c *gin.Context) {
	topic := c.Param("topic")

	key, err := Key(c)
	if err != nil {
		return
	}

	out, err := exec.Command("docker", "run", "--rm", "ovhcom/queue-kafka-topics-tools",
		"--zookeeper", zkURL()+"/"+key,
		"--delete", "--topic", topic).CombinedOutput()
	if err != nil {
		handlHTTPErr(c, errors.New(string(out)+"(err: "+err.Error()+")"))
		return
	}

	c.JSON(200, gin.H{"message": string(out)})
}

func CreateTopic(c *gin.Context) {
	topic := c.Param("topic")
	partitions := c.Query("p")
	replicationFactor := c.Query("r")

	key, err := Key(c)
	if err != nil {
		return
	}

	out, err := exec.Command("docker", "run", "--rm", "ovhcom/queue-kafka-topics-tools",
		"--zookeeper", zkURL()+"/"+key,
		"--create", "--topic", topic, "--partitions", partitions,
		"--replication-factor", replicationFactor).CombinedOutput()
	if err != nil {
		handlHTTPErr(c, errors.New(string(out)))
		return
	}

	c.JSON(200, gin.H{"message": string(out)})
}

func execCommand(c *gin.Context, params ...string) {
	key, err := Key(c)
	if err != nil {
		return
	}

	p := append([]string{
		"run", "--rm", "ovhcom/queue-kafka-topics-tools",
		"--zookeeper", zkURL() + "/" + key}, params...)

	out, err := exec.Command("docker", p...).CombinedOutput()
	if err != nil {
		handlHTTPErr(c, errors.New(string(out)))
		return
	}

	c.JSON(200, gin.H{"message": string(out)})
}
