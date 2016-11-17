package main

import (
	"errors"
	"os/exec"

	"github.com/gin-gonic/gin"
)

func Key(c *gin.Context) (string, error) {
	key := c.Request.Header.Get("X-Auth")
	if key == "" {
		c.JSON(401, "Empty key")
		return "", errors.New("Invalid key")
	}
	return key, nil
}

func zkURLByKey(key string) string {
	zkPort := ZookyPort
	path := "/" + key

	if key == conf.AdminPassword {
		zkPort = ZkPort
		path = ""
	}

	return zkURL(zkPort) + path
}

func UpdateTopic(c *gin.Context) {
	topic := c.Param("topic")
	partitions := c.Query("p")

	key, err := Key(c)
	if err != nil {
		return
	}

	out, err := exec.Command("docker", "run", "--rm", "ovhcom/queue-kafka-topics-tools",
		"--zookeeper", zkURLByKey(key),
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
		"--zookeeper", zkURLByKey(key),
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
		"--zookeeper", zkURLByKey(key),
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
		"--zookeeper", zkURL(ZookyPort) + "/" + key}, params...)

	out, err := exec.Command("docker", p...).CombinedOutput()
	if err != nil {
		handlHTTPErr(c, errors.New(string(out)))
		return
	}

	c.JSON(200, gin.H{"message": string(out)})
}
