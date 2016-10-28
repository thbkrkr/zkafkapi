package main

import (
	"encoding/json"
	gopath "path"
	"sort"
	"strings"

	log "github.com/Sirupsen/logrus"
	"github.com/gin-gonic/gin"
	"github.com/samuel/go-zookeeper/zk"
)

func chroot() string { return "/" + conf.Key }

type partition struct {
	Leader int32   `json:"leader"`
	Isr    []int32 `json:"isr"`
}

func ZkListTopics(c *gin.Context) {
	paths, err := childrenRecursiveInternal(conn, chroot()+"/brokers/topics", "")
	if handlHTTPErr(c, err) {
		return
	}

	topics := map[string]map[string]interface{}{}
	for _, path := range paths {
		if strings.Contains(path, "/state") {
			parts := strings.Split(path, "/")
			topicID := parts[0]
			partitionID := parts[2]
			topic := topics[topicID]
			if topic == nil {
				topic = map[string]interface{}{}
			}

			data, _, err := conn.Get(chroot() + "/brokers/topics/" + path)
			if handlHTTPErr(c, err) {
				return
			}

			var p partition
			err = json.Unmarshal(data, &p)
			if handlHTTPErr(c, err) {
				return
			}

			topic[partitionID] = p
			topics[topicID] = topic
		}
	}

	c.JSON(200, topics)
}

func ZkListConsumers(c *gin.Context) {
	paths, err := childrenRecursiveInternal(conn, chroot()+"/consumers", "")
	if handlHTTPErr(c, err) {
		return
	}

	topics := map[string]map[string]interface{}{}
	for _, path := range paths {
		if strings.Contains(path, "/state") {
			parts := strings.Split(path, "/")
			topicID := parts[0]
			partitionID := parts[2]
			topic := topics[topicID]
			if topic == nil {
				topic = map[string]interface{}{}
			}

			data, _, err := conn.Get(chroot() + "/" + path)
			if handlHTTPErr(c, err) {
				return
			}

			var p partition
			err = json.Unmarshal(data, &p)
			if handlHTTPErr(c, err) {
				return
			}

			topic[partitionID] = p
			topics[topicID] = topic
		}
	}

	c.JSON(200, paths)
}

func childrenRecursiveInternal(connection *zk.Conn, path string, incrementalPath string) ([]string, error) {
	children, _, err := connection.Children(path)
	if err != nil {
		return children, err
	}
	sort.Sort(sort.StringSlice(children))
	recursiveChildren := []string{}
	for _, child := range children {
		incrementalChild := gopath.Join(incrementalPath, child)
		recursiveChildren = append(recursiveChildren, incrementalChild)
		log.Debugf("incremental child: %+v", incrementalChild)
		incrementalChildren, err := childrenRecursiveInternal(connection, gopath.Join(path, child), incrementalChild)
		if err != nil {
			return children, err
		}
		recursiveChildren = append(recursiveChildren, incrementalChildren...)
	}
	return recursiveChildren, err
}

func zkURL() string {
	return strings.Replace(conf.Broker, "9092", "2181", -1)
}

//

// Custom logger for the zk client to replace the default logger:
// https://github.com/samuel/go-zookeeper/blob/177002e16a0061912f02377e2dd8951a8b3551bc/zk/structs.go#L20
type zkLogger struct{}

func (zkLogger) Printf(format string, a ...interface{}) {
	if strings.Split(format, " ")[0] == "Failed" {
		log.WithField("from", "zkClient").Errorf(format, a...)
		return
	}

	if a != nil && a[0] != nil {
		switch a[0].(type) {
		case error:
			log.WithField("from", "zkClient").Errorf(format, a...)
		}
	}
}
