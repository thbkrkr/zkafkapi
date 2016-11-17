package main

import (
	"encoding/json"
	"errors"
	gopath "path"
	"sort"
	"strings"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/gin-gonic/gin"
	"github.com/samuel/go-zookeeper/zk"
)

func CreateZkClient(port string) (*zk.Conn, error) {
	c, _, err := zk.Connect([]string{zkURL(port)}, time.Second)
	if err != nil {
		return nil, err
	}

	c.SetLogger(zkLogger{})

	return c, err
}

func zkURL(zkPort string) string {
	url := strings.Replace(conf.Broker, KafkyPort, zkPort, -1)
	url = strings.Replace(url, KafkaPort, zkPort, -1)
	return url
}

func ZkChroot(c *gin.Context) (string, error) {
	key := c.Request.Header.Get("X-Auth")
	if !auth(key) {
		c.JSON(401, "Invalid key")
		return "", errors.New("Invalid key")
	}
	if key == conf.AdminPassword {
		return "", nil
	}
	return "/" + key, nil
}

type TopicPartition struct {
	Leader int32   `json:"leader"`
	Isr    []int32 `json:"isr"`
}

func ZkListTopics(c *gin.Context) {
	zkConn := c.MustGet("zkConn").(*zk.Conn)

	chroot, err := ZkChroot(c)
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

func ZkListTopicsOffsets(c *gin.Context) {
	zkConn := c.MustGet("zkConn").(*zk.Conn)

	chroot, err := ZkChroot(c)
	if err != nil {
		return
	}

	paths, err := childrenRecursiveInternal(zkConn, chroot+"/brokers/topics", "")
	//zkConn.Children(chroot + "/brokers/topics")
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

				var tp TopicPartition
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

type ZkTopic struct {
	Name       string
	Partitions map[string][]int32
}

func ZkGetTopic(c *gin.Context) {
	zkConn := c.MustGet("zkConn").(*zk.Conn)
	topicName := c.Param("topic")

	chroot, err := ZkChroot(c)
	if handlHTTPErr(c, err) {
		return
	}

	zkTopic, err := getZkTopic(zkConn, chroot, topicName)
	if handlHTTPErr(c, err) {
		return
	}

	c.JSON(200, zkTopic)
}

func getZkTopic(zkConn *zk.Conn, chroot string, topicName string) (*ZkTopic, error) {
	var zkTopic ZkTopic

	data, _, err := zkConn.Get(chroot + "/brokers/topics/" + topicName)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(data, &zkTopic)
	if err != nil {
		return nil, err
	}
	zkTopic.Name = topicName

	return &zkTopic, nil
}

func ZkListConsumers(c *gin.Context) {
	zkConn := c.MustGet("zkConn").(*zk.Conn)

	chroot, err := ZkChroot(c)
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

			var tp TopicPartition
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

func childrenRecursiveInternal(connection *zk.Conn, path string, incrementalPath string) ([]string, error) {
	children, _, err := connection.Children(path)
	if err != nil {
		return children, err
	}
	sort.Sort(sort.StringSlice(children))
	recursiveChildren := []string{}

	wg := &sync.WaitGroup{}
	mutex := &sync.Mutex{}

	for _, c := range children {
		wg.Add(1)
		go func(child string) {
			defer wg.Done()

			incrementalChild := gopath.Join(incrementalPath, child)

			mutex.Lock()
			recursiveChildren = append(recursiveChildren, incrementalChild)
			mutex.Unlock()

			log.Debugf("incremental child: %+v", incrementalChild)
			incrementalChildren, e := childrenRecursiveInternal(connection, gopath.Join(path, child), incrementalChild)
			if e != nil {
				mutex.Lock()
				err = e
				mutex.Unlock()
				return
			}

			mutex.Lock()
			recursiveChildren = append(recursiveChildren, incrementalChildren...)
			mutex.Unlock()
		}(c)
	}
	wg.Wait()

	return recursiveChildren, err
}

//

func ZkDeleteTopic(c *gin.Context) {
	zkConn := c.MustGet("zkConn").(*zk.Conn)

	// TODO: Auth
	_, err := ZkChroot(c)
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

//

// Custom logger for the zk client to replace the default logger:
// https://github.com/samuel/go-zookeeper/blob/177002e16a0061912f02377e2dd8951a8b3551bc/zk/structs.go#L20
type zkLogger struct{}

func (zkLogger) Printf(format string, a ...interface{}) {
	if strings.Split(format, " ")[0] == "Failed" {
		log.WithField("from", "ZookyClient").Errorf(format, a...)
		return
	}

	if a != nil && a[0] != nil {
		switch a[0].(type) {
		case error:
			log.WithField("from", "ZookyClient").Errorf(format, a...)
		}
	}
}
