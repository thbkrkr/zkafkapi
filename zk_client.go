package main

import (
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

func zkChroot(c *gin.Context) (string, error) {
	key, err := clientIDAuth(c)
	if err != nil {
		c.JSON(401, "Invalid key")
		return "", errors.New("Invalid key")
	}

	if key == conf.AdminPassword {
		return "", nil
	}

	return "/" + key, nil
}

// Custom zookeeper logger
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

func childrenRecursive(connection *zk.Conn, path string, incrementalPath string) ([]string, error) {
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

			incrementalChildren, e := childrenRecursive(connection, gopath.Join(path, child), incrementalChild)
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
