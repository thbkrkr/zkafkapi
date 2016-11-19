package main

import (
	log "github.com/Sirupsen/logrus"
	"github.com/gin-gonic/gin"
	"github.com/samuel/go-zookeeper/zk"
)

func AuthRequired() gin.HandlerFunc {
	return func(c *gin.Context) {
		key := c.Request.Header.Get("X-Auth")

		if auth(key) {
			client, err := KafkaClient(key)
			if err != nil {
				log.WithField("key", key).WithError(err).Error("Fail to create Kafka client")
				c.AbortWithStatus(500)
			}
			c.Set("kafkaClient", client)

			var zkConn *zk.Conn
			if key == conf.AdminPassword {
				zkConn = ZkClient
			} else {
				if ZookyClient == nil {
					ZookyClient, err = CreateZkClient(ZookyPort)
					if err != nil {
						log.WithField("key", key).WithError(err).Error("Fail to create Zk client")
						c.AbortWithStatus(500)
					}
				}
				zkConn = ZookyClient
			}
			c.Set("zkConn", zkConn)

		} else {
			c.AbortWithStatus(401)
		}
	}
}

func auth(key string) bool {
	return key != ""
}
