package main

import (
	"encoding/base64"
	"errors"
	"strings"

	log "github.com/Sirupsen/logrus"
	"github.com/gin-gonic/gin"
	"github.com/samuel/go-zookeeper/zk"
)

func AuthRequired() gin.HandlerFunc {
	return func(c *gin.Context) {

		key, clientIDAuthErr := clientIDAuth(c)
		user, password, basicAuthErr := basicAuth(c)

		if clientIDAuthErr != nil && basicAuthErr != nil {
			c.AbortWithStatus(401)
		}

		// Set Kafka client
		if clientIDAuthErr != nil {

			client, err := KafkaSSLSASLClient(user, password)
			if err != nil {
				log.WithField("user", user).WithError(err).Error("Fail to create Kafka client")
				c.AbortWithStatus(500)
			}
			c.Set("kafkaClient", client)

		} else if basicAuthErr != nil {

			client, err := KafkaClient(key)
			if err != nil {
				log.WithField("key", key).WithError(err).Error("Fail to create Kafka client")
				c.AbortWithStatus(500)
			}
			c.Set("kafkaClient", client)

		} else {
			c.AbortWithStatus(401)
		}

		// Set Zk client
		if clientIDAuthErr != nil {

			var zkConn *zk.Conn
			if password == conf.AdminPassword {
				zkConn = ZkClient
			} else {
				if ZookyClient == nil {
					var err error
					ZookyClient, err = CreateZkClient(ZookyPort)
					if err != nil {
						log.WithField("user", user).WithError(err).Error("Fail to create Zk client")
						c.AbortWithStatus(500)
						ZookyClient.Close()
					}
				}
			}
			c.Set("zkConn", zkConn)

		} else if basicAuthErr != nil {

			var zkConn *zk.Conn
			if key == conf.AdminPassword {
				zkConn = ZkClient
			} else {
				if ZookyClient == nil {
					var err error
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

func clientIDAuth(c *gin.Context) (string, error) {
	key := c.Request.Header.Get("X-Auth")
	if key == "" {
		return "", errors.New("Invalid client.id auth")
	}

	return key, nil
}

func basicAuth(c *gin.Context) (string, string, error) {
	basiAuth := c.Request.Header.Get("Authorization")
	if !strings.Contains(basiAuth, "Basic ") {
		return "", "", errors.New("Invalid basic auth: 'Basic' not found")
	}

	userPassBs, err := base64.StdEncoding.DecodeString(strings.Replace(basiAuth, "Basic ", "", -1))
	if err != nil {
		return "", "", err
	}
	userPwd := strings.Split(string(userPassBs), ":")
	if len(userPwd) != 2 {
		return "", "", errors.New("Invalid basic auth: ':' not found")
	}

	return userPwd[0], userPwd[1], nil
}
