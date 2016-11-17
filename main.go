package main

import (
	"os"

	log "github.com/Sirupsen/logrus"
	"github.com/gin-gonic/gin"
	"github.com/kelseyhightower/envconfig"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/thbkrkr/go-utilz/http"
)

var (
	name      = "kafka-topics"
	buildDate = "dev"
	gitCommit = "dev"

	conf Config

	KafkyPort = "9092"
	KafkaPort = "9091"
	ZookyPort = "2181"
	ZkPort    = "2182"

	ZkClient    *zk.Conn
	ZookyClient *zk.Conn
)

type Config struct {
	AdminPassword string `envconfig:"A" required:"true"`

	Broker string `envconfig:"B" required:"true"`
	//Key    string `envconfig:"K" required:"true"`

	MetricsHost  string `envconfig:"METRICS_HOST" required:"true"`
	MetricsToken string `envconfig:"METRICS_TOKEN" required:"true"`
}

func main() {
	envConfig()

	var err error

	ZkClient, err = CreateZkClient(ZkPort)
	fatalErr(err)

	ZookyClient, err = CreateZkClient(ZookyPort)
	fatalErr(err)

	go ProcessConsumerOffsetsMessage()

	http.API(name, buildDate, gitCommit, router)
}

func envConfig() {
	err := envconfig.Process("", &conf)
	if err != nil {
		log.WithError(err).Fatal("Fail to process env config")
	}
}

func router(r *gin.Engine) {
	r.GET("/help", func(c *gin.Context) {
		c.JSON(200, []string{
			" -- Kafka -- ",
			"GET     /k/topics                 ListTopics",
			"GET     /k/topics/:topic          GetTopic",
			"POST    /k/topics/:topic?p=3&r=2  CreateTopic",
			"PUT     /k/topics/:topic?p=6      UpdateTopic",
			"DELETE  /k/topics/:topic          DeleteTopic",
			"GET     /k/topics/:topic/metrics  TopicMetrics",
			"GET     /k/offsets          			 KafkaTopicsOffsets",
			"GET     /k/t/:topic/c/:consumer   KafkaTopicConsumerOffsets",
			" -- Zk -- ",
			"GET     /z/topics                 ZkListTopics",
			"GET     /z/topics/offsets         ZkListTopicsOffsets",
			"GET     /z/consumers              ZkListConsumers",
		})
	})

	a := r.Group("/")
	a.Use(AuthRequired())

	a.GET("/k/topics", ListTopics)
	a.GET("/k/t", ListTopics)
	a.GET("/k/topics/:topic", GetTopic)
	a.GET("/k/t/:topic", GetTopic)
	a.GET("/t/:topic", FullTopic)

	a.POST("/k/topics/:topic", CreateTopic)
	a.POST("/k/t/:topic", CreateTopic)
	a.PUT("/k/topics/:topic", UpdateTopic)
	a.PUT("/k/t/:topic", UpdateTopic)
	a.DELETE("/k/topics/:topic", DeleteTopic)
	a.DELETE("/k/t/:topic", DeleteTopic)

	a.GET("/k/topics/:topic/metrics", TopicMetrics)
	a.GET("/k/t/:topic/m", TopicMetrics)

	a.GET("/k/offsets", KafkaTopicsOffsets)
	a.GET("/k/to", KafkaTopicsOffsets)
	a.GET("/k/t/:topic/c/:consumer", KafkaTopicConsumerOffsets)
	a.GET("/k/tc", KafkaTopicConsumersOffsets)

	a.GET("/z/topics", ZkListTopics)
	a.GET("/z/t", ZkListTopics)
	a.GET("/z/topics/:topic", ZkGetTopic)
	a.GET("/z/t/:topic", ZkGetTopic)
	a.GET("/z/topics-offsets", ZkListTopicsOffsets)
	a.GET("/z/to", ZkListTopicsOffsets)
	a.DELETE("/z/topics/:topic", ZkDeleteTopic)
	a.DELETE("/z/t/:topic", ZkDeleteTopic)
	a.GET("/z/consumers", ZkConsumersOffsets)
	a.GET("/z/c", ZkConsumersOffsets)

	a.GET("/lag", Lag)
}

func AuthRequired() gin.HandlerFunc {
	return func(c *gin.Context) {
		key := c.Request.Header.Get("X-Auth")

		if auth(key) {
			client, err := KafkaClient(key)
			if err != nil {
				log.WithError(err).Error("Fail to create Sarama client")
				c.AbortWithStatus(500)
			}
			c.Set("kafkaClient", client)

			var zkConn *zk.Conn
			if key == conf.AdminPassword {
				zkConn = ZkClient
			} else {
				zkConn = ZookyClient
			}
			c.Set("zkConn", zkConn)

		} else {
			c.AbortWithStatus(401)
		}
	}
}

func auth(key string) bool {
	// TODO
	return key != ""
}

// -------------

func handlHTTPErr(c *gin.Context, err error) bool {
	isErr := err != nil
	if isErr {
		c.JSON(500, gin.H{"message": err.Error()})
	}
	return isErr
}

func fatalErr(err error) {
	if err != nil {
		log.Error(err)
		os.Exit(1)
	}
}
