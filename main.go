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
	name      = "zkafkapi"
	buildDate = "dev"
	gitCommit = "dev"

	conf Config

	ZkClient    *zk.Conn
	ZookyClient *zk.Conn
)

const (
	KafkaTLSPort = "9093"
	KafkyPort    = "9092"
	KafkaPort    = "9091"
	ZookyPort    = "2181"
	ZkPort       = "2182"
)

type Config struct {
	AdminPassword string `envconfig:"A" required:"true"`
	Broker        string `envconfig:"B" default:"localhost:9092"`
	Proxy         bool   `envconfig:"P" default:"false"`

	MetricsHost  string `envconfig:"METRICS_HOST"`
	MetricsToken string `envconfig:"METRICS_TOKEN"`
}

func main() {
	envConfig()

	var err error

	ZkClient, err = CreateZkClient(ZkPort)
	fatalErr(err)

	go FetchTopicsOffsetsAndConsumeConsumerOffsets()

	http.API(name, buildDate, gitCommit, 4242, router)
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
			" -- Topics -- ",
			"GET     /topics               ListTopics",
			"GET     /topics/:topic        GetTopic",
			"POST    /topics/:topic        CreateTopic",
			"PUT     /topics/:topic        UpdateTopic",
			"DELETE  /topics/:topic        DeleteTopic",
			" -- Consumers -- ",
			"GET     /consumers            ListConsumers",
			"GET     /consumers/:consumer  GetConsumer",
			" -- Lag --",
			"GET     /lag                  Lag",
		})
	})

	a := r.Group("/")
	a.Use(AuthRequired())

	// -- Topics
	a.GET("/topics", ZkListTopics)
	a.GET("/topics/:topic", GetTopic)
	a.POST("/topics/:topic", CreateTopic)
	a.PUT("/topics/:topic", UpdateTopic)
	a.DELETE("/topics/:topic", DeleteTopic)

	// -- Consumers
	a.GET("/consumers", KafkaListConsumers)
	a.GET("/consumers/:consumer", KafkaGetConsumer)

	// -- Lag
	a.GET("/lag", LagSummary)
	a.GET("/lag/full", Lag)

	// -- Kafka
	a.GET("/k/topics", KafkaListTopics)
	a.GET("/k/topics/:topic", KafkaGetTopicOffsets)
	a.GET("/k/topics/:topic/consumers/:consumer", KafkaListTopicConsumerOffsets)
	a.GET("/k/offsets/topics", KafkaListAllTopicsOffsets)
	a.GET("/k/offsets/consumers", KafkaListAllConsumersOffsets)
	a.GET("/k/consumers", KafkaListConsumers)
	a.GET("/k/consumers/:consumer/topics/:topic/", KafkaListTopicConsumerOffsets)

	// -- Zk
	a.GET("/z/topics", ZkListTopics)
	a.GET("/z/topics/:topic", ZkGetTopicPartitions)
	a.GET("/z/partitions", ZkListTopicsPartitions)
	a.GET("/z/consumers", ZkListConsumersOffsets)
	a.DELETE("/z/topics/:topic", ZkDeleteTopic)

	a.GET("/topics/:topic/metrics", TopicMetrics)
}

func handlHTTPErr(c *gin.Context, err error) bool {
	isErr := err != nil
	if isErr {
		log.WithError(err).Error("HTTP error")
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
