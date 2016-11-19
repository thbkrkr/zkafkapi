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
	KafkyPort = "9092"
	KafkaPort = "9091"
	ZookyPort = "2181"
	ZkPort    = "2182"
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

	go FetchOffsets()

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
			"GET     /k/topics/:topic          GetTopicOffsets",
			"GET     /topics/:topic            FullTopic",
			"POST    /k/topics/:topic?p=3&r=2  CreateTopic",
			"PUT     /k/topics/:topic?p=6      UpdateTopic",
			"DELETE  /k/topics/:topic          DeleteTopic",
			"GET     /k/offsets/topics         KafkaListTopicsOffsets",
			"GET     /k/offsets/consumers      KafkaListConsumersOffsets",
			"GET     /k/consumers              KafkaListConsumers",
			"GET     /k/t/:topic/c/:consumer   KafkaListConsumerOffsets",
			" -- Zk -- ",
			"GET     /z/topics                 ZkListTopics",
			"GET     /z/topics/:topic          ZkGetTopicPartitions",
			"GET     /z/partitions             ZkListTopicsPartitions",
			"GET     /z/consumers              ZkListConsumersOffsets",
			" -- Lag --",
			"GET     /lag                      Lag",
			"GET     /lag/status               LagStatus",
			" -- Metrics --",
			"GET     /k/topics/:topic/metrics  TopicMetrics",
		})
	})

	a := r.Group("/")
	a.Use(AuthRequired())

	a.GET("/k/topics", ListTopics)
	a.GET("/k/topics/:topic", GetTopicOffsets)
	a.GET("/topics/:topic", FullTopic)

	a.POST("/k/topics/:topic", CreateTopic)
	a.PUT("/k/topics/:topic", UpdateTopic)
	a.DELETE("/k/topics/:topic", DeleteTopic)

	a.GET("/k/offsets/topics", KafkaListTopicsOffsets)
	a.GET("/k/offsets/consumers", KafkaListConsumersOffsets)
	a.GET("/k/consumers", KafkaListConsumers)
	a.GET("/k/t/:topic/c/:consumer", KafkaListConsumerOffsets)

	a.GET("/z/topics", ZkListTopics)
	a.GET("/z/topics/:topic", ZkGetTopicPartitions)

	a.GET("/z/partitions", ZkListTopicsPartitions)
	a.GET("/z/consumers", ZkListConsumersOffsets)

	a.DELETE("/z/topics/:topic", ZkDeleteTopic)

	a.GET("/lag", Lag)
	a.GET("/lag/status", LagStatus)

	a.GET("/k/topics/:topic/metrics", TopicMetrics)
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
