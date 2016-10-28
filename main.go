package main

import (
	"os"
	"time"

	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
	"github.com/gin-gonic/gin"
	"github.com/kelseyhightower/envconfig"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/thbkrkr/go-utilz/http"
)

var (
	conf   Config
	client sarama.Client
	conn   *zk.Conn

	buildDate = "dev"
	gitCommit = "dev"
	name      = "kafka-topics"
)

type Config struct {
	Broker string `envconfig:"B" required:"true"`
	Key    string `envconfig:"K" required:"true"`
}

func main() {
	envConfig()
	createKafkaClient()
	createZkClient()
	http.API(name, buildDate, gitCommit, router)
}

func router(r *gin.Engine) {
	r.GET("/help", func(c *gin.Context) {
		c.JSON(200, []string{
			" -- Kafka -- ",
			"POST    /k/topics/:topic  CreateTopic",
			"GET     /k/topics/:topic  GetTopic",
			"DELETE  /k/topics/:topic  DeleteTopic",
			"GET     /k/topics         ListTopics",
			"GET     /k/offsets        Offsets",
			" -- Zk -- ",
			"GET     /z/topics         ZkListTopics",
			"GET     /z/consumers      ZkListConsumers",
		})
	})

	r.POST("/k/topics/:topic", CreateTopic)
	r.GET("/k/topics/:topic", GetTopic)
	r.DELETE("/k/topics/:topic", DeleteTopic)
	r.GET("/k/topics", ListTopics)
	r.GET("/k/offsets", Offsets)

	r.GET("/z/topics", ZkListTopics)
	r.GET("/z/consumers", ZkListConsumers)
}

func envConfig() {
	err := envconfig.Process("", &conf)
	if err != nil {
		log.WithError(err).Fatal("Fail to process env config")
	}
}

func createKafkaClient() {
	config := sarama.NewConfig()
	config.ClientID = conf.Key
	c, err := sarama.NewClient([]string{conf.Broker}, config)
	handleErr(err)
	client = c
}

func createZkClient() {
	c, _, err := zk.Connect([]string{zkURL()}, time.Second)
	handleErr(err)
	c.SetLogger(zkLogger{})
	conn = c
}

func handleErr(err error) {
	if err != nil {
		log.Error(err)
		os.Exit(1)
	}
}
