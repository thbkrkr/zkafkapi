package main

import (
	"errors"
	"os"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
	"github.com/gin-gonic/gin"
	"github.com/kelseyhightower/envconfig"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/thbkrkr/go-utilz/http"
)

var (
	conf     Config
	zkClient *zk.Conn

	buildDate = "dev"
	gitCommit = "dev"
	name      = "kafka-topics"
)

type Config struct {
	Broker string `envconfig:"B" required:"true"`
	//Key    string `envconfig:"K" required:"true"`

	MetricsHost  string `envconfig:"METRICS_HOST" required:"true"`
	MetricsToken string `envconfig:"METRICS_TOKEN" required:"true"`
}

func main() {
	envConfig()

	err := createZkClient()
	fatalErr(err)

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
			"POST    /k/topics/:topic?p=3&r=2  CreateTopic",
			"GET     /k/topics/:topic          GetTopic",
			"PUT     /k/topics/:topic?p=6      UpdateTopic",
			"DELETE  /k/topics/:topic          DeleteTopic",
			"GET     /k/topics/:topic/metrics  TopicMetrics",
			"GET     /k/brokers                BrokersLogSize",
			"GET     /k/consumers              ConsumersOffsets",
			" -- Zk -- ",
			"GET     /z/topics                 ZkListTopics",
			"GET     /z/consumers              ZkListConsumers",
		})
	})

	a := r.Group("/")
	a.Use(AuthRequired())

	a.GET("/k/topics", ListTopics)

	a.POST("/k/topics/:topic", CreateTopic)
	a.GET("/k/topics/:topic", GetTopic)
	a.PUT("/k/topics/:topic", UpdateTopic)
	a.DELETE("/k/topics/:topic", DeleteTopic)

	a.GET("/k/topics/:topic/metrics", TopicMetrics)

	a.GET("/k/brokers", BrokersLogSize)
	a.GET("/k/consumers", ConsumersOffsets)

	a.GET("/z/topics", ZkListTopics)
	a.GET("/z/consumers", ZkListConsumers)
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
		} else {
			c.AbortWithStatus(401)
		}
	}
}

func auth(key string) bool {
	// TODO
	return key != ""
}

func ZkChroot(c *gin.Context) (string, error) {
	key := c.Request.Header.Get("X-Auth")
	if !auth(key) {
		c.JSON(401, "Invalid key")
		return "", errors.New("Invalid key")
	}
	return "/" + key, nil
}

// -------------

var kafkaClients = map[string]sarama.Client{}
var kafkaLock = sync.RWMutex{}

var zkClients = map[string]*zk.Conn{}
var zkLock = sync.RWMutex{}

func KafkaClient(key string) (sarama.Client, error) {
	kafkaLock.RLock()
	kafkaClient := kafkaClients[key]
	kafkaLock.RUnlock()

	if kafkaClient == nil {
		config := sarama.NewConfig()
		config.ClientID = key
		c, err := sarama.NewClient([]string{conf.Broker}, config)
		if err != nil {
			return nil, err
		}

		kafkaClient = c
		kafkaLock.Lock()
		kafkaClients[key] = c
		kafkaLock.Unlock()
	}

	return kafkaClient, nil
}

func createZkClient() error {
	c, _, err := zk.Connect([]string{zkURL()}, time.Second)
	if err != nil {
		return err
	}

	c.SetLogger(zkLogger{})

	zkClient = c
	return nil
}

func fatalErr(err error) {
	if err != nil {
		log.Error(err)
		os.Exit(1)
	}
}

func handlHTTPErr(c *gin.Context, err error) bool {
	isErr := err != nil
	if isErr {
		c.JSON(500, gin.H{"message": err.Error()})
	}
	return isErr
}
