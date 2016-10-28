package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/Shopify/sarama"
)

var (
	broker   = flag.String("broker", "localhost:9091", "Broker")
	clientID = flag.String("client-id", "sarama", "Client ID")
	command  = flag.String("command", "ls", "Command")
	topic    = flag.String("topic", "", "Topic")
)

func main() {
	flag.Parse()

	switch *command {
	case "ls":
		client := createClient()
		defer client.Close()
		listTopics(client)
	case "show":
		client := createClient()
		defer client.Close()
		listPartitions(client)
	case "rm":
		rmTopic()
	default:
		fmt.Println("Invalid command")
		os.Exit(1)
	}
}

func createClient() sarama.Client {
	config := sarama.NewConfig()
	config.ClientID = *clientID
	client, err := sarama.NewClient([]string{*broker}, config)
	handleErr(err)
	return client
}

func listTopics(client sarama.Client) {
	topics, err := client.Topics()
	handleErr(err)

	printJson(topics)
}

type TopicInfo struct {
	Name       string
	Partitions map[string]int64
}

func listPartitions(client sarama.Client) {
	topicInfo := TopicInfo{Name: *topic, Partitions: map[string]int64{}}

	partitions, err := client.Partitions(*topic)
	handleErr(err)

	for _, partitionID := range partitions {
		logSize, err := client.GetOffset(*topic, partitionID, sarama.OffsetNewest)
		handleErr(err)

		topicInfo.Partitions[fmt.Sprintf("%d", partitionID)] = logSize
	}

	printJson(topicInfo)
}

func printJson(obj interface{}) {
	data, err := json.Marshal(obj)
	handleErr(err)
	handleErr(err)
	fmt.Println(string(data))
}

func handleErr(err error) {
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
}

func rmTopic() {
	out, err := exec.Command("docker", "run", "--rm", "ovhcom/queue-kafka-topics-tools",
		"--zookeeper", strings.Replace(*broker, "9092", "2181", -1)+"/"+*clientID,
		"--delete", "--topic", *topic).CombinedOutput()
	if err != nil {
		fmt.Println(string(out))
	}
	handleErr(err)

	printJson(map[string]string{"out": string(out)})
}
