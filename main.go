package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/Shopify/sarama"
)

var (
	broker = flag.String("broker", "localhost:9091", "Broker")
	topic  = flag.String("topic", "", "Topic")
)

func main() {
	flag.Parse()

	client, err := sarama.NewClient([]string{*broker}, sarama.NewConfig())
	handleErr(err)
	defer client.Close()

	if *topic == "" {
		listTopics(client)
	} else {
		listPartitions(client)
	}
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

	fmt.Println(string(data))
}

func handleErr(err error) {
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
}
