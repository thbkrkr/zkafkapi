package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/Shopify/sarama"
)

var (
	broker          = flag.String("broker", "localhost:9091", "Broker")
	topic           = flag.String("topic", "", "Topic")
	clientID        = flag.String("clientID", "", "ClientID")
	refreshMetadata = flag.Bool("refreshMetadata", false, "RefreshMetadata")
)

func main() {
	flag.Parse()

	config := sarama.NewConfig()
	config.ClientID = *clientID
	//config.Version = sarama.V0_9_0_1
	client, err := sarama.NewClient([]string{*broker}, config)
	handleErr(err)
	defer client.Close()

	if *topic == "" {
		listTopics(client)
	} else {
		if *refreshMetadata {
			getTopic(client)
		} else {
			getPartitionsAndOffsets(client)
		}
	}
}

func getTopic(client sarama.Client) {
	err := client.RefreshMetadata(*topic)
	handleErr(err)

	printJSON(*topic)
}

func listTopics(client sarama.Client) {
	topics, err := client.Topics()
	handleErr(err)

	printJSON(topics)
}

type TopicInfo struct {
	Name       string
	Partitions map[string]int64
}

func getPartitionsAndOffsets(client sarama.Client) {
	topicInfo := TopicInfo{Name: *topic, Partitions: map[string]int64{}}

	partitions, err := client.Partitions(*topic)
	handleErr(err)

	for _, partitionID := range partitions {
		logSize, err := client.GetOffset(*topic, partitionID, sarama.OffsetNewest)
		handleErr(err)

		topicInfo.Partitions[fmt.Sprintf("%d", partitionID)] = logSize
	}

	printJSON(topicInfo)
}

func printJSON(obj interface{}) {
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
