# ZKafkAPI

API to manage Kafka topics and consumers.

### Run

```
docker run -d -p 4242:4242 \
  --net=host \
  -A=$ADMIN_PASSWORD \
  -v /var/run/docker.sock:/var/run/docker.sock \
  krkr/zkafkapi
```

### API

```
> curl -s localhost:4242/help
[
  " -- Kafka -- ",
  "GET     /k/topics                 ListTopics",
  "GET     /k/topics/:topic          GetTopicOffsets",
  "GET     /topics/:topic            FullTopic",
  "POST    /k/topics/:topic?p=3&r=2  CreateTopic",
  "PUT     /k/topics/:topic?p=6      UpdateTopic",
  "DELETE  /k/topics/:topic          DeleteTopic",
  "GET     /k/offsets                KafkaListTopicsOffsets",
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
  "GET     /k/topics/:topic/metrics  TopicMetrics"
]
```
