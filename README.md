# Kafka topics

API to manage Kafka topics.

### Run

```
cat .env
A=*********
B=kafka.com:9092
METRICS_HOST=*********
METRICS_TOKEN=*********
```

```
docker run -d -p 4242:4242 \
  --net=host \
  --env-file .env \
  -v /var/run/docker.sock:/var/run/docker.sock \
  krkr/kafka-topics
```

### API

```
# -- Kafka --
GET     /k/topics                 ListTopics
GET     /k/topics/:topic          GetTopic
GET     /topics/:topic            FullTopic
POST    /k/topics/:topic?p=3&r=2  CreateTopic
PUT     /k/topics/:topic?p=6      UpdateTopic
DELETE  /k/topics/:topic          DeleteTopic
GET     /k/offsets                KafkaListTopicsOffsets
GET     /k/t/:topic/c/:consumer   KafkaListConsumerOffsets
# -- Zk --
GET     /z/topics                 ZkListTopics
GET     /z/topics/:topic          ZkGetTopic
GET     /z/offsets                ZkListTopicsOffsets
GET     /z/consumers              ZkListConsumersOffsets
 -- Lag --
GET     /lag                      Lag
GET     /lag/status               LagSummary
# -- Metrics --
GET     /k/topics/:topic/metrics  TopicMetrics

```
