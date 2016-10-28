# Kafka topics

API to manage Kafka topics.

### Run

```
cat .env
B=*********
K=*********
T=*********
METRICS_HOST=*********
METRICS_TOKEN=*********
```

```
docker run --rm -p 4242:4242 \
  --env-file ..env \
  -v /var/run/docker.sock:/var/run/docker.sock \
  krkr/kafka-topics
```

### API

```
# Kafka
POST    /k/topics/:topic?p=3&r=2  CreateTopic
GET     /k/topics/:topic          GetTopic
GET     /k/topics/:topic?p=6      UpdateTopic
DELETE  /k/topics/:topic          DeleteTopic
GET     /k/topics                 ListTopics
GET     /k/topics/:topic/metrics  TopicMetrics
GET     /k/offsets                Offsets

# Zk
GET     /z/topics                 ZkListTopics
GET     /z/consumers              ZkListConsumer
```
