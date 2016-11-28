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
  "GET     /lag                  Lag"
]

```
