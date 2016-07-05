# Topics info

Tools to get info about Kafka topics.

### List topics

```
> d run --rm krkr/kafka-topics -broker k1.c1.blurb.space:9092 | jq .
[
  "__consumer_offsets",
  "thb-1.botopik",
  "thb-1.zuperbotopik"
  "qaastor-42.ping",
]
```

### Get topic

```
> d run --rm krkr/kafka-topics -broker k1.c1.blurb.space:9092 -topic qaastor-42.ping | jq .
{
  "Name": "qaastor-42.ping",
  "Partitions": {
    "0": 5672999,
    "1": 5670647,
    "2": 5669446,
    "3": 1749620,
    "4": 1744987,
    "5": 1748227,
    "6": 1748466,
    "7": 1747886,
    "8": 1751454
  }
}

```