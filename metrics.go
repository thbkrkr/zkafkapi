package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

func TopicMetrics(c *gin.Context) {
	topic := c.Param("topic")

	metrics, err := getMetrics(conf.MetricsHost, conf.MetricsToken, topic, "3", "")
	if handlHTTPErr(c, err) {
		return
	}

	c.JSON(200, metrics)
}

type Query struct {
	Start   int64         `json:"start"`
	Queries []MetricQuery `json:"queries"`
}

type MetricQuery struct {
	Metric     string            `json:"metric"`
	Aggregator string            `json:"aggregator"`
	Downsample string            `json:"downsample"`
	Tags       map[string]string `json:"tags"`
}

func getMetrics(host, token string, topic string, minutes string, downsampling string) (interface{}, error) {
	d := "30"
	if downsampling != "" {
		d = downsampling
	}
	m := 10
	if minutes != "" {
		m, _ = strconv.Atoi(minutes)
	}

	q := Query{
		Start: time.Now().Add(time.Duration(-1*m) * time.Minute).Unix(),
		Queries: []MetricQuery{
			MetricQuery{
				Metric:     "requests.messages-in-per-topic.m1",
				Aggregator: "sum",
				Downsample: d + "s-avg",
				Tags: map[string]string{
					"topic-name": topic,
				},
			},
			MetricQuery{
				Metric:     "responses.messages-out-per-topic.m1",
				Aggregator: "sum",
				Downsample: d + "s-avg",
				Tags: map[string]string{
					"topic-name": topic,
				},
			},
		},
	}

	body, err := json.Marshal(q)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", host+"/api/query", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	auth := strings.Split(token, ":")
	req.SetBasicAuth(auth[0], auth[1])
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("Invalid status code: %d body: %s", resp.StatusCode, string(body))
	}

	if len(body) == 0 {
		return nil, errors.New("Empty body")
	}

	var v interface{}
	err = json.Unmarshal(body, &v)
	if err != nil {
		return nil, err
	}

	return v, nil
}
