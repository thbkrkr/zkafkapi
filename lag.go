package main

import "github.com/gin-gonic/gin"

type ConsumerGroupsOffsetsLag struct {
	Total      ConsumerLag
	Partitions map[int32]ConsumerLag
}

type ConsumerLag struct {
	LogSize   int64
	Offset    int64
	Lag       int64
	Timestamp int64
}

func Lag(c *gin.Context) {
	c.JSON(200, getLag())
}

type LagState struct {
	Lag    int64
	Status string
}

func LagSummary(c *gin.Context) {
	summary := map[string]map[string]LagState{}

	lags := getLag()
	for topic := range lags {
		summary[topic] = map[string]LagState{}
		for consumer := range lags[topic] {
			lag := lags[topic][consumer].Total.Lag
			ts := lags[topic][consumer].Total.Timestamp
			status := "ERROR"
			if ts < 30000 && ts > -30000 && lag < 1000 {
				status = "OK"
			}
			summary[topic][consumer] = LagState{
				Lag:    lag,
				Status: status,
			}
		}
	}

	c.JSON(200, summary)
}

func getLag() map[string]map[string]*ConsumerGroupsOffsetsLag {
	lag := map[string]map[string]*ConsumerGroupsOffsetsLag{}

	consumersOffsetsMutex.RLock()
	for topic, _ := range ConsumersTopicsOffsets {
		for consumer, _ := range ConsumersTopicsOffsets[topic] {
			for partition, consumerOffset := range ConsumersTopicsOffsets[topic][consumer] {

				topicsOffsetsMutex.RLock()
				logSize := TopicsOffsets[topic][partition]
				topicsOffsetsMutex.RUnlock()

				pLag := ConsumerLag{
					LogSize:   logSize.Value,
					Offset:    consumerOffset.Value,
					Lag:       logSize.Value - consumerOffset.Value,
					Timestamp: logSize.Timestamp - consumerOffset.Timestamp,
				}

				tLag := lag[topic]
				if tLag == nil {
					tLag = map[string]*ConsumerGroupsOffsetsLag{}
				}

				cLag := tLag[consumer]
				if cLag == nil {
					cLag = &ConsumerGroupsOffsetsLag{
						Total:      ConsumerLag{},
						Partitions: map[int32]ConsumerLag{},
					}
				}

				cLag.Total.LogSize += pLag.LogSize
				cLag.Total.Offset += pLag.Offset
				cLag.Total.Lag += pLag.Lag
				cLag.Partitions[partition] = pLag
				tLag[consumer] = cLag
				lag[topic] = tLag
			}
		}
	}
	consumersOffsetsMutex.RUnlock()

	return lag
}
