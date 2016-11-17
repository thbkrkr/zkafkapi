package main

import "github.com/gin-gonic/gin"

type ConsumerGroupsOffsetsLag struct {
	Total      ConsumerOffsetsLag
	Partitions map[int32]ConsumerOffsetsLag
}

type ConsumerOffsetsLag struct {
	LogSize int64
	Offset  int64
	Lag     int64
}

func Lag(c *gin.Context) {
	lag := map[string]map[string]*ConsumerGroupsOffsetsLag{}

	consumersOffsetsMutex.RLock()
	for topic, _ := range consumerTopicsOffsets {
		for consumer, _ := range consumerTopicsOffsets[topic] {
			for partition, consumerOffset := range consumerTopicsOffsets[topic][consumer] {

				topicsOffsetsMutex.RLock()
				logSize := topicsOffsets[topic][partition]
				topicsOffsetsMutex.RUnlock()

				pLag := ConsumerOffsetsLag{
					LogSize: logSize,
					Offset:  consumerOffset,
					Lag:     logSize - consumerOffset,
				}

				tLag := lag[topic]
				if tLag == nil {
					tLag = map[string]*ConsumerGroupsOffsetsLag{}
				}

				cLag := tLag[consumer]
				if cLag == nil {
					cLag = &ConsumerGroupsOffsetsLag{
						Total:      ConsumerOffsetsLag{},
						Partitions: map[int32]ConsumerOffsetsLag{},
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

	c.JSON(200, lag)
}
