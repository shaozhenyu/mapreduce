package mapreduce

import (
	"fmt"
)

type JobPhase string

const (
	mapPhase    JobPhase = "map"
	reducePhase          = "reduce"
)

type KeyValue struct {
	Key   string
	Value string
}

func ReduceName(jobName string, mapTask, reduceTask int) string {
	return fmt.Sprintf("mrtmp.%s-%d-%d", jobName, mapTask, reduceTask)
}

func MergeName(jobName string, reduceTask int) string {
	return fmt.Sprintf("mrtmp.%s-res-%d", jobName, reduceTask)
}
