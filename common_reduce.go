package mapreduce

import (
	"encoding/json"
	"io"
	"log"
	"os"
	"strconv"
)

func doReduce(
	jobName string,
	ntask int,
	nMap int,
	reduceF func(key string, value []string) string,
) {
	merge := make(map[string][]string)
	for i := 0; i < nMap; i++ {
		rf, err := os.Open(ReduceName(jobName, i, ntask))
		if err != nil {
			log.Fatalf("os.Open error(%v)", err)
		}
		defer rf.Close()
		dec := json.NewDecoder(rf)
		for {
			var kvs []KeyValue
			if err := dec.Decode(&kvs); err == io.EOF {
				break
			} else if err != nil {
				log.Fatalf("dec.Decode error(%v)", err)
			}

			for _, kv := range kvs {
				if _, ok := merge[kv.Key]; !ok {
					merge[kv.Key] = make([]string, 0)
				}
				merge[kv.Key] = append(merge[kv.Key], kv.Value)
			}
		}
	}

	mergeFile, err := os.OpenFile(MergeName(jobName, ntask), os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		log.Fatalf("os.OpenFile error(%v)", err)
	}
	defer mergeFile.Close()

	enc := json.NewEncoder(mergeFile)
	for k, v := range merge {
		if err := enc.Encode(KeyValue{k, reduceF(k, v)}); err != nil {
			log.Fatalf("enc.Encode error(%v)", err)
		}
	}
}

func WCReduceF(key string, values []string) string {
	return strconv.Itoa(len(values))
}
