package mapreduce

import (
	"encoding/json"
	"hash/fnv"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"unicode"
)

func doMap(
	jobName string,
	inFile string,
	ntask int,
	nReduce int,
	mapF func(file string, content string) []KeyValue,
) {
	// read input file
	content, err := ioutil.ReadFile(inFile)
	if err != nil {
		log.Fatalf("ioutil.ReadFile error(%v)", err)
	}
	// create reduce file
	files := make(map[uint32]*os.File)
	for i := 0; i < nReduce; i++ {
		f, err := os.Create(ReduceName(jobName, ntask, i))
		if err != nil {
			log.Fatalf("os.Create error(%v)", err)
		}
		defer f.Close()
		files[uint32(i)] = f
	}

	kvs := mapF(inFile, string(content))
	mKv := make(map[uint32][]KeyValue)
	for _, kv := range kvs {
		index := ihash(kv.Key) % uint32(nReduce)
		if _, ok := mKv[index]; !ok {
			mKv[index] = make([]KeyValue, 0)
		}
		mKv[index] = append(mKv[index], kv)
	}

	for index, val := range mKv {
		enc := json.NewEncoder(files[index])
		if err := enc.Encode(&val); err != nil {
			log.Fatalf("enc.Encode error(%v)", err)
		}
	}
}

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func WCMapF(document string, value string) (res []KeyValue) {
	res = make([]KeyValue, 0)
	vals := strings.FieldsFunc(value, func(r rune) bool {
		return !unicode.IsLetter(r)
	})
	for _, v := range vals {
		res = append(res, KeyValue{v, ""})
	}
	return
}
