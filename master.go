package mapreduce

import (
	"fmt"
	"net"
	"sync"
)

type Master struct {
	lock sync.Mutex

	address         string
	registerChannel chan string
	doneChannel     chan bool
	workers         []string // protected by the mutex

	jobName string   // Name of currently executing job
	files   []string // Input files
	nReduce int      // Number of reduce partitions

	shutdown chan struct{}
	l        net.Listener
	stats    []int
}

func (mr *Master) Register(args *RegisterArgs, _ *struct{}) error {
	mr.lock.Lock()
	defer mr.lock.Unlock()

	fmt.Printf("Master(%s) Regsiter work(%s)\n", mr.address, args.Worker)
	if len(mr.workers) == 0 {
		mr.workers = make([]string, 0)
	}
	mr.workers = append(mr.workers, args.Worker)
	go func() {
		mr.registerChannel <- args.Worker
	}()
	return nil
}

func newMaster(masterName string) (mr *Master) {
	mr = new(Master)
	mr.address = masterName
	mr.shutdown = make(chan struct{})
	mr.registerChannel = make(chan string)
	mr.doneChannel = make(chan bool)
	return
}

func RunMaster(masterName, jobName string, files []string, nReduce int) {
	mr := newMaster(masterName)
	mr.startRPCServer()

	go mr.run(jobName, files, nReduce, mr.schedule, func() {
		mr.stopWorkers()
		mr.stopRPCServer()
	})

	mr.wait()
	mr.cleanupFiles()
}

func (mr *Master) run(jobName string, files []string, nReduce int, schedule func(phase JobPhase), finish func()) {
	mr.jobName = jobName
	mr.files = files
	mr.nReduce = nReduce

	schedule("map")
	schedule("reduce")
	finish()
	mr.merge()
	fmt.Printf("%s: Map/Reduce task completed\n", mr.address)
	mr.doneChannel <- true
}

func (mr *Master) wait() {
	<-mr.doneChannel
}

func (mr *Master) stopWorkers() {
	mr.lock.Lock()
	defer mr.lock.Unlock()
	for _, worker := range mr.workers {
		if ok := call(worker, "Worker.ShutDown", &struct{}{}, &struct{}{}); !ok {
			fmt.Printf("Master: RPC Call error\n")
		}
	}
}
