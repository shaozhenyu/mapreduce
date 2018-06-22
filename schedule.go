package mapreduce

import (
	"fmt"
	"sync"
)

func (mr *Master) schedule(phase JobPhase) {
	var ntasks, nops int
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nops = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nops = len(mr.files)
	}

	fmt.Println("Schedule workers :", phase)
	var wg sync.WaitGroup
	wg.Add(ntasks)
	for i := 0; i < ntasks; i++ {
		go mr.handleWork(phase, i, nops, &wg)
	}
	wg.Wait()
}

func (mr *Master) handleWork(phase JobPhase, task, numOhterPhase int, wg *sync.WaitGroup) {
	workName := <-mr.registerChannel
	registerArgs := &DoTaskArgs{
		Phase:            phase,
		JobName:          mr.jobName,
		TaskNumber:       task,
		NumberOtherPhase: numOhterPhase,
	}

	if phase == mapPhase {
		registerArgs.File = mr.files[task]
	}

	if ok := call(workName, "Worker.DoTask", registerArgs, &struct{}{}); !ok {
		go mr.handleWork(phase, task, numOhterPhase, wg)
	} else {
		wg.Done()
		mr.registerChannel <- workName
	}
}
