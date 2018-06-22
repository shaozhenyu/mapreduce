package mapreduce

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"
)

type Worker struct {
	lock sync.Mutex

	name     string
	l        net.Listener
	shutDown bool
	Map      func(string, string) []KeyValue
	Reduce   func(string, []string) string
}

type DoTaskArgs struct {
	Phase            JobPhase
	JobName          string
	File             string
	TaskNumber       int
	NumberOtherPhase int
}

func (wk *Worker) register(masterAddress string) error {
	args := &RegisterArgs{Worker: wk.name}
	ok := call(masterAddress, "Master.Register", args, &struct{}{})
	if !ok {
		return fmt.Errorf("Register RPC worker to Master error")
	}
	return nil
}

func (wk *Worker) DoTask(args *DoTaskArgs, _ *struct{}) error {
	switch args.Phase {
	case mapPhase:
		doMap(args.JobName, args.File, args.TaskNumber, args.NumberOtherPhase, wk.Map)
	case reducePhase:
		doReduce(args.JobName, args.TaskNumber, args.NumberOtherPhase, wk.Reduce)
	}
	fmt.Printf("%s: %v task #%d done\n", wk.name, args.Phase, args.TaskNumber)
	return nil
}

func (wk *Worker) ShutDown(_ *struct{}, _ *struct{}) error {
	wk.lock.Lock()
	defer wk.lock.Unlock()
	wk.shutDown = true
	wk.l.Close()
	return nil
}

func RunWorker(
	masterAddress string,
	workerName string,
	mapF func(string, string) []KeyValue,
	reduceF func(string, []string) string,
) (err error) {
	if masterAddress == "" || workerName == "" {
		err = fmt.Errorf("masterAddress and workName should not be nil")
		return
	}
	wk := new(Worker)
	wk.name = workerName
	wk.shutDown = false

	wk.Map = mapF
	wk.Reduce = reduceF

	rpcs := rpc.NewServer()
	rpcs.Register(wk)
	os.Remove(wk.name)
	l, err := net.Listen("unix", wk.name)
	if err != nil {
		log.Fatalf("net.Listen error(%v)", err)
	}
	wk.l = l

	err = wk.register(masterAddress)
	if err != nil {
		log.Fatalf("wk.register error(%v)", err)
	}

	for {
		wk.lock.Lock()
		if wk.shutDown {
			fmt.Printf("Worker(%s) exit\n", wk.name)
			wk.lock.Unlock()
			return
		}
		wk.lock.Unlock()

		conn, err := wk.l.Accept()
		if err == nil {
			go rpcs.ServeConn(conn)
		} else {
			wk.lock.Lock()
			if !wk.shutDown {
				fmt.Printf("l.Accept error(%v)\n", err)
				wk.lock.Unlock()
				break
			}
			wk.lock.Unlock()
		}
	}
	wk.l.Close()
	return
}
