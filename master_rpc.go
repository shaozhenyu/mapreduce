package mapreduce

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
)

func (mr *Master) startRPCServer() {
	rpcs := rpc.NewServer()
	rpcs.Register(mr)
	os.Remove(mr.address)
	l, err := net.Listen("unix", mr.address)
	if err != nil {
		log.Fatalf("net.Listen error(%v)", err)
	}

	mr.l = l
	go func() {
	loop:
		for {
			select {
			case <-mr.shutdown:
				break loop
			default:
			}
			conn, err := mr.l.Accept()
			if err != nil {
				fmt.Printf("mr.l.Accept error(%v)\n", err)
				break
			}
			go func() {
				rpcs.ServeConn(conn)
				conn.Close()
			}()
		}
	}()
	fmt.Println("Master startRPCServer finish")
}

func (mr *Master) stopRPCServer() {
	if !call(mr.address, "Master.Shutdown", &struct{}{}, &struct{}{}) {
		fmt.Printf("Cleanup: RPC %s error\n", mr.address)
	}
}

func (mr *Master) Shutdown(_ *struct{}, _ *struct{}) error {
	fmt.Println("Shutdown: registration server")
	close(mr.shutdown)
	mr.l.Close()
	return nil
}
