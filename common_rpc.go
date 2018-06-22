package mapreduce

import (
	"fmt"
	"net/rpc"
)

type RegisterArgs struct {
	Worker string
}

func call(srv, rpcName string, args interface{}, reply interface{}) bool {
	r, err := rpc.Dial("unix", srv)
	if err != nil {
		fmt.Printf("rpc.Dail %s(%s) error(%v)\n", srv, rpcName, err)
		return false
	}
	defer r.Close()

	err = r.Call(rpcName, args, reply)
	if err != nil {
		fmt.Printf("rpc.Call %s(%s) error(%v)\n", srv, rpcName, err)
		return false
	}
	return true
}
