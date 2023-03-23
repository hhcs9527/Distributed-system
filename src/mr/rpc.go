package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type WorkerArgs struct {
	X          int
	PID        int
	Done       bool
	file       string
	mapf       *func(string, string) []KeyValue
	reducef    *func(string, []string) string
	State      string
	phase      int
	taskWorker *TaskWorker
}

type CoordinatorReply struct {
	Y    int
	Done bool
}

type MapArgs struct {
	X    int
	PID  int
	Done bool
	file string
}

type MapReply struct {
	Y    int
	Done bool
}

type ReduceArgs struct {
	X    int
	PID  int
	Done bool
	file string
}

type ReduceReply struct {
	Y    int
	Done bool
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
