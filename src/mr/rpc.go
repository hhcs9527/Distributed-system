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

type WorkerArgs struct {
	Phase    int
	Done     bool
	TempFile string
	FilePair KeyValue
	RObj     ReduceObj
}

type CoordinatorReply struct {
	Phase    int
	Done     bool
	FilePair KeyValue
	RObj     ReduceObj
}

type ReduceObj struct {
	Key   string
	Value []string
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
