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
	Phase        int
	X            int
	Pid          int
	File         string
	Done         bool
	TempFile     string
	TempFilePair KeyValue
	Worker       *TaskWorker
}

type CoordinatorReply struct {
	Y        int
	Phase    int
	Done     bool
	File     string
	FilePair KeyValue
	RObj     ReduceObj
}

type ReduceObj struct {
	nth   int
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
