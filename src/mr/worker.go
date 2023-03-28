package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type TaskWorker struct {
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
	ofile   os.File
	mu      sync.Mutex
}

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	w := TaskWorker{}
	w.mapf = mapf
	w.reducef = reducef

	args := WorkerArgs{}
	reply := CoordinatorReply{}

	tmpFile := "mr-out-" + strconv.Itoa(os.Getpid()) + ".txt"
	ofile, err := os.Create(tmpFile)
	w.ofile = *ofile
	defer w.ofile.Close()
	if err != nil {
		panic(err)
	}

	for !reply.Done {
		w.RequestTask(&args, &reply)

		if reply.Phase == intermediatePhase || (reply.Phase == idle && (reply.FilePair.Key == "" && reply.RObj.Key == "")) {
			time.Sleep(taskIdleTime * time.Second)
			continue
		}

		if reply.FilePair.Key != "" {
			reply.Phase = mapPhase
		} else if reply.RObj.Key == "" {
			reply.Phase = reducePhase
		}

		w.ExecTask(&args, &reply)
		w.ReportTaskExecResult(&args, &reply)
	}
	return
}

// Ask coordinator for task, expect to get the phase of a job
func (w *TaskWorker) RequestTask(args *WorkerArgs, reply *CoordinatorReply) {
	call("Coordinator.DispatchTask", args, reply)
}

func (w *TaskWorker) ExecTask(args *WorkerArgs, reply *CoordinatorReply) error {
	// Counter for the map / reduce function
	ch := make(chan bool)
	ticker := time.After(taskExecTime * time.Second)

	// Variables for phase change
	mapPhase, reducePhase := 0, 2
	switch phase := reply.Phase; phase {
	case mapPhase:
		go func() {
			ch <- w.ExecMap(args, reply)
		}()
	case reducePhase:
		go func() {
			ch <- w.ExecReduce(args, reply)
		}()
	}

	for {
		select {
		case completeTask := <-ch: // Work in time.
			args.Done = completeTask
			return nil

		case <-ticker:
			args.Done = false
			return nil
		}
	}
}

func (w *TaskWorker) ExecMap(args *WorkerArgs, reply *CoordinatorReply) bool {
	args.Done = false
	file, err := os.Open(reply.FilePair.Value)
	if err != nil {
		log.Fatalf("cannot open %v", reply.FilePair.Value)
		return false
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.FilePair.Value)
		return false
	}
	file.Close()

	kva := w.mapf(reply.FilePair.Value, string(content))
	// write to tmp file
	tmpFile := "mr-" + reply.FilePair.Key + ".txt"
	file, err = os.Create(tmpFile)

	enc := json.NewEncoder(file)
	for _, kv := range kva {
		err := enc.Encode(&kv)
		if err != nil {
			panic(err)
		}
	}
	file.Close()
	args.TempFile = tmpFile
	args.FilePair = reply.FilePair
	reply.FilePair = KeyValue{}
	return true
}

func (w *TaskWorker) ExecReduce(args *WorkerArgs, reply *CoordinatorReply) bool {
	output := w.reducef(reply.RObj.Key, reply.RObj.Value)
	if len(output) != 0 {
		fmt.Fprintf(&w.ofile, "%v %v\n", reply.RObj.Key, output)
	}
	args.RObj = reply.RObj
	reply.RObj = ReduceObj{}
	return true
}

func (w *TaskWorker) ReportTaskExecResult(args *WorkerArgs, reply *CoordinatorReply) {
	args.Phase = reply.Phase
	call("Coordinator.ReceiveTaskExecResult", args, reply)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args *WorkerArgs, reply *CoordinatorReply) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
