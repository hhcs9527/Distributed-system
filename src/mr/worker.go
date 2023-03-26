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
	"time"
)

type TaskWorker struct {
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
	ofile   os.File
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
	args.Worker = &w
	reply := CoordinatorReply{}

	intermediatePhase, idle := 1, 3
	tmpFile := "mr-out-" + strconv.Itoa(os.Getpid()) + ".txt"
	ofile, err := os.Create(tmpFile)
	w.ofile = *ofile
	defer w.ofile.Close()
	if err != nil {
		panic(err)
	}

	// fmt.Printf("[worker] Init reply : %v\n", !reply.Done)
	for {
		fmt.Println("[Worker Before RequestTask] ", &args, &reply)
		w.RequestTask(&args, &reply)
		fmt.Printf("[worker After RequestTask] Current args : %v, reply: %v, with pid : %v\n\n", args, reply, args.Pid)
		if reply.Phase == intermediatePhase || reply.Phase == idle {
			time.Sleep(time.Second * time.Duration(2))
			// // fmt.Printf("[worker Idle]\n\n\n")
			continue
		}
		w.ExecTask(&args, &reply)
		fmt.Printf("[worker After ExecTask] Current args : %v, reply: %v, with pid : %v\n", args, reply, args.Pid)
		w.ReportTaskExecResult(&args, &reply)
		// fmt.Printf("[worker After ReportTaskExecResult] Current args : %v, reply: %v, with pid : %v\n\n", args, reply, args.Pid)
		// fmt.Printf("[worker After ReportTaskExecResult] Init reply : %v\n", !reply.Done)
		// // fmt.Printf("call %v times! result %v\n", count, isDone)
	}
}

func CallExample() {

	// declare an argument structure.
	args := WorkerArgs{}

	// fill in the argument(s).
	// args.X = 9

	// declare a reply structure.
	reply := CoordinatorReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		// // fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		// // fmt.Printf("call failed!\n")
	}
}

// Ask coordinator for task, expect to get the phase of a job
func (w *TaskWorker) RequestTask(args *WorkerArgs, reply *CoordinatorReply) {
	args.Pid = os.Getpid()
	fmt.Printf("[worker in RequestTask] Current args : %v, reply: %v, with pid : %v\n", args, reply, args.Pid)
	call("Coordinator.DispatchTask", args, reply)
}

func (w *TaskWorker) ExecTask(args *WorkerArgs, reply *CoordinatorReply) error {
	// Counter for the map / reduce function
	ch := make(chan bool)
	ticker := time.After(10 * time.Second)

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
			// reply.Done = false
			args.Done = false
			return nil
		}
	}
}

func (w *TaskWorker) ExecMap(args *WorkerArgs, reply *CoordinatorReply) bool {
	// os.Exit(1)
	// args.Done = false
	// // fmt.Printf("call func 3 %v\n", w.mapf)
	fmt.Printf("I sleep here in call ExecMap with: %v, with pid : %v\n", reply.FilePair.Value, args.Pid)
	// time.Sleep(time.Second * time.Duration(2))
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
	defer file.Close()

	enc := json.NewEncoder(file)
	for _, kv := range kva {
		err := enc.Encode(&kv)
		if err != nil {
			panic(err)
		}
	}
	args.TempFile = tmpFile
	// // // fmt.Printf("get tmpFile: %v\n", args.TempFile)
	// // // write into a filec
	// // // iFileName := "mr-intermediate-0"
	// // // ifile, _ := os.Create(iFileName)

	return len(kva) != 0
}

func (w *TaskWorker) ExecReduce(args *WorkerArgs, reply *CoordinatorReply) bool {
	// args.Done = false
	// // fmt.Printf("call func 3 %v\n", w.reducef)
	output := w.reducef(reply.RObj.Key, reply.RObj.Value)
	if len(output) != 0 {
		fmt.Fprintf(&w.ofile, "%v %v\n", reply.RObj.Key, output)
	}
	return len(output) != 0
}

func (w *TaskWorker) ReportTaskExecResult(args *WorkerArgs, reply *CoordinatorReply) {
	// // fmt.Printf("[worker In ReportTaskExecResult] Current args : %v, reply: %v, with pid : %v\n\n\n", args, reply, args.Pid)
	args.Phase = reply.Phase
	call("Coordinator.ReceiveTaskExecResult", args, reply)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args *WorkerArgs, reply *CoordinatorReply) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	fmt.Printf("[worker] call with args : %v, \n", args)
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
