package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"time"
)

type TaskWorker struct {
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
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

	// Your worker implementation here.
	w := TaskWorker{}
	w.mapf = mapf
	w.reducef = reducef

	args := WorkerArgs{}
	reply := CoordinatorReply{}

	intermediatePhase, idle := 1, 3

	// uncomment to send the Example RPC to the coordinator.
	isDone := false
	count := 1
	fmt.Printf("[worker] Init args : %v\n", args)
	for isDone == false {
		w.RequestTask(&args, &reply)
		fmt.Printf("[worker] Current phase : %v, reply.Done: %v, with pid : %v\n", args, reply.Done, args.Pid)
		if args.Phase == intermediatePhase || args.Phase == idle {
			time.Sleep(2)
			continue
		}
		w.ExecTask(&args, &reply)
		w.ReportTaskExecResult(&args, &reply)
		fmt.Printf("call %v times! result %v\n", count, isDone)
		count++
		// break
	}
}

// Ask coordinator for task, expect to get the phase of a job
func (w *TaskWorker) RequestTask(args *WorkerArgs, reply *CoordinatorReply) {
	args.Phase = 781
	args.X = 8
	args.Done = false
	args.Pid = os.Getpid()
	fmt.Printf("[worker in RequestTask] Current phase : %v, reply.Done: %v, with pid : %v\n", args.Phase, args, args.Pid)
	call("Coordinator.DispatchTask", args, reply)
}

func (w *TaskWorker) ExecTask(args *WorkerArgs, reply *CoordinatorReply) error {
	// Counter for the map / reduce function
	ch := make(chan bool)
	ticker := time.After(5 * time.Second)

	// Variables for phase change
	mapPhase, reducePhase := 0, 2

	switch phase := args.Phase; phase {
	case mapPhase:
		go func() {
			fmt.Printf("run map, pid : %v\n", args.Pid)
			ch <- w.ExecMap(args, reply)
		}()
	case reducePhase:
		go func() {
			fmt.Printf("run reduce, pid : %v\n", args.Pid)
			ch <- w.ExecReduce(args, reply)
		}()
	}

	for {
		select {
		case <-ch: // Work in time.
			reply.Done = true
			args.Done = true
			return nil

		case <-ticker:
			reply.Done = false
			args.Done = false
			return nil
		}
	}
}

func (w *TaskWorker) ExecMap(args *WorkerArgs, reply *CoordinatorReply) bool {
	// args.Done = false
	fmt.Printf("call func 3 %v\n", w.mapf)
	fmt.Printf("I sleep here in call ExecMap with: %v, with pid : %v\n", args.File, args.Pid)
	time.Sleep(time.Second * time.Duration(1))
	// file, err := os.Open(args.File)
	// if err != nil {
	// 	log.Fatalf("cannot open %v", args.File)
	// 	return false
	// }
	// content, err := ioutil.ReadAll(file)
	// if err != nil {
	// 	log.Fatalf("cannot read %v", args.File)
	// 	return false
	// }
	// file.Close()
	// fmt.Printf("call f %v", args.mapf)
	// kva := args.mapf(args.File, string(content))

	// // write into a filec
	// // iFileName := "mr-intermediate-0"
	// // ifile, _ := os.Create(iFileName)

	// for i := 0; i < len(kva); i++ {
	// 	k, v := kva[0].Key, kva[0].Value
	// 	fmt.Printf("Key : %v, Val: %v\n", k, v)
	// }
	return true
}

func (w *TaskWorker) ExecReduce(args *WorkerArgs, reply *CoordinatorReply) bool {
	// args.Done = false
	fmt.Printf("call func 3 %v\n", w.reducef)
	fmt.Printf("I sleep here in call ExecReduce for %v sec, with pid : %v\n", args.X, args.Pid)
	time.Sleep(time.Second * time.Duration(1))
	return true
}

func (w *TaskWorker) ReportTaskExecResult(args *WorkerArgs, reply *CoordinatorReply) bool {
	fmt.Printf("[worker] ReportTaskExecResult with phase : %v, reply.Done: %v, with pid : %v\n\n\n", args.Phase, reply.Done, args.Pid)
	ok := call("Coordinator.ReceiveTaskExecResult", args, reply)
	if ok {
		// reply.Y should be 100.
		// fmt.Printf("reply.Y %v\n", reply.Y)
		return reply.Done
	} else {
		// fmt.Printf("call failed!\n")
		return false
	}
	return true
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
