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

	// uncomment to send the Example RPC to the coordinator.
	isDone := false
	count := 1

	for isDone == false {
		// isDone := CallExample(count)
		isDone := w.RequestJob()
		break
		fmt.Printf("\ncall %v times! result %v\n", count, isDone)
		count++
		// break
	}
	//
}

// Ask coordinator for jobs
func (w *TaskWorker) RequestJob() bool {

	// declare an argument structure.
	args := WorkerArgs{}
	// fill in the argument(s).
	args.X = 1
	args.Done = false
	args.PID = os.Getpid()
	fmt.Printf("call func 2 %v\n", args.mapf)
	args.taskWorker = w
	// declare a reply structure.
	reply := CoordinatorReply{}
	ok := true
	ok = call("Coordinator.DispatchJob", &args, &reply)
	if ok {
		// reply.Y should be 100.
		// fmt.Printf("reply.Y %v\n", reply.Y)
		return reply.Done
	} else {
		// fmt.Printf("call failed!\n")
		return false
	}
}

func ExecMap(args *WorkerArgs, reply *CoordinatorReply) bool {
	args.Done = false
	fmt.Printf("call func 3 %v\n", args.taskWorker.mapf)
	fmt.Printf("I sleep here in call ExecMap for %v sec, with pid : %v\n", args.file, args.PID)
	// file, err := os.Open(args.file)
	// if err != nil {
	// 	log.Fatalf("cannot open %v", args.file)
	// 	return false
	// }
	// content, err := ioutil.ReadAll(file)
	// if err != nil {
	// 	log.Fatalf("cannot read %v", args.file)
	// 	return false
	// }
	// file.Close()
	// fmt.Printf("call f %v", args.mapf)
	// kva := args.mapf(args.file, string(content))

	// // write into a filec
	// // iFileName := "mr-intermediate-0"
	// // ifile, _ := os.Create(iFileName)

	// for i := 0; i < len(kva); i++ {
	// 	k, v := kva[0].Key, kva[0].Value
	// 	fmt.Printf("Key : %v, Val: %v\n", k, v)
	// }
	return true
}

func ExecReduce(args *WorkerArgs, reply *CoordinatorReply) bool {
	args.Done = false
	time.Sleep(time.Second * time.Duration(1))
	fmt.Printf("I sleep here in call ExecReduce for %v sec, with pid : %v\n", args.X, args.PID)
	return true
}

// example function to show how to make an RPC call to the coordinator.
// the RPC argument and reply types are defined in rpc.go.
// func CallExample(count int) bool {

// 	// declare an argument structure.
// 	args := WorkerArgs{}

// 	// fill in the argument(s).
// 	args.X = count
// 	args.Done = false
// 	args.PID = os.Getpid()

// 	// declare a reply structure.
// 	reply := CoordinatorReply{}

// 	// send the RPC request, wait for the reply.
// 	// the "Coordinator.Example" tells the
// 	// receiving server that we'd like to call
// 	// the Example() method of struct Coordinator.
// 	ok := call("Coordinator.Example", &args, &reply)
// 	if ok {
// 		// reply.Y should be 100.
// 		// fmt.Printf("reply.Y %v\n", reply.Y)
// 		return reply.Done
// 	} else {
// 		// fmt.Printf("call failed!\n")
// 		return false
// 	}
// }

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
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
