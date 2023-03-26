package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type Coordinator struct {
	groups                  []string
	tmpFiles                []string
	mapFilePairs            []KeyValue
	reduceObjPairs          []ReduceObj
	mapFilePairsInProcess   []KeyValue
	reduceObjPairsInProcess []ReduceObj
	phase                   int // 0 => map, 1 => intermediate, 2 => reduce
	mu                      sync.Mutex
	intermediateStatus      int // Describe intermediate with bit 1 => not run, 2 => running , 4 => done
	mScheduluer             Scheduluer
	rScheduluer             Scheduluer
}

func (c *Coordinator) Example(args *WorkerArgs, reply *CoordinatorReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) InitScheduler(phase int) {
	c.mu.Lock()
	mapPhase := 0
	if phase == mapPhase {
		// fmt.Printf("c.mapFilePairs : %v \n\n", c.mapFilePairs)
		c.mScheduluer.phase = phase
		c.mScheduluer.taskCnt = 0
		c.mScheduluer.queue = &c.mapFilePairs
		c.mScheduluer.totalTaskCnt = len(c.mapFilePairs)
	} else {
		c.rScheduluer.phase = phase
		c.rScheduluer.taskCnt = 0
		c.rScheduluer.reduceQueue = &c.reduceObjPairs
		c.rScheduluer.totalTaskCnt = len(c.reduceObjPairs)
	}
	c.mu.Unlock()
}

func (c *Coordinator) MakeFilePairs(files []string) []KeyValue {
	var filePairs []KeyValue
	for i, file := range files {
		filePairs = append(filePairs, KeyValue{strconv.Itoa(i), file})
	}
	return filePairs
}

// control the list of files name to read/write for map and reduce
func popFromReduceQueue(ReduceObjs *[]ReduceObj, reply *CoordinatorReply) bool {
	reply.RObj = ReduceObj{}
	if len(*ReduceObjs) > 0 {
		robj := (*ReduceObjs)[len(*ReduceObjs)-1]
		*ReduceObjs = (*ReduceObjs)[:len(*ReduceObjs)-1]
		reply.RObj = robj
		// // fmt.Printf("I read, file : %v", reply.File)
		return true
	}
	return false
}

func popFromQueue(filePairs *[]KeyValue, reply *CoordinatorReply) bool {
	reply.FilePair = KeyValue{}
	if len(*filePairs) > 0 {
		filePair := (*filePairs)[len(*filePairs)-1]
		*filePairs = (*filePairs)[:len(*filePairs)-1]
		reply.FilePair = filePair
		// // fmt.Printf("I read, file pair : %v", reply.FilePair)
		return true
	}
	return false
}

func (c *Coordinator) pushToReduceQueue(ReduceObjs *[]ReduceObj, RObj ReduceObj) {
	c.mu.Lock()
	*ReduceObjs = append(*ReduceObjs, RObj)
	c.mu.Unlock()
}

func (c *Coordinator) pushToQueue(filePairs *[]KeyValue, filePair KeyValue) {
	c.mu.Lock()
	*filePairs = append(*filePairs, filePair)
	c.mu.Unlock()
}

// func (c *Coordinator) RunIntermediatePhase() {
// 	// // fmt.Printf("\n\nRunIntermediatePhase...\n\n")
// 	intermediate := []KeyValue{}
// 	for _, filename := range c.tmpFiles {
// 		file, err := os.Open(filename)
// 		if err != nil {
// 			log.Fatalf("cannot open %v", filename)
// 		}
// 		dec := json.NewDecoder(file)
// 		for {
// 			var kv KeyValue
// 			if err := dec.Decode(&kv); err != nil {
// 				break
// 			}
// 			intermediate = append(intermediate, kv)
// 		}
// 		file.Close()
// 	}

// 	// sort
// 	sort.Sort(ByKey(intermediate))

// 	i := 0
// 	count := 0
// 	for i < len(intermediate) {
// 		j := i + 1
// 		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
// 			j++
// 		}

// 		values := []string{}
// 		key := ""
// 		for k := i; k < j; k++ {
// 			values = append(values, intermediate[k].Value)
// 			key = intermediate[k].Key
// 		}

// 		c.reduceObjPairs = append(c.reduceObjPairs, ReduceObj{count, key, values})
// 		i = j
// 		count++
// 	}
// }

func (c *Coordinator) CompleteIntermediate() bool {
	return (c.intermediateStatus & 4) == 4
}

func (c *Coordinator) RunningIntermediate() bool {
	return (c.intermediateStatus & 2) == 2
}

// func (c *Coordinator) SetIntermediateStatus(status int) {
// 	c.mu.Lock()
// 	c.intermediateStatus = status
// 	c.mu.Unlock()
// }

func (c *Coordinator) IncreaseSchedulerTaskCnt(s *Scheduluer, args *WorkerArgs) {
	mapPhase := 0
	c.mu.Lock()
	s.taskCnt++
	if s.phase == mapPhase {
		c.tmpFiles = append(c.tmpFiles, args.TempFile)
	}
	c.mu.Unlock()
}

func (c *Coordinator) DecidePhase(args *WorkerArgs, reply *CoordinatorReply) int {
	mapPhase, intermediatePhase, reducePhase, idle := 0, 1, 2, 3
	phase := mapPhase
	fmt.Printf("c.mScheduluer.CompleteTaskDispatch(args) : %v\n", c.mScheduluer.CompleteTaskDispatch(reply))
	fmt.Printf("c.mScheduluer.Done() : %v\n", c.mScheduluer.Done())
	if !c.mScheduluer.Done() {
		popFromQueue(&c.mapFilePairs, reply)
		return c.mScheduluer.GetCurrentPhase(reply)
	}
	fmt.Printf("c.CompleteIntermediate : %v\n", c.CompleteIntermediate())
	fmt.Printf("c.RunningIntermediate : %v\n", c.RunningIntermediate())
	phase = intermediatePhase
	if !c.CompleteIntermediate() {
		if c.RunningIntermediate() {
			phase = idle
		}
		return phase
	}
	phase = reducePhase
	fmt.Printf("c.rScheduluer.CompleteTaskDispatch(args) : %v\n", c.rScheduluer.CompleteTaskDispatch(reply))
	fmt.Printf("c.rScheduluer.Done() : %v\n", c.rScheduluer.Done())
	if !c.rScheduluer.Done() {
		popFromReduceQueue(&c.reduceObjPairs, reply)
		return c.rScheduluer.GetCurrentPhase(reply)
	}
	phase = idle
	return phase
}

func (c *Coordinator) DispatchTask(args *WorkerArgs, reply *CoordinatorReply) error {
	// Variables for phase change
	// intermediatePhase, reducePhase := 1, 2
	// running, complete := 2, 4

	fmt.Printf("[Coordinator] Before decide phase : %v, reply.Done: %v, with pid : %v\n", args.Phase, args, args.Pid)

	// c.mu.Lock()
	// args.Phase = c.DecidePhase(args, reply)
	// fmt.Printf("[Coordinatorc in DispatchTask] Current arge : %v, reply: %v, with pid : %v\n\n\n", args, reply, args.Pid)
	// reply.Phase = args.Phase
	// c.phase = reply.Phase
	// c.mu.Unlock()

	// if reply.Phase == intermediatePhase {
	// 	// // fmt.Printf("coord is run intermediate, pid : %v\n", args.Pid)
	// 	c.SetIntermediateStatus(running)
	// 	c.RunIntermediatePhase()
	// 	c.InitScheduler(reducePhase)
	// 	c.SetIntermediateStatus(complete)
	// }

	return nil

	// ch := make(chan bool)
	// ticker := time.After(10 * time.Second)

	// // Variables for phase change
	// mapPhase, reducePhase := 0, 2

	// switch phase := reply.Phase; phase {
	// case mapPhase:
	// 	go func() {
	// 		ch <- args.Worker.ExecMap(args, reply)
	// 	}()
	// case reducePhase:
	// 	go func() {
	// 		ch <- args.Worker.ExecReduce(args, reply)
	// 	}()
	// }

	// for {
	// 	select {
	// 	case completeTask := <-ch: // Work in time.
	// 		reply.Done = completeTask
	// 		c.ReceiveTaskExecResult(args, reply)
	// 		return nil

	// 	case <-ticker:
	// 		// reply.Done = false
	// 		reply.Done = false
	// 		c.ReceiveTaskExecResult(args, reply)
	// 		return nil
	// 	}
	// }
}
func (c *Coordinator) ReceiveTaskExecResult(args *WorkerArgs, reply *CoordinatorReply) error {
	// // fmt.Printf("[Coordinator in ReportTaskExecResult]  with reply : %v, args: %v, with pid : %v\n", reply, args, args.Pid)
	mapPhase, reducePhase := 0, 2
	if reply.Done {
		if reply.Phase == mapPhase {
			c.IncreaseSchedulerTaskCnt(&c.mScheduluer, args)
			fmt.Printf("Nicly done map, id : %v, file: %v, c.tmpFiles: %v,  cnt : %v\n\n\n", args.Pid, args.File, c.tmpFiles, c.mScheduluer.taskCnt)
		} else if reply.Phase == reducePhase {
			c.IncreaseSchedulerTaskCnt(&c.rScheduluer, args)
			fmt.Printf("Nicly done reduce, id : %v, file: %v, reply: %v, cnt : %v\n\n\n", args.Pid, args.File, reply, c.rScheduluer.taskCnt)
		}
	} else {
		if reply.Phase == mapPhase {
			c.pushToQueue(&c.mapFilePairs, reply.FilePair)
			fmt.Printf("Not done map, baddddddd, id : %v, file: %v\n\n\n", args.Pid, args.File)
		} else if reply.Phase == reducePhase {
			c.pushToReduceQueue(&c.reduceObjPairs, reply.RObj)
			fmt.Printf("Not done reduce, baddddddd, id : %v, file: %v\n\n\n", args.Pid, args.File)
		}
	}
	// success => counter ++
	// else => push back to other
	return nil
}

func (c *Coordinator) ReceiveTaskExecResult1(args *WorkerArgs, reply *CoordinatorReply) error {
	// // fmt.Printf("[Coordinator in ReportTaskExecResult]  with reply : %v, args: %v, with pid : %v\n", reply, args, args.Pid)
	mapPhase, reducePhase := 0, 2
	if args.Done {
		if args.Phase == mapPhase {
			c.IncreaseSchedulerTaskCnt(&c.mScheduluer, args)
			fmt.Printf("Nicly done map, id : %v, file: %v, c.tmpFiles: %v,  cnt : %v\n\n\n", args.Pid, args.File, c.tmpFiles, c.mScheduluer.taskCnt)
		} else if args.Phase == reducePhase {
			c.IncreaseSchedulerTaskCnt(&c.rScheduluer, args)
			fmt.Printf("Nicly done reduce, id : %v, file: %v, reply: %v, cnt : %v\n\n\n", args.Pid, args.File, reply, c.rScheduluer.taskCnt)
		}
	} else {
		if args.Phase == mapPhase {
			c.pushToQueue(&c.mapFilePairs, reply.FilePair)
			fmt.Printf("Not done map, baddddddd, id : %v, file: %v\n\n\n", args.Pid, args.File)
		} else if args.Phase == reducePhase {
			c.pushToReduceQueue(&c.reduceObjPairs, reply.RObj)
			fmt.Printf("Not done reduce, baddddddd, id : %v, file: %v\n\n\n", args.Pid, args.File)
		}
	}
	// success => counter ++
	// else => push back to other
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	isDone := false
	if c.rScheduluer.totalTaskCnt > 0 {
		isDone = c.rScheduluer.Done()
	}
	c.mu.Unlock()
	fmt.Print("Hi done func \n")
	return isDone
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.mapFilePairs = c.MakeFilePairs(files)
	c.mScheduluer = Scheduluer{}
	c.rScheduluer = Scheduluer{}
	c.InitScheduler(0)
	c.rScheduluer.totalTaskCnt = -1
	c.server()
	return &c
}
