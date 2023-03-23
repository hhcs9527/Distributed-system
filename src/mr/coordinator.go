package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	files              *[]string
	groups             *[]string
	phase              int // 0 => map, 1 => intermediate, 2 => reduce
	mu                 sync.Mutex
	taskCnt            int
	mapCnt             int
	reduceCnt          int
	intermediate       bool
	intermediateStatus int // Describe intermediate with bit 1 => not run, 2 => running , 4 => done
	done               bool
	mScheduluer        Scheduluer
	rScheduluer        Scheduluer
}

// define status

// control the list of files name to read/write for map and reduce
func popFromQueue(files *[]string, args *WorkerArgs) bool {
	if len(*files) > 0 {
		file := (*files)[len(*files)-1]
		*files = (*files)[:len(*files)-1]
		args.File = file
		fmt.Printf("I read, id : %v, file: %v\n", args.Pid, args.File)
		return true
	}
	return false
}

func (c *Coordinator) pushToQueue(files *[]string, file string) {
	c.mu.Lock()
	*files = append(*files, file)
	c.mu.Unlock()
}

func (c *Coordinator) RunIntermediatePhase(x int) {
	// intermediate := []KeyValue{}
}

func (c *Coordinator) CompleteIntermediate() bool {
	return (c.intermediateStatus & 4) == 4
}

func (c *Coordinator) RunningIntermediate() bool {
	return (c.intermediateStatus & 2) == 2
}

func (c *Coordinator) SetIntermediateStatus(status int) {
	c.mu.Lock()
	c.intermediateStatus = status
	c.mu.Unlock()
}

func (c *Coordinator) IncreaseSchedulerTaskCnt(s *Scheduluer) {
	c.mu.Lock()
	s.taskCnt++
	c.mu.Unlock()
}

func (c *Coordinator) DecidePhase(args *WorkerArgs) int {
	mapPhase, intermediatePhase, reducePhase, idle := 0, 1, 2, 3
	phase := mapPhase
	fmt.Printf("c.mScheduluer.CompleteTaskDispatch(args) : %v\n", c.mScheduluer.CompleteTaskDispatch(args))
	fmt.Printf("c.mScheduluer.Done() : %v\n", c.mScheduluer.Done())
	if !c.mScheduluer.Done() {
		popFromQueue(c.files, args)
		return c.mScheduluer.GetCurrentPhase(args)
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
	fmt.Printf("c.rScheduluer.CompleteTaskDispatch(args) : %v\n", c.rScheduluer.CompleteTaskDispatch(args))
	fmt.Printf("c.rScheduluer.Done() : %v\n", c.rScheduluer.Done())
	if !c.rScheduluer.Done() {
		popFromQueue(c.groups, args)
		return c.rScheduluer.GetCurrentPhase(args)
	}
	phase = idle
	return phase
}

func (c *Coordinator) DispatchTask(args *WorkerArgs, reply *CoordinatorReply) error {
	// Variables for phase change
	intermediatePhase, reducePhase := 1, 2
	running, complete := 2, 4

	reply.Y = args.X
	// fmt.Printf("[Coordinator] Before decide phase : %v, reply.Done: %v, with pid : %v\n", args.Phase, args, args.Pid)

	args.File = ""
	reply.Done = false

	c.mu.Lock()
	args.Phase = c.DecidePhase(args)
	fmt.Printf("[Coordinatorc] Current phase : %v, reply.Done: %v, with pid : %v\n", args.Phase, args.X, args.Pid)
	c.mu.Unlock()

	if args.Phase == intermediatePhase {
		fmt.Printf("coord is run intermediate, pid : %v\n", args.Pid)
		c.SetIntermediateStatus(running)
		c.RunIntermediatePhase(1)
		c.SetIntermediateStatus(complete)

		// Get the data for the reduce phase
		g := []string{"Japan", "Australia", "Germany"}
		c.groups = &g
		c.rScheduluer = Scheduluer{&g, reducePhase, 0, len(g)}
	}

	// switch phase := args.Phase; phase {
	// case mapPhase:
	// 	fmt.Printf("call func 5 %v\n", args.mapf)
	// 	go func() {

	// 		fmt.Printf("run map, pid : %v\n", args.Pid)
	// 		mpf <- ExecMap(args, reply)
	// 	}()
	// case intermediatePhase:
	// 	fmt.Printf("coord is run intermediate, pid : %v\n", args.Pid)
	// 	c.SetIntermediateStatus(running)
	// 	c.RunIntermediatePhase(1)
	// 	c.SetIntermediateStatus(complete)

	// 	// Get the data for the reduce phase
	// 	g := []string{"Japan", "Australia", "Germany"}
	// 	c.groups = &g
	// 	c.rScheduluer = Scheduluer{&g, reducePhase, 0, len(g)}
	// 	return nil
	// case reducePhase:
	// 	go func() {
	// 		fmt.Println("run reduce")
	// 		rdf <- ExecReduce(args, reply)
	// 	}()
	// case idle:
	// 	fmt.Printf("idle the worker, pid : %v\n\n\n", args.Pid)
	// 	time.Sleep(time.Second * time.Duration(5))
	// 	return nil
	// }

	// for {
	// 	select {
	// 	case <-mpf: // Work in time.
	// 		fmt.Printf("Nicly done Map, id : %v, file: %v\n", args.Pid, args.File)
	// 		c.IncreaseSchedulerTaskCnt(&c.mScheduluer)
	// 		fmt.Printf("Nicly done, id : %v, file: %v, cnt : %v\n\n\n", args.Pid, args.File, c.mScheduluer.taskCnt)
	// 		return nil

	// 	case <-rdf: // Work in time.
	// 		fmt.Printf("Nicly done Reduce, id : %v, file: %v\n", args.Pid, args.File)
	// 		c.IncreaseSchedulerTaskCnt(&c.rScheduluer)
	// 		fmt.Printf("Nicly done, id : %v, file: %v, cnt : %v\n\n\n", args.Pid, args.File, c.rScheduluer.taskCnt)
	// 		return nil

	// 	case <-ticker:
	// 		fmt.Printf("Not done, baddddddd, id : %v, file: %v\n\n\n", args.Pid, args.File)
	// 		if args.Phase == mapPhase {
	// 			c.pushToQueue(c.files, args.File)
	// 		} else if args.Phase == reducePhase {
	// 			c.pushToQueue(c.groups, args.File)
	// 		}
	// 		return nil
	// 	}
	// }
	return nil
}

func (c *Coordinator) ReceiveTaskExecResult(args *WorkerArgs, reply *CoordinatorReply) error {
	fmt.Printf("[Coordinator] ReportTaskExecResult with phase : %v, args: %v, with pid : %v\n", args.Phase, args, args.Pid)
	mapPhase, reducePhase := 0, 2
	if args.Done {
		if args.Phase == mapPhase {
			c.IncreaseSchedulerTaskCnt(&c.mScheduluer)
			fmt.Printf("Nicly done, id : %v, file: %v, cnt : %v\n\n\n", args.Pid, args.File, c.mScheduluer.taskCnt)
		} else if args.Phase == reducePhase {
			c.IncreaseSchedulerTaskCnt(&c.rScheduluer)
			fmt.Printf("Nicly done, id : %v, file: %v, cnt : %v\n\n\n", args.Pid, args.File, c.rScheduluer.taskCnt)
		}
	} else {
		if args.Phase == mapPhase {
			c.pushToQueue(c.files, args.File)
			fmt.Printf("Not done map, baddddddd, id : %v, file: %v\n\n\n", args.Pid, args.File)
		} else if args.Phase == reducePhase {
			c.pushToQueue(c.groups, args.File)
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
	return false
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.files = &files
	c.mScheduluer = Scheduluer{&files, 0, 0, len(files)}

	c.server()
	return &c
}
