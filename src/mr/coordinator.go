package mr

import (
	"encoding/json"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

var mapPhase = 0
var intermediatePhase = 1
var reducePhase = 2
var idle = 3
var taskCrashTime = time.Duration(12)
var taskIdleTime = time.Duration(2)
var taskExecTime = time.Duration(10)

type Coordinator struct {
	tmpFiles                []string
	mapFilePairsKeys        []string
	reduceObjPairsKeys      []string
	mapFilePairsMap         map[string]KeyValue
	reduceObjPairsMap       map[string]ReduceObj
	finishMapFilePairsMap   map[string]KeyValue
	finishReduceObjPairsMap map[string]ReduceObj
	phase                   int // 0 => map, 1 => intermediate, 2 => reduce
	mu                      sync.Mutex
	intermediateStatus      int // Describe intermediate with bit 1 => not run, 2 => running , 4 => done
	mScheduluer             Scheduluer
	rScheduluer             Scheduluer
	mapCh                   chan KeyValue
	reduceCh                chan ReduceObj
}

func (c *Coordinator) InitScheduler(phase int) {
	c.mu.Lock()
	mapPhase := 0
	if phase == mapPhase {
		c.mScheduluer.phase = phase
		c.mScheduluer.taskCnt = 0
		c.mScheduluer.queue = &c.mapFilePairsMap
		c.mScheduluer.totalTaskCnt = len(c.mapFilePairsMap)
	} else {
		c.rScheduluer.phase = phase
		c.rScheduluer.taskCnt = 0
		c.rScheduluer.reduceQueue = &c.reduceObjPairsMap
		c.rScheduluer.totalTaskCnt = len(c.reduceObjPairsMap)
	}
	c.mu.Unlock()
}

func (c *Coordinator) MakeFilePairs(files []string) {
	for i, file := range files {
		c.mapFilePairsKeys = append(c.mapFilePairsKeys, strconv.Itoa(i))
		c.mapFilePairsMap[strconv.Itoa(i)] = KeyValue{strconv.Itoa(i), file}
	}
}

func (c *Coordinator) popFromQueue(reply *CoordinatorReply) bool {
	reply.FilePair = KeyValue{}
	if len(c.mapFilePairsMap) > 0 && len(c.mapFilePairsKeys) > 0 {
		filePair := c.mapFilePairsMap[c.mapFilePairsKeys[0]]
		delete(c.mapFilePairsMap, c.mapFilePairsKeys[0])
		c.mapFilePairsKeys = c.mapFilePairsKeys[1:]
		reply.FilePair = filePair
		reply.Phase = mapPhase
		c.mapCh <- filePair
		return true
	}
	return false
}

func (c *Coordinator) popFromReduceQueue(reply *CoordinatorReply) bool {
	reply.RObj = ReduceObj{}
	if len(c.reduceObjPairsMap) > 0 && len(c.reduceObjPairsKeys) > 0 {
		robj := c.reduceObjPairsMap[c.reduceObjPairsKeys[0]]
		delete(c.reduceObjPairsMap, c.reduceObjPairsKeys[0])
		c.reduceObjPairsKeys = c.reduceObjPairsKeys[1:]
		reply.RObj = robj
		reply.Phase = reducePhase
		c.reduceCh <- robj
		return true
	}
	return false
}

func (c *Coordinator) pushToReduceQueue(RObj ReduceObj, isFinish bool) {
	if isFinish {
		c.finishReduceObjPairsMap[RObj.Key] = RObj
	} else {
		c.reduceObjPairsMap[RObj.Key] = RObj
	}
}

func (c *Coordinator) pushToQueue(filePair KeyValue, isFinish bool) {
	if isFinish {
		c.finishMapFilePairsMap[filePair.Key] = filePair
	} else {
		c.mapFilePairsMap[filePair.Key] = filePair
	}
}

func (c *Coordinator) RunIntermediatePhase() {
	intermediate := []KeyValue{}
	for _, filename := range c.tmpFiles {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	// sort
	sort.Sort(ByKey(intermediate))

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}

		values := []string{}
		key := ""
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
			key = intermediate[k].Key
		}

		c.reduceObjPairsMap[key] = ReduceObj{key, values}
		c.reduceObjPairsKeys = append(c.reduceObjPairsKeys, key)
		i = j
	}
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

func (c *Coordinator) IncreaseSchedulerTaskCnt(s *Scheduluer, args *WorkerArgs, reply *CoordinatorReply) {
	s.taskCnt++
	if s.phase == mapPhase {
		c.tmpFiles = append(c.tmpFiles, args.TempFile)
		c.pushToQueue(args.FilePair, true)
	} else {
		c.pushToReduceQueue(args.RObj, true)
		reply.Done = c.rScheduluer.Done()
	}
}

func (c *Coordinator) DecidePhase(args *WorkerArgs, reply *CoordinatorReply) int {
	phase := mapPhase
	if !c.mScheduluer.Done() {
		c.popFromQueue(reply)
		return c.mScheduluer.GetCurrentPhase(reply)
	}
	phase = intermediatePhase
	if !c.CompleteIntermediate() {
		if c.RunningIntermediate() {
			phase = idle
		}
		return phase
	}
	phase = reducePhase
	if !c.rScheduluer.Done() {
		c.popFromReduceQueue(reply)
		return c.rScheduluer.GetCurrentPhase(reply)
	}
	phase = idle
	return phase
}

func (c *Coordinator) DispatchTask(args *WorkerArgs, reply *CoordinatorReply) error {
	// Variables for phase change
	running, complete := 2, 4

	c.mu.Lock()
	phase := c.DecidePhase(args, reply)
	reply.Phase = phase
	c.phase = phase
	c.mu.Unlock()

	if reply.Phase == intermediatePhase {
		c.SetIntermediateStatus(running)
		c.RunIntermediatePhase()
		c.InitScheduler(reducePhase)
		c.SetIntermediateStatus(complete)
	}

	return nil
}

func (c *Coordinator) ReceiveTaskExecResult(args *WorkerArgs, reply *CoordinatorReply) error {
	if args.Done {
		if args.Phase == mapPhase {
			c.mu.Lock()
			c.IncreaseSchedulerTaskCnt(&c.mScheduluer, args, reply)
			c.mu.Unlock()
		} else if args.Phase == reducePhase {
			c.mu.Lock()
			c.IncreaseSchedulerTaskCnt(&c.rScheduluer, args, reply)
			c.mu.Unlock()
		}
	}
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
	return isDone
}

func (c *Coordinator) taskTracker() {
	for {
		select {
		case task := <-c.mapCh:
			timer := time.NewTimer(taskCrashTime * time.Second)
			go func() {
				<-timer.C
				c.mu.Lock()
				if _, ok := c.finishMapFilePairsMap[task.Key]; !ok {
					c.pushToQueue(task, false)
					c.mapFilePairsKeys = append(c.mapFilePairsKeys, task.Key)
				}
				c.mu.Unlock()
			}()
		case task := <-c.reduceCh:
			timer := time.NewTimer(taskCrashTime * time.Second)
			go func() {
				<-timer.C
				c.mu.Lock()
				if _, ok := c.finishReduceObjPairsMap[task.Key]; !ok {
					c.pushToReduceQueue(task, false)
					c.reduceObjPairsKeys = append(c.reduceObjPairsKeys, task.Key)
				}
				c.mu.Unlock()
			}()
		}
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// init Coordinator
	c := Coordinator{
		mScheduluer:             Scheduluer{},
		rScheduluer:             Scheduluer{},
		mapCh:                   make(chan KeyValue, 100),
		reduceCh:                make(chan ReduceObj, 100),
		mapFilePairsMap:         make(map[string]KeyValue),
		reduceObjPairsMap:       make(map[string]ReduceObj),
		finishMapFilePairsMap:   make(map[string]KeyValue),
		finishReduceObjPairsMap: make(map[string]ReduceObj),
	}
	c.MakeFilePairs(files)
	c.InitScheduler(mapPhase)
	c.rScheduluer.totalTaskCnt = -1

	// start listening
	go c.taskTracker()
	c.server()
	return &c
}
