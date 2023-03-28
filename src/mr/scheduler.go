package mr

import (
	"sync"
)

// Helper class to manage the phase change
type Scheduluer struct {
	queue        *map[string]KeyValue
	reduceQueue  *map[string]ReduceObj
	phase        int // 0 => map, 1 => intermediate, 2 => reduce
	taskCnt      int
	totalTaskCnt int
	mu           sync.Mutex
}

func (s *Scheduluer) CompleteTaskDispatch(reply *CoordinatorReply) bool {
	s.mu.Lock()
	// // // fmt.Printf("checking reply : %v\n", reply.Phase)
	completeDispatch := false
	if s.phase == mapPhase {
		// // fmt.Printf("checking map phase.., *s.queue : %v\n", *s.queue)
		// // fmt.Printf("checking map phase..,w len(*s.queue) : %v, reply.FilePair : %v\n", len(*s.queue), reply.FilePair)
		completeDispatch = len(*s.queue) == 0 && reply.FilePair.Value == ""
	} else {
		// // // fmt.Printf("checking reducewc phase.., *s.reduceQueue : %v\n", *s.reduceQueue)
		// // fmt.Printf("checking map reduce.., len(*s.reduceQueue) : %v, reply.RObj.Key : %v\n", len(*s.reduceQueue), reply.RObj.Key)
		completeDispatch = len(*s.reduceQueue) == 0 && reply.RObj.Key == ""
	}
	s.mu.Unlock()

	return completeDispatch
}

func (s *Scheduluer) CompleteTask() bool {
	s.mu.Lock()
	complete := s.taskCnt == s.totalTaskCnt
	s.mu.Unlock()
	return complete
}

func (s *Scheduluer) Done() bool {
	return s.CompleteTask()
}

func (s *Scheduluer) GetCurrentPhase(reply *CoordinatorReply) int {
	idle := 3
	if s.CompleteTaskDispatch(reply) {
		return idle
	}
	return s.phase
}
