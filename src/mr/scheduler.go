package mr

import "sync"

// Helper class to manage the phase change
type Scheduluer struct {
	queue        *[]KeyValue
	reduceQueue  *[]ReduceObj
	phase        int // 0 => map, 1 => intermediate, 2 => reduce
	taskCnt      int
	totalTaskCnt int
	mu           sync.Mutex
}

func (s *Scheduluer) CompleteTaskDispatch(reply *CoordinatorReply) bool {
	s.mu.Lock()
	mapPhase, completeDispatch := 0, false
	if s.phase == mapPhase {
		// fmt.Printf("checking map phase.., *s.queue : %v\n", *s.queue)
		// fmt.Printf("checking map phase.., len(*s.queue) : %v, reply.FilePair.Value : %v\n", len(*s.queue), reply.FilePair.Value)
		completeDispatch = len(*s.queue) == 0 && reply.FilePair.Value == ""
	} else {
		// fmt.Println("checking reduce phase...")
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
