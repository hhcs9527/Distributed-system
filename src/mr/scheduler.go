package mr

// Helper class to manage the phase change
type Scheduluer struct {
	queue        *[]string
	phase        int // 0 => map, 1 => intermediate, 2 => reduce
	taskCnt      int
	totalTaskCnt int
}

func (s *Scheduluer) CompleteTaskDispatch(args *WorkerArgs) bool {
	return len(*s.queue) == 0 && args.file == ""
}

func (s *Scheduluer) CompleteTask() bool {
	return s.taskCnt == s.totalTaskCnt
}

func (s *Scheduluer) Done() bool {
	return s.CompleteTask()
}

func (s *Scheduluer) GetCurrentPhase(args *WorkerArgs) int {
	idle := 3
	if s.CompleteTaskDispatch(args) {
		return idle
	}
	return s.phase
}
