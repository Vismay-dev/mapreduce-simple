package mapreduce

import (
	"log"
	"sync"
)


type TaskStatus string

const (
	IDLE 		TaskStatus = "IDLE"
	IN_PROGRESS TaskStatus = "IN_PROGRESS"
	COMPLETED	TaskStatus = "COMPLETED"
	FAILED		TaskStatus = "FAILED"
)

type TaskInfo struct {
	taskId		int
	taskType	TaskType
	status		TaskStatus
	fileNames 	[]string
}

type Coordinator struct {
	mu		sync.Mutex
	wg		sync.WaitGroup

	done 	bool

	mapTasks 	map[int]TaskInfo
 	reduceTasks map[int]TaskInfo
}

// func (c *Coordinator) monitorTaskAssignments() {
// 	for {
		
// 	}
// }

func (c *Coordinator) Done() bool {
	return c.done
}

func StartCoordinator(input_files []string) *Coordinator {
	c := &Coordinator{}

	c.mapTasks = make(map[int]TaskInfo, len(input_files))
	for i, filename := range input_files {
		taskInfo := TaskInfo{}
		taskInfo.taskId = i+1
		taskInfo.taskType = MAP
		taskInfo.status = IDLE
		taskInfo.fileNames = []string{filename}
		c.mapTasks[i+1] = taskInfo
	}

	c.reduceTasks = make(map[int]TaskInfo)
	c.done = false

	log.Printf("Started coordinator")

	// go c.monitorTaskAssignments()

	return c
}