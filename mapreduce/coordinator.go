package mapreduce

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"
)

type TaskStatus string

const (
	IDLE        TaskStatus = "IDLE"
	IN_PROGRESS TaskStatus = "IN_PROGRESS"
	COMPLETED   TaskStatus = "COMPLETED"
	FAILED      TaskStatus = "FAILED"
)

type TaskInfo struct {
	taskId     int
	status     TaskStatus
	fileNames  []string
	assignedAt time.Time
}

type Coordinator struct {
	mu sync.Mutex
	wg sync.WaitGroup

	done bool

	mapTasks    map[int]TaskInfo
	reduceTasks map[int]TaskInfo
}

func (c *Coordinator) monitorTaskAssignments() {
	for {
		c.mu.Lock()
		if c.done {
			c.mu.Unlock()
			break
		}

		// check MAP tasks

		// check REDUCE tasks
	}
	c.wg.Done()
}

func (c *Coordinator) server() {
	rpc.HandleHTTP()
	rpc.Register(c)

	sockname := coordinatorSocket()

	ln, err := net.Listen("unix", sockname)
	if err != nil {
		log.Fatal("error listening on designated socket: ", err)
	}

	go http.Serve(ln, nil)
}

// PUBLIC FUNCTIONS (API)

func (c *Coordinator) Done() bool {
	return c.done
}

func StartCoordinator(input_files []string) *Coordinator {
	c := &Coordinator{}

	c.mapTasks = make(map[int]TaskInfo, len(input_files))
	for i, filename := range input_files {
		taskInfo := TaskInfo{}
		taskInfo.taskId = i + 1
		taskInfo.status = IDLE
		taskInfo.fileNames = []string{filename}
		c.mapTasks[i+1] = taskInfo
	}

	c.reduceTasks = make(map[int]TaskInfo)
	c.done = false

	c.server()

	log.Printf("Started coordinator")

	c.wg.Add(1)
	go c.monitorTaskAssignments()

	return c
}
