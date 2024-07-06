package mapreduce

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"regexp"
	"strconv"
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
	workerId   string
	status     TaskStatus
	fileNames  []string
	assignedAt time.Time
}

type Coordinator struct {
	mu sync.Mutex
	wg sync.WaitGroup

	done bool

	mapTasks    map[int]*TaskInfo
	reduceTasks map[int]*TaskInfo
}

var timeOut = time.Second * 10
var intRegex = regexp.MustCompile("[0-9]+")

func (c *Coordinator) monitorTaskAssignments() {
	for {
		c.mu.Lock()
		if c.done {
			c.mu.Unlock()
			break
		}

		timeNow := time.Now()

		// check MAP tasks
		for taskId, taskInfo := range c.mapTasks {
			if taskInfo.status == IN_PROGRESS && timeNow.Sub(taskInfo.assignedAt) > timeOut {
				log.Printf(
					"found a stuck MAP task {task-id:%d} @ {worker-id:%s}; marking it as failed",
					taskId,
					taskInfo.workerId,
				)
				taskInfo.status = FAILED
			}
		}

		// check REDUCE tasks
		for taskId, taskInfo := range c.reduceTasks {
			if taskInfo.status == IN_PROGRESS && timeNow.Sub(taskInfo.assignedAt) > timeOut {
				log.Printf(
					"found a stuck REDUCE task {task-id:%d} @ {worker-id:%s}; marking it as failed",
					taskId,
					taskInfo.workerId,
				)
				taskInfo.status = FAILED
			}
		}

		c.mu.Unlock()
		time.Sleep(10 * time.Second)
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

func (c *Coordinator) mapTasksDone() bool {
	for _, task := range c.mapTasks {
		if task.status != COMPLETED {
			return false
		}
	}
	return true
}

func (c *Coordinator) reduceTasksDone() bool {
	for _, task := range c.reduceTasks {
		if task.status != COMPLETED {
			return false
		}
	}
	return true
}

func getReduceTaskId(filename string) int {
	matches := intRegex.FindAllString(filename, -1)
	if len(matches) == 2 {
		reduceTaskId, _ := strconv.Atoi(matches[1])
		return reduceTaskId
	}
	log.Fatalf("error parsing intermediate filename: %s", filename)
	return -1
}

func (c *Coordinator) createReduceTask(filename string) *TaskInfo {
	task := &TaskInfo{}
	task.status = IDLE
	task.fileNames = []string{filename}
	return task
}

func (c *Coordinator) updateReduceTasks(filenames []string) {
	for _, intermediate_file := range filenames {
		id := getReduceTaskId(intermediate_file)
		reduceTask, ok := c.reduceTasks[id]
		if !ok {
			c.reduceTasks[id] = c.createReduceTask(intermediate_file)
		} else {
			reduceTask.fileNames = append(reduceTask.fileNames, intermediate_file)
		}
	}
}

// RPC INTERFACE FUNCS

func (c *Coordinator) AssignTask(taskRequest *TaskRequest, taskAssignment *TaskAssigment) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for taskId, taskInfo := range c.mapTasks {
		if taskInfo.status == IDLE || taskInfo.status == FAILED {
			taskInfo.assignedAt = time.Now()
			taskInfo.status = IN_PROGRESS
			taskInfo.workerId = taskRequest.WorkerId
			taskAssignment.Filenames = taskInfo.fileNames
			taskAssignment.Type = MAP
			taskAssignment.TaskId = taskId
			fmt.Printf("MAP task {%d} assigned to {worker-id: %s}", taskAssignment.TaskId, taskInfo.workerId)
			return nil
		}
	}

	if c.mapTasksDone() {
		for taskId, taskInfo := range c.mapTasks {
			if taskInfo.status == IDLE || taskInfo.status == FAILED {
				taskInfo.assignedAt = time.Now()
				taskInfo.status = IN_PROGRESS
				taskInfo.workerId = taskRequest.WorkerId
				taskAssignment.Filenames = taskInfo.fileNames
				taskAssignment.Type = REDUCE
				taskAssignment.TaskId = taskId
				fmt.Printf("REDUCE task {%d} assigned to {worker-id: %s}", taskAssignment.TaskId, taskInfo.workerId)
				return nil
			}
		}
	}

	return nil
}

func (c *Coordinator) TaskDone(notification *TaskCompletionNotification, taskDoneAck *TaskCompletionAck) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if notification.Type == MAP {
		taskInfo := c.mapTasks[notification.TaskId]
		if taskInfo.status == COMPLETED {
			fmt.Printf("Completion notification received from {worker: %s} for already completed task (%d); ignoring", notification.WorkerId, notification.TaskId)
		} else {
			taskInfo.status = COMPLETED
			fmt.Printf("MAP task {%d} @ {worker: %s} completed!", notification.TaskId, notification.WorkerId)
			c.updateReduceTasks(notification.Filenames)
		}
		taskDoneAck.ack = true
	} else {
		taskInfo := c.reduceTasks[notification.TaskId]
		taskInfo.status = COMPLETED
		fmt.Printf("REDUCE task {%d} @ {worker: %s} completed!", notification.TaskId, notification.WorkerId)
		taskDoneAck.ack = true
	}

	return nil
}

// PUBLIC FUNCTIONS (API)

func (c *Coordinator) Done() bool {
	c.mu.Lock()
	if c.mapTasksDone() && c.reduceTasksDone() {
		c.done = true
		c.mu.Unlock()
		c.wg.Wait()
		return true
	} else {
		c.done = false
		c.mu.Unlock()
		return false
	}
}

func StartCoordinator(input_files []string) *Coordinator {
	c := &Coordinator{}

	c.mapTasks = make(map[int]*TaskInfo, len(input_files))
	for i, filename := range input_files {
		taskInfo := &TaskInfo{}
		taskInfo.status = IDLE
		taskInfo.fileNames = []string{filename}
		c.mapTasks[i+1] = taskInfo
	}

	c.reduceTasks = make(map[int]*TaskInfo)
	c.done = false

	c.server()

	log.Printf("Started coordinator")

	c.wg.Add(1)
	go c.monitorTaskAssignments()

	return c
}
