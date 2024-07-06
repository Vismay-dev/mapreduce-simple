package mapreduce

import (
	"fmt"
	"log"
	"math/rand"
	"net/rpc"
	"time"
)

var workerId string

func Worker(
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) []string,
) {
	workerId = randomString(6)
	log.Printf("Started worker: {%s}", workerId)

	for {
		task, err := GetTask()
		if err != nil {
			log.Fatal("couldn't connect to coordinator, quitting...")
			break
		}
		if task.Type == MAP {
			runMapOp(task)
		} else {
			runReduceOp(task)
		}
		time.Sleep(5 * time.Second)
	}
}

func GetTask() (*TaskAssigment, error) {
	taskRequest := &TaskRequest{}
	taskRequest.WorkerId = workerId
	taskAssignment := &TaskAssigment{}
	if call("Coordinator.AssignTask", taskRequest, taskAssignment) {
		return taskAssignment, nil
	} else {
		return nil, fmt.Errorf("couldn't connect to Coordinator")
	}
}

func runMapOp(task *TaskAssigment) {

}

func runReduceOp(task *TaskAssigment) {

}

func call(rpcname string, args interface{}, res interface{}) bool {
	sockName := coordinatorSocket()
	c, err := rpc.DialHTTP("unix", sockName)
	if err != nil {
		log.Fatal("error dialing HTTP @ unix socket: ", err)
	}
	defer c.Close()

	err = c.Call(rpcname, &args, &res)
	if err == nil {
		return true
	}

	log.Fatal("error calling RPC service method: ", err)
	return false
}

func randomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}
