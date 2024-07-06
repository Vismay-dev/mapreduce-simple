package mapreduce

import (
	"os"
	"strconv"
)

type TaskType string

const (
	REDUCE TaskType = "REDUCE"
	MAP    TaskType = "MAP"
)

type TaskRequest struct {
	WorkerId string
}

type TaskAssignment struct {
	Filenames []string
	Type      TaskType
	TaskId    int
	NReduce   int
}

type TaskCompletionNotification struct {
	Filenames []string
	Type      TaskType
	TaskId    int
	WorkerId  string
}

type TaskCompletionAck struct {
	Ack bool
}

func coordinatorSocket() string {
	path := "/var/tmp/vismay-mapreduce-"
	path += strconv.Itoa(os.Getuid())
	return path
}
