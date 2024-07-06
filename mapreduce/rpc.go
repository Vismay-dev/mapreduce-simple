package mapreduce

import "os"

type TaskRequest struct {
	WorkerId string
}

type TaskAssigment struct {
	Filenames []string
	Type      TaskType
	TaskId    int
}

type TaskCompletionNotification struct {
	Filenames []string
	Type      TaskType
	TaskId    int
	WorkerId  string
}

type TaskCompletionAck struct {
	ack bool
}

func coordinatorSocket() string {
	unixSockPath := "/var/tmp/mapreduce-simple-rpc.sock"
	os.Remove(unixSockPath)
	return unixSockPath
}
