package mapreduce

import (
	"crypto/md5"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"sort"
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
			runMapOp(mapf, task)
		} else if task.Type == REDUCE {
			runReduceOp(reducef, task)
		}

		time.Sleep(5 * time.Second)
	}
}

func GetTask() (*TaskAssignment, error) {
	taskRequest := &TaskRequest{}
	taskRequest.WorkerId = workerId
	taskAssignment := &TaskAssignment{}
	if call("Coordinator.AssignTask", &taskRequest, &taskAssignment) {
		return taskAssignment, nil
	} else {
		return nil, fmt.Errorf("couldn't connect to Coordinator")
	}
}

// helpers

func runMapOp(mapf func(string, string) []KeyValue, task *TaskAssignment) {
	if len(task.Filenames) != 1 {
		log.Printf("expected 1 file, got %d; incorrect task assignment; ignoring", len(task.Filenames))
		return
	}

	filename := task.Filenames[0]
	content, err := ReadFile(filename)
	if err != nil {
		log.Fatal("error reading file: ", filename)
	}

	keyValPairs := mapf(filename, content)
	intermediateFileNames := []string{}

	encoders := make(map[int]*json.Encoder)

	for i := 0; i < task.NReduce; i++ {
		file, err := os.CreateTemp("intermediates", "map")
		if err != nil {
			log.Fatal("error creating temp intermediate file: ", err)
		}
		intermediateFileNames = append(intermediateFileNames, file.Name())
		enc := json.NewEncoder(file)
		encoders[i] = enc
	}

	for _, keyVal := range keyValPairs {
		id := iHash(keyVal.Key) % task.NReduce
		if err := encoders[id].Encode(&keyVal); err != nil {
			log.Fatal("error encoding intermediate keyval to temp file: ", err)
		}
	}

	for i := 0; i < task.NReduce; i++ {
		intermediateOutputFilename := fmt.Sprintf("./intermediates/mr-%d-%d", task.TaskId, i)
		os.Rename(intermediateFileNames[i], intermediateOutputFilename)
		intermediateFileNames[i] = intermediateOutputFilename
	}

	TaskDone(intermediateFileNames, task.TaskId, MAP)
}

func runReduceOp(reducef func(string, []string) []string, task *TaskAssignment) {
	filenames := task.Filenames

	intermediates := []KeyValue{}
	for _, filename := range filenames {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatal("error opening intermediate file: ", err)
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediates = append(intermediates, kv)
		}
	}

	sort.Sort(ByKey(intermediates))

	oname := fmt.Sprintf("./outputs/mr-out-%d", task.TaskId)
	tmpfile, _ := os.CreateTemp("", "pre-")

	i := 0
	for i < len(intermediates) {
		j := i + 1
		for j < len(intermediates) && intermediates[i].Key == intermediates[j].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediates[k].Value)
		}
		output := reducef(intermediates[i].Key, values)[0]
		fmt.Fprintf(tmpfile, "%v %v\n", intermediates[i].Key, output)
		i = j
	}

	tmpfile.Close()
	os.Rename(tmpfile.Name(), oname)

	TaskDone([]string{oname}, task.TaskId, REDUCE)
}

func TaskDone(filenames []string, taskId int, taskType TaskType) {
	notification := &TaskCompletionNotification{}
	notification.Filenames = filenames
	notification.TaskId = taskId
	notification.Type = taskType
	notification.WorkerId = workerId

	taskDoneAck := &TaskCompletionAck{Ack: false}

	if call("Coordinator.TaskDone", &notification, &taskDoneAck) {
		if !taskDoneAck.Ack {
			log.Panicf("coordinator didn't acknowledge task completion")
		}
	} else {
		log.Panic("error connecting to coordinator...")
	}
}

func call(rpcname string, args interface{}, res interface{}) bool {
	sockName := coordinatorSocket()
	c, err := rpc.DialHTTP("unix", sockName)
	if err != nil {
		log.Fatal("error dialing HTTP @ unix socket: ", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, res)
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

func iHash(key string) int {
	h := md5.New()
	h.Write([]byte(key))
	sum := h.Sum(nil)
	value := binary.BigEndian.Uint32(sum[:4])
	return int(value & 0x7fffffff)
}
