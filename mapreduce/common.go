package mapreduce

import (
	"fmt"
	"io"
	"os"
)

type TaskType string

const (
	REDUCE TaskType = "REDUCE"
	MAP    TaskType = "REDUCE"
)

type KeyValue struct {
	Key string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len()			int  { return len(a) }
func (a ByKey) Less(i, j int) 	bool { return a[i].Key < a[j].Key }
func (a ByKey) Swap(i, j int)   	 { a[i], a[j] = a[j], a[i] }

func ReadFile(filename string) (string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return "", fmt.Errorf("error opening file (%s): %s", filename, err)
	}
	defer file.Close()

	content, err := io.ReadAll(file)
	if err != nil {
		return "", fmt.Errorf("error reading file (%v): %s", file, err)
	}

	return string(content), nil
}