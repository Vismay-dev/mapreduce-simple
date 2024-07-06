package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"plugin"
	"strconv"
	"sync"
	"time"

	"github.com/Vismay-dev/mapreduce-simple/mapreduce"
)

// usage: ./cmd/worker/main.go xxx.so numWorkers
func main() {
	if len(os.Args) != 3 {
		fmt.Fprintf(os.Stderr, "usage: ./cmd/worker/main.go xxx.so numWorkers")
		os.Exit(1)
	}
	plugin := os.Args[1]
	mapf, reducef := loadPlugin(plugin)

	numWorkers, err := strconv.Atoi(os.Args[2])
	if err != nil {
		log.Fatal("error parsing numWorkers")
	}

	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			workerId := randomString(6)
			mapreduce.RunWorker(mapf, reducef, workerId)
		}()
	}

	wg.Wait()
}

func randomString(length int) string {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[r.Intn(len(charset))]
	}
	return string(b)
}

func loadPlugin(filename string) (
	mapf func(string, string) []mapreduce.KeyValue,
	reducef func(string, []string) []string,
) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatal("error loading plugin: ", err)
	}

	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatal("error looking plugin symbol `Map`: ", err)
	}

	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatal("error looking plugin symbol `Reduce`: ", err)
	}

	mapf = xmapf.(func(string, string) []mapreduce.KeyValue)
	reducef = xreducef.(func(string, []string) []string)

	return mapf, reducef
}
