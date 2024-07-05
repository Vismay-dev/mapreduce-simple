package main

import (
	"fmt"
	"log"
	"os"
	"plugin"
	"strconv"

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

	for i := 0; i < numWorkers; i++ {
		go mapreduce.Worker(mapf, reducef)
	}

	select {}
}


func loadPlugin(filename string) (
	mapf 		func(string, string)   []mapreduce.KeyValue,
	reducef 	func(string, []string) []string,
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

	mapf 	= xmapf.(func(string, string) []mapreduce.KeyValue)
	reducef	= xreducef.(func(string, []string) []string)

	return mapf, reducef
}