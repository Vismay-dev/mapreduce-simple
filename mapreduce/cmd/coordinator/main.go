package main

import (
	"fmt"
	"os"
	"time"

	"github.com/Vismay-dev/mapreduce-simple/mapreduce"
)

// usage: ./cmd/coordinator/main.go inputfiles...
func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "usage: ./cmd/coordinator/main.go inputfiles...")
		os.Exit(1)
	}

	input_files := os.Args[1:]
	c := mapreduce.StartCoordinator(input_files, 8)

	for !c.Done() {
		time.Sleep(time.Second)
	}

	time.Sleep(time.Second)
}
