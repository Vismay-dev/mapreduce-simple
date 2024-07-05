package main

import (
	"fmt"
	"log"
	"os"
	"plugin"
	"sort"

	"github.com/Vismay-dev/mapreduce-simple/mapreduce"
)

// usage: cmd/client/main.go xxx.so inputfiles...
func main() {
	if len(os.Args) < 2 {
		log.Fatal("usage: cmd/client/main.go xxx.so inputfiles...")
		return
	}

	Sequential()
}


func Sequential() {
	plugin := os.Args[1]
	input_files := os.Args[2:]

	mapf, reducef := loadPlugin(plugin)

	kvPairs := []mapreduce.KeyValue{}

	for _, filename := range input_files {
		content, err := mapreduce.ReadFile(filename)
		if err != nil {
			log.Fatal("error reading contents from file: ", err)
		}
		mappings := mapf(filename, content)
		kvPairs = append(kvPairs, mappings...)
	}

	sort.Sort(mapreduce.ByKey(kvPairs))
	ofile, err := os.Create("./outputs/mr-out-0")
	if err != nil {
		log.Fatal("error creating output file: ", err)
	}

	for i := 0; i < len(kvPairs); {
		j := i + 1
		for j < len(kvPairs) && kvPairs[i].Key == kvPairs[j].Key {
			j += 1
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kvPairs[k].Value)
		}
		key := kvPairs[i].Key
		output := reducef(key, values)[0]
		fmt.Fprintf(ofile, "%v %v\n", key, output)
		i = j
	}
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