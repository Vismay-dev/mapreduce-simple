package main

import (
	"strconv"
	"strings"
	"unicode"

	"github.com/Vismay-dev/mapreduce-simple/mapreduce"
)

func Map(filename string, contents string) []mapreduce.KeyValue {
	words := strings.FieldsFunc(contents, func(r rune) bool {
		return !unicode.IsLetter(r)
	})

	res := []mapreduce.KeyValue{}
	for _, word := range words {
		res = append(res, mapreduce.KeyValue{Key: word, Value: "1"})
	}

	return res
}

func Reduce(key string, values []string) []string {
	res := strconv.Itoa(len(values))
	return []string{res}
}
