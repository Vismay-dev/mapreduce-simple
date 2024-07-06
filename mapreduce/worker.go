package mapreduce

import (
	"log"
	"math/rand"
)

var workerId string

func Worker(
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) []string,
) {
	workerId = randomString(6)
	log.Printf("Started worker: {%s}", workerId)

}

func randomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}
