package mapreduce

import "os"

func coordinatorSocket() string {
	unixSockPath := "/var/tmp/mapreduce-simple-rpc.sock"
	os.Remove(unixSockPath)
	return unixSockPath
}
