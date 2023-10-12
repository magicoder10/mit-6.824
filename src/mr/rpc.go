package mr

import (
	"os"
	"strconv"
)

type TaskType string

const (
	MapTaskType    = "Map"
	ReduceTaskType = "Reduce"
	ExitTaskType   = "Exit"
)

type MapTaskReply struct {
	InputFilePath    string
	ReducePartitions int
}

type ReduceTaskReply struct {
	IntermediateFilePaths []string
}

type RequestTaskReply struct {
	TaskType   TaskType
	TaskID     uint64
	MapTask    MapTaskReply
	ReduceTask ReduceTaskReply
}

type ReportMapCompleteArgs struct {
	TaskID                uint64
	IntermediateFilePaths []string
}

type ReportReduceCompleteArgs struct {
	TaskID uint64
}

type Empty struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
