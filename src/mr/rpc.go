package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int

}

type AskWorkArgs struct {
	WorkId int
}
type AskWorkReply struct {
	WorkId	 int
	TaskId	int
	Filenames []string
	NReduce	 int
	TaskType int // 1 for map  ; 2 for reduce ; 0 for wait ; -1 for close
}

type DoneWorkArgs struct {
	// done workerID
	TaskId int
	WorkerId	int
	Filepaths	[]string
}
type DoneWorkReply struct {
	//
}


// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
