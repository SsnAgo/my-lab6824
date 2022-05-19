package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"


type AskWorkArgs struct {
	WorkId int64
}
type AskWorkReply struct {
	WorkId	 int64
	TaskId	int64
	Filenames []string
	NReduce	 int64
	TaskType int64 // 1 for map  ; 2 for reduce ; 0 for wait ; -1 for close
}

type DoneWorkArgs struct {
	// done workerID
	TaskId int64
	WorkerId	int64
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
