package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
)
import "log"
import "net/rpc"
import "hash/fnv"

// save its worker id when first \call for work
var workerId int64

type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func ihash(key string) int64 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int64(h.Sum32() & 0x7fffffff)
}



func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// 1. call for work
	for {
		//time.Sleep(time.Second)
		reply, ok := CallForWork()
		// ok means rpc call success
		if !ok { continue }
		// no task
		if reply.TaskType == 0 { continue }
		// coordinator exit   worker done
		if reply.TaskType == -1 { return  }

		var outfiles []string
		if reply.TaskType == 1 {
			outfiles = domap(mapf, reply)
		}
		if reply.TaskType == 2 {
			outfiles = doreduce(reducef, reply)
		}
		// send message to announce workerID done, and no matter if its map or reduce
		doneWorkArgs := DoneWorkArgs{
			TaskId: reply.TaskId,
			WorkerId:  workerId,
			Filepaths: outfiles,
		}
		CallForDone(doneWorkArgs)
	}
}

func doreduce(reducef func(string, []string) string, reply AskWorkReply) (outfiles []string){
	filenames := reply.Filenames
	Y := reply.TaskId
	files := make([]*os.File, len(filenames))

	oname := fmt.Sprintf("*-mr-out-%v", Y)
	ofile, _ := ioutil.TempFile("./",oname)
	outfiles = []string{ofile.Name()}

	for i, filename := range filenames {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatal("open file error : %s, err : %v\n", filename, err)
			return
		}
		files[i] = file
	}
	all_kvs := make(ByKey, 0)
	for _, file := range files {
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			all_kvs = append(all_kvs, kv)
		}
		file.Close()
	}
	sort.Sort(all_kvs)

	i := 0
	for i < len(all_kvs) {
		j := i + 1
		for j < len(all_kvs) && all_kvs[i].Key == all_kvs[j].Key {
			j++
		}
		values := []string{}
		for k := i; k < j;k++ {
			values = append(values, all_kvs[k].Value)
		}
		output := reducef(all_kvs[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n",all_kvs[i].Key, output)
		i = j
	}
	ofile.Close()
	return outfiles

}

func domap(mapf func(string, string) []KeyValue, reply AskWorkReply) (outfiles []string) {
	filenames := reply.Filenames
	//workerId := reply.WorkId
	taskId := reply.TaskId
	nReduce := reply.NReduce
	// save kvsa to reduce file  create nreduce file per worker
	mrXYs := make([]*os.File, nReduce)
	encs := make([]*json.Encoder, nReduce)
	// init encoder and out-files
	for i := 0; i < int(nReduce);i++ {
		// delete old file
		fname := fmt.Sprintf("*-mr-%d-%d", taskId, i)

		os.Remove(fname)
		mrXY, err := ioutil.TempFile("./",fname)
		if err != nil {
			log.Fatal("create file error : %s, err : %v", fname, err)
			return
		}
		outfiles = append(outfiles, mrXY.Name())
		mrXYs[i] = mrXY
		encs[i] = json.NewEncoder(mrXY)
	}
	defer func() {
		for i := 0; i < int(nReduce);i++ {
			mrXYs[i].Close()
		}
	}()
	// read all task file and encode the content, then write to out-files
	files := make([]*os.File, len(filenames))
	for i, filename := range filenames {
		file, err := os.Open(filename)
		files[i] = file
		if err != nil {
			log.Fatal("cannot open %s", filename)
			return
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatal("cannot read %s", filename)
			return
		}
		file.Close()
		kva := mapf(filename, string(content))
		// write to nreduce buckets
		// and write to file
		for _, kv := range kva {
			reduceId := ihash(kv.Key) % nReduce
			encs[reduceId].Encode(&kv)
		}
		//fmt.Printf("one file done: %s\n", filename)
	}
	// close files
	defer func() {
		for i := 0; i < len(filenames);i++ {
			files[i].Close()
		}
	}()
	//fmt.Printf("map task done, outputfiles:%v, workerId:%v\n", outfiles, workerId)
	return outfiles

}

func CallForWork() (AskWorkReply, bool) {
	args := AskWorkArgs{workerId}
	reply := AskWorkReply{}
	ok := call("Coordinator.WorkSend", &args,&reply)
	//fmt.Println(ok, reply)
	if ok {
		if workerId == 0 { workerId = reply.WorkId }
		return reply, ok
	}
	return AskWorkReply{}, ok
}

func CallForDone(doneWorkArgs DoneWorkArgs) (DoneWorkReply, error) {
	reply := DoneWorkReply{}
	ok := call("Coordinator.WorkDone", &doneWorkArgs,&reply)
	if ok{
		return reply, nil
	}
	return DoneWorkReply{}, errors.New("call done fail")
}

func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
