package mr

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"




type Coordinator struct {
	// status, 0: alldone , 1: map, 2: reduce
	Status  int64// 1:map  2:reduce
	// amounts
	NReduce int64
	NMapTask    int64
	NReduceTask int64
	// counter
	DoneCount int64
	// auto-inc worker id
	NextWorkerId int64
	// it the worker alive, if timeout then add the workerid to this map, if done workerid in this, ignore !
	MapWorkerDead sync.Map
	ReduceWorkerDead sync.Map
	// taskId: taskFiles
	MapTasks    sync.Map
	ReduceTasks    sync.Map
	// working tasks  taskId:WorkingTask
	WorkingTasks sync.Map
	// time ticker

}

type Task struct {
	taskId	int64
	taskFiles []string
}
type WorkingTask struct {
	worker    int64
	task *Task
}

func (c *Coordinator) WorkSend(args *AskWorkArgs, reply *AskWorkReply) error {
	var task *Task
	// get worker id, if workerid is 0, assign a auto inc workerid
	status := atomic.LoadInt64(&c.Status)
	if status == 0 {
		reply.TaskType = -1
		return nil
	}
	wid := args.WorkId
	if wid == 0 {
		wid = c.NextWorkerId
		atomic.AddInt64(&c.NextWorkerId,1)
	}
	// do map or reduce
	if status == 1 {
		task = c.getMapTask(wid)
	} else if status == 2 {
		task = c.getReduceTask(wid)
	}
	// no more task
	if task == nil {
		reply.WorkId = wid
		reply.TaskType = 0
		return nil
	}
	// assignment reply
	reply.NReduce = c.NReduce
	reply.TaskType = status
	reply.Filenames = task.taskFiles
	reply.WorkId = wid
	reply.TaskId = task.taskId

	c.AddWorkingTask(wid, task, status)
	fmt.Println("send work :", reply)
	// add to working task
	return nil
}
func (c *Coordinator) WorkDone(args *DoneWorkArgs, reply *DoneWorkReply) error {
	// done this  delete the worker from working map
	status := atomic.LoadInt64(&c.Status)
	if status == 1 {
		c.mapDone(args, reply)
	} else if status == 2 {
		c.reduceDone(args, reply)
	}
	//fmt.Println(c)
	return nil
}

func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	// remove means remove the already exists sockname
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) Done() bool {
	return  atomic.LoadInt64(&c.Status) == 0
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	mapTasks := sync.Map{}
	for i, file := range files {
		mapTasks.Store(int64(i), &Task{
			taskId: int64(i),
			taskFiles: []string{file},
		})
	}
	reduceTasks := sync.Map{}
	for i := 0; i < nReduce; i++ {
		reduceTasks.Store(int64(i), &Task{
			taskId: int64(i),
			taskFiles: []string{},
		})
	}

	c := Coordinator{
		NReduce:          int64(nReduce),
		MapWorkerDead:    sync.Map{},
		ReduceWorkerDead: sync.Map{},
		Status:    1,
		NMapTask:         int64(len(files)),
		MapTasks:         mapTasks,
		NReduceTask:      int64(nReduce),
		ReduceTasks: 	  reduceTasks,
		DoneCount:        0,
		WorkingTasks:     sync.Map{},
		NextWorkerId:     1,
	}
	c.server()
	return &c
}

func (c *Coordinator) getReduceTask(wid int64) (task *Task) {
	// check the wid if dead
	if dead, ok := c.ReduceWorkerDead.Load(wid);ok && dead.(bool)  {
		return nil
	}
	//c.reduceTaskLock.Lock()
	//defer c.reduceTaskLock.Unlock()
	//if len(c.ReduceTasks) > 0 {
	//	// init task info
	//	task = c.ReduceTasks[0]
	//	// pop task
	//	c.ReduceTasks = c.ReduceTasks[1:]
	//}
	c.ReduceTasks.Range(func(key, value interface{}) bool {
		task = value.(*Task)
		return false
	})
	return task
}

func (c *Coordinator) getMapTask(wid int64) (task *Task) {
	// check the wid is dead
	if dead, ok := c.MapWorkerDead.Load(wid);ok && dead.(bool) {
		return nil
	}
	c.MapTasks.Range(func(key, value interface{}) bool {
		task = value.(*Task)
		return false
	})
	return task
}

func (c *Coordinator) mapDone(args *DoneWorkArgs, reply *DoneWorkReply) {
	// if done worker dead, ignore
	if dead, ok := c.MapWorkerDead.Load(args.WorkerId); ok && dead.(bool)  {
		return
	}

	mrXYs := args.Filepaths
	// accept file changname   add task to reduceFiles
	doneTaskId := args.TaskId
	for _, xy := range mrXYs {
		Y, _ := strconv.Atoi(xy[strings.LastIndex(xy, "-")+1:])
		newname := xy[strings.Index(xy,"-")+1:]

		task, _ := c.ReduceTasks.Load(int64(Y))
		task.(*Task).taskFiles = append(task.(*Task).taskFiles, newname)
		c.ReduceTasks.Store(task.(*Task).taskId, task.(*Task))
		os.Rename(xy, newname)
	}
	// done count ++
	fmt.Println("one map done")
	atomic.AddInt64(&c.DoneCount, 1)
	// mark workdead in WorkingTasks, and free the slot to reuse
	c.WorkingTasks.Delete(doneTaskId)
	// check if all map task done
	c.checkMapDone()
}

func (c *Coordinator) reduceDone(args *DoneWorkArgs, reply *DoneWorkReply) {
	if dead, ok := c.ReduceWorkerDead.Load(args.WorkerId);ok&&dead.(bool)   {
		return
	}
	ofilename := args.Filepaths[0]
	// doneTaskId == Y
	doneTaskId := args.TaskId
	// rename
	os.Rename(ofilename, ofilename[strings.Index(ofilename,"-")+1:])

	c.WorkingTasks.Delete(doneTaskId)
	fmt.Println("one reduce done, task Id :", doneTaskId)
	atomic.AddInt64(&c.DoneCount, 1)
	c.PrintWorkingTasks()
	// check if all reduce task done
	c.checkReduceDone()
}

func (c *Coordinator) checkMapDone() {
	fmt.Println(atomic.LoadInt64(&c.DoneCount))
	if c.NMapTask == atomic.LoadInt64(&c.DoneCount) {
		atomic.StoreInt64(&c.DoneCount, 0)
		atomic.StoreInt64(&c.Status, 2)
		fmt.Println("change status to: should be 2 , change done count to : should be 0  ", atomic.LoadInt64(&c.Status), atomic.LoadInt64(&c.DoneCount))
	}
}

func (c *Coordinator) checkReduceDone() {
	fmt.Println(atomic.LoadInt64(&c.DoneCount))
	if c.NReduceTask == atomic.LoadInt64(&c.DoneCount) {
		//fmt.Println("all reduce done")
		atomic.StoreInt64(&c.DoneCount, 0)
		atomic.StoreInt64(&c.Status, 0)
		//fmt.Println("change status to: should be 0   ", atomic.LoadInt64(&c.Status))
		fmt.Println("change status to: should be 0 , change done count to : should be  0", atomic.LoadInt64(&c.Status), atomic.LoadInt64(&c.DoneCount))
	}
}

func (c *Coordinator) AddWorkingTask(wid int64, task *Task, status int64) {

	c.WorkingTasks.Store(task.taskId, &WorkingTask{
		worker: wid,
		task:   task,
	})
	if status == 1 {
		c.MapTasks.Delete(task.taskId)
		go c.recycleMapTask(wid, task.taskId)
	}else if status == 2 {
		c.ReduceTasks.Delete(task.taskId)
		go c.recycleReduceTask(wid, task.taskId)
	}
}

func (c *Coordinator) recycleMapTask(wid, tid int64) {
	time.Sleep(10 * time.Second)
	c.MapWorkerDead.Store(wid, true)
	// delete from working map
	task, _ := c.WorkingTasks.LoadAndDelete(tid)
	if task == nil { return }
	// recycle task
	c.MapTasks.Store(task.(*WorkingTask).task.taskId, task.(*WorkingTask).task)
}

func (c *Coordinator) recycleReduceTask(wid int64, tid int64) {
	time.Sleep(10 * time.Second)
	c.ReduceWorkerDead.Store(wid, true)
	// delete from working map
	task, _ := c.WorkingTasks.LoadAndDelete(tid)
	if task == nil { return}
	// recycle task
	c.ReduceTasks.Store(task.(*WorkingTask).task.taskId, task.(*WorkingTask).task)
}

func (c *Coordinator) PrintWorkingTasks()  {
	cnt := 0
	c.WorkingTasks.Range(func(key, value interface{}) bool {
		fmt.Printf("taskId:%v, workerId:%v, \t",key.(int64), value.(*WorkingTask).worker)
		cnt++
		return true
	})
	fmt.Println("working size : ", cnt)
}



