package mr

import (
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
	// status, 0: all work done , 1: map, 2: reduce
	Status int64 // 1:map  2:reduce
	// amounts
	NReduce     int64
	NMapTask    int64
	NReduceTask int64
	// counter
	DoneCount int64
	// auto-inc worker id
	NextWorkerId int64
	// dead worker id
	WorkerDead sync.Map
	// taskId: taskFiles
	MapTasks    sync.Map
	ReduceTasks sync.Map
	// working tasks  taskId:WorkingTask
	WorkingTasks sync.Map
}

// Task task info,
type Task struct {
	taskId    int64
	taskFiles []string
}

// WorkingTask working works
type WorkingTask struct {
	worker int64
	task   *Task
}

// MakeCoordinator init coordinator and tasks
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	mapTasks := sync.Map{}
	for i, file := range files {
		mapTasks.Store(int64(i), &Task{
			taskId:    int64(i),
			taskFiles: []string{file},
		})
	}
	reduceTasks := sync.Map{}
	for i := 0; i < nReduce; i++ {
		reduceTasks.Store(int64(i), &Task{
			taskId:    int64(i),
			taskFiles: []string{},
		})
	}

	c := Coordinator{
		Status:       1,
		NReduce:      int64(nReduce),
		NMapTask:     int64(len(files)),
		NReduceTask:  int64(nReduce),
		DoneCount:    0,
		NextWorkerId: 1,
		WorkerDead:   sync.Map{},
		MapTasks:     mapTasks,
		ReduceTasks:  reduceTasks,
		WorkingTasks: sync.Map{},
	}
	c.server()
	return &c
}

// server start
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

// Done check done
func (c *Coordinator) Done() bool {
	return atomic.LoadInt64(&c.Status) == 0
}

// WorkSend check c.Status and send corresponding task
func (c *Coordinator) WorkSend(args *AskWorkArgs, reply *AskWorkReply) error {
	var task *Task
	// get current phrase
	status := atomic.LoadInt64(&c.Status)
	// if 0, tell worker to exit
	if status == 0 {
		reply.TaskType = -1
		return nil
	}
	wid := args.WorkId
	if wid == 0 {
		wid = c.NextWorkerId
		// auto inc workerid
		atomic.AddInt64(&c.NextWorkerId, 1)
	}
	// do map or reduce
	if status == 1 {
		task = c.getMapTask(wid)
	}
	if status == 2 {
		task = c.getReduceTask(wid)
	}
	// no more task
	if task == nil {
		reply.WorkId = wid
		reply.TaskType = 0
		return nil
	}
	// assignment reply
	{
		reply.WorkId = wid
		reply.TaskId = task.taskId
		reply.NReduce = c.NReduce
		// 1 for map, 2 for reduce
		reply.TaskType = status
		reply.Filenames = task.taskFiles

	}
	// add task to workingTasks, means
	c.addWorkingTask(wid, task, status)

	return nil
}

// getReduceTask get reduce task from reduceTasks
func (c *Coordinator) getReduceTask(wid int64) (task *Task) {
	// check if the worker dead
	if c.isDead(wid) {
		return nil
	}
	// randomly assign one task
	c.ReduceTasks.Range(func(key, value interface{}) bool {
		task = value.(*Task)
		return false
	})
	return task
}

// getMapTask get map task from reduceTasks
func (c *Coordinator) getMapTask(wid int64) (task *Task) {
	// check if the worker dead
	if c.isDead(wid) {
		return nil
	}
	// randomly assign one task
	c.MapTasks.Range(func(key, value interface{}) bool {
		task = value.(*Task)
		return false
	})
	return task
}

// addWorkingTask add task to workingTasks
func (c *Coordinator) addWorkingTask(wid int64, task *Task, status int64) {
	// add to workingTasks
	c.WorkingTasks.Store(task.taskId, &WorkingTask{
		worker: wid,
		task:   task,
	})
	if status == 1 {
		c.MapTasks.Delete(task.taskId)
		// run a goroutine to check if timeout or crash
		go c.recycleMapTask(wid, task.taskId)
	}
	if status == 2 {
		c.ReduceTasks.Delete(task.taskId)
		// run a goroutine to check if timeout or crash
		go c.recycleReduceTask(wid, task.taskId)
	}
}

// recycleMapTask if worker not dont when timeout, recycle task to c.MapTasks
func (c *Coordinator) recycleMapTask(wid, tid int64) {
	// sleep 10 seconds when the task start
	time.Sleep(10 * time.Second)
	// after 10 seconds, check if the task done, if done , return
	task, ok := c.WorkingTasks.LoadAndDelete(tid)
	if !ok {
		return
	}
	// come head means not done yet, we can say the worker crashed or too slow to complete the task
	// so add the workerId to MapDead
	c.WorkerDead.Store(wid, true)
	// recycle map task
	c.MapTasks.Store(task.(*WorkingTask).task.taskId, task.(*WorkingTask).task)
}

// recycleReduceTask if worker not dont when timeout, recycle task to c.ReduceTasks
func (c *Coordinator) recycleReduceTask(wid int64, tid int64) {
	time.Sleep(10 * time.Second)
	// delete from working map
	// after 10 seconds, check if the task done, if done, return
	task, ok := c.WorkingTasks.LoadAndDelete(tid)
	if !ok {
		return
	}
	// come head means not done yet, we can say the worker crashed or too slow to complete the task
	// so add the workerId to MapDead
	c.WorkerDead.Store(wid, true)
	// recycle reduce task
	c.ReduceTasks.Store(task.(*WorkingTask).task.taskId, task.(*WorkingTask).task)
}

// WorkDone execute mapDone or reduceDone according to c.Status
func (c *Coordinator) WorkDone(args *DoneWorkArgs, reply *DoneWorkReply) error {
	// done this  delete the worker from working map
	status := atomic.LoadInt64(&c.Status)
	if status == 1 {
		c.mapDone(args, reply)
	}
	if status == 2 {
		c.reduceDone(args, reply)
	}
	return nil
}

// mapDone one map task done, and check if all map task done
func (c *Coordinator) mapDone(args *DoneWorkArgs, reply *DoneWorkReply) {
	// if done worker dead, ignore
	if c.isDead(args.WorkerId) {
		return
	}
	mrXYs := args.Filepaths
	// accept file and chang name, add task to reduceFiles
	doneTaskId := args.TaskId
	for _, xy := range mrXYs {
		Y, _ := strconv.Atoi(xy[strings.LastIndex(xy, "-")+1:])
		newname := xy[strings.Index(xy, "-")+1:]
		// like this: ReduceTasks[i] = append(ReduceTasks[i], filename)
		{
			task, _ := c.ReduceTasks.Load(int64(Y))
			task.(*Task).taskFiles = append(task.(*Task).taskFiles, newname)
			c.ReduceTasks.Store(task.(*Task).taskId, task.(*Task))
		}
		// rename to mr-X-Y
		os.Rename(xy, newname)
	}
	// done count ++
	atomic.AddInt64(&c.DoneCount, 1)
	// mark workdead in WorkingTasks, and free the slot to reuse
	c.WorkingTasks.Delete(doneTaskId)
	// check if all map task done
	c.checkAllMapDone()
}

// reduceDone one reduce task done, and check if all reduce task done
func (c *Coordinator) reduceDone(args *DoneWorkArgs, reply *DoneWorkReply) {
	// if done worker dead, ignore
	if c.isDead(args.WorkerId) {
		return
	}
	ofilename := args.Filepaths[0]
	// doneTaskId == Y
	doneTaskId := args.TaskId
	// rename to mr-out-Y
	os.Rename(ofilename, ofilename[strings.Index(ofilename, "-")+1:])
	// remove from workingTasks
	c.WorkingTasks.Delete(doneTaskId)
	// add done count
	atomic.AddInt64(&c.DoneCount, 1)
	// check if all reduce task done
	c.checkAllReduceDone()
}

// checkAllMapDone check and set status
func (c *Coordinator) checkAllMapDone() {
	if c.NMapTask == atomic.LoadInt64(&c.DoneCount) {
		// reset done count
		atomic.StoreInt64(&c.DoneCount, 0)
		// alter c.Status to reduce phrase
		atomic.StoreInt64(&c.Status, 2)
	}
}

// checkAllReduceDone check and set status
func (c *Coordinator) checkAllReduceDone() {
	if c.NReduceTask == atomic.LoadInt64(&c.DoneCount) {
		// alter c.Status to reduce phrase
		atomic.StoreInt64(&c.Status, 0)
	}
}

// isDead return if worker is dead
func (c *Coordinator) isDead(wid int64) bool {
	dead, ok := c.WorkerDead.Load(wid)
	return ok && dead.(bool)
}
