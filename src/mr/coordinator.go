package mr

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// nReduce
	NReduce int
	// it the worker alive    timeout   then add the workerid to this map     if done workerid in this, ignore !
	MapWorkerDead map[int]bool
	// map dead lock
	mdl              sync.Mutex
	ReduceWorkerDead map[int]bool
	// reduce dead lock
	rdl sync.Mutex
	// current status
	CurrentStatus int // 1:map  2:reduce
	statusLock    sync.Mutex
	// map file and exists
	NMapTask    int
	MapTasks    []*Task
	mapTaskLock sync.Mutex

	NReduceTask int

	ReduceTasks    []*Task
	reduceTaskLock sync.Mutex

	DoneCount int
	countLock sync.Mutex

	workingLock  sync.Mutex
	WorkingTasks []*WorkingTask
	freeLock     sync.Mutex
	WorkingFree  []int

	ticker *time.Ticker
	// auto-inc worker id
	NextWorkerId int
	idLock       sync.Mutex

	resultFiles []string
	resLock sync.Mutex
}

type Task struct {
	taskId	int
	taskFiles []string
}
type WorkingTask struct {
	worker    int
	free bool
	task *Task
	cost int
}

func (c *Coordinator) WorkSend(args *AskWorkArgs, reply *AskWorkReply) error {
	var task *Task
	// get worker id, if workerid is 0, assign a auto inc workerid
	c.statusLock.Lock()
	currentStatus := c.CurrentStatus
	c.statusLock.Unlock()

	if currentStatus == 0 {
		reply.TaskType = -1
		return nil
	}

	wid := args.WorkId
	if wid == 0 {
		c.idLock.Lock()
		wid = c.NextWorkerId
		c.NextWorkerId++
		c.idLock.Unlock()
	}
	// do map or reduce
	if currentStatus == 1 {
		task = c.getMapTask(wid)
	} else if currentStatus == 2 {
		task = c.getReduceTask(wid)
	}
	// assignment reply

	// no more task
	if task == nil {
		reply.WorkId = wid
		reply.TaskType = 0
		return nil
	}
	reply.NReduce = c.NReduce
	reply.TaskType = currentStatus
	reply.Filenames = task.taskFiles
	reply.WorkId = wid
	reply.TaskId = task.taskId

	c.AddWorkingTask(task)

	// add to working task
	fmt.Println("sendwork : ", reply)
	return nil
}

func (c *Coordinator) timeTicker() {
	// map phrase
	for {
		<-c.ticker.C
		c.statusLock.Lock()
		status := c.CurrentStatus
		c.statusLock.Unlock()
		if status == 0 {
			c.ticker.Stop()
			return
		}
		if status == 1 {
			c.recycleMapTask()
		}
		if status == 2 {
			c.recycleReduceTask()
		}
	}
}

func (c *Coordinator) WorkDone(args *DoneWorkArgs, reply *DoneWorkReply) error {
	// done this  delete the worker from working map

	if c.CurrentStatus == 1 {
		c.mapDone(args, reply)
	} else if c.CurrentStatus == 2 {
		c.reduceDone(args, reply)
	}
	fmt.Println("one work done : ", args)
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
	// 0 means all reduce done
	// todo: add an rpc call,
	c.statusLock.Lock()
	status := c.CurrentStatus
	c.statusLock.Unlock()
	return status == 0
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	mapFiles := make([]*Task, len(files))
	for i, file := range files {
		mapFiles[i] = &Task{
			taskId:    i,
			taskFiles: []string{file},
		}
	}
	reduceTasks := make([]*Task, nReduce)
	for i := 0; i < nReduce; i++ {
		reduceTasks[i] = &Task{
			taskId: i,
			taskFiles: make([]string, 0),
		}
	}

	c := Coordinator{
		NReduce:          nReduce,
		MapWorkerDead:    make(map[int]bool),
		mdl:              sync.Mutex{},
		ReduceWorkerDead: make(map[int]bool),
		rdl:              sync.Mutex{},
		CurrentStatus:    1,
		statusLock:       sync.Mutex{},
		NMapTask:         len(files),
		MapTasks:         mapFiles,
		mapTaskLock:      sync.Mutex{},
		NReduceTask:      nReduce,
		ReduceTasks:      reduceTasks,
		reduceTaskLock:   sync.Mutex{},
		DoneCount:        0,
		countLock:        sync.Mutex{},
		workingLock:      sync.Mutex{},
		WorkingTasks:     make([]*WorkingTask, 0),
		ticker:           time.NewTicker(time.Second),
		NextWorkerId:     1,
		idLock:           sync.Mutex{},
		WorkingFree:      make([]int, 0),
		freeLock: sync.Mutex{},
		resultFiles: make([]string, nReduce),
		resLock: sync.Mutex{},
	}

	c.server()
	go c.timeTicker()
	return &c
}

func (c *Coordinator) getReduceTask(wid int) (task *Task) {
	// check the wid if dead
	c.rdl.Lock()
	isDead := c.ReduceWorkerDead[wid]
	if isDead {
		return nil
	}
	c.rdl.Unlock()

	c.reduceTaskLock.Lock()
	defer c.reduceTaskLock.Unlock()
	if len(c.ReduceTasks) > 0 {
		// init task info
		task = c.ReduceTasks[0]
		// pop task
		c.ReduceTasks = c.ReduceTasks[1:]
	}
	return task
}

func (c *Coordinator) getMapTask(wid int) (task *Task) {
	// check the wid is dead
	c.mdl.Lock()
	isDead := c.MapWorkerDead[wid]
	if isDead {
		return nil
	}
	c.mdl.Unlock()

	c.mapTaskLock.Lock()
	// have available task
	if len(c.MapTasks) > 0 {
		// init task info
		task = c.MapTasks[0]
		// pop task
		c.MapTasks = c.MapTasks[1:]
	}
	c.mapTaskLock.Unlock()
	return task
}

func (c *Coordinator) mapDone(args *DoneWorkArgs, reply *DoneWorkReply) {
	// if done worker dead, ignore
	c.mdl.Lock()
	isDead :=  c.MapWorkerDead[args.WorkerId]
	c.mdl.Unlock()
	if isDead { return }

	mrXYs := args.Filepaths
	// accept file   add task to reduceFiles
	doneTaskId := args.TaskId
	for _, xy := range mrXYs {
		Y, _ := strconv.Atoi(xy[strings.LastIndex(xy, "-")+1:])
		c.reduceTaskLock.Lock()
		c.ReduceTasks[Y].taskFiles = append(c.ReduceTasks[Y].taskFiles, xy)
		c.reduceTaskLock.Unlock()
	}
	// done count ++
	c.countLock.Lock()
	c.DoneCount++
	c.countLock.Unlock()
	// mark workdead in WorkingTasks, and free the slot to reuse
	c.markTaskDone(doneTaskId)
	// check if all map task done
	c.checkMapDone()
}

func (c *Coordinator) reduceDone(args *DoneWorkArgs, reply *DoneWorkReply) {
	c.rdl.Lock()
	isDead :=  c.ReduceWorkerDead[args.WorkerId]
	c.rdl.Unlock()
	if isDead { return }

	ofilename := args.Filepaths[0]
	// doneTaskId == Y
	doneTaskId := args.TaskId
	c.resLock.Lock()
	c.resultFiles[doneTaskId] = ofilename
	c.resLock.Unlock()

	//oname := "mr-out-" + strconv.Itoa(doneTaskId)
	//os.Rename(ofilename, oname)
	// add done count
	c.countLock.Lock()
	c.DoneCount++
	c.countLock.Unlock()

	c.markTaskDone(doneTaskId)
	// check if all reduce task done
	c.checkReduceDone()
}

func (c *Coordinator) checkMapDone() {
	c.countLock.Lock()
	defer c.countLock.Unlock()
	if c.NMapTask == c.DoneCount {
		fmt.Println("all map done")
		c.DoneCount = 0
		c.statusLock.Lock()
		c.CurrentStatus = 2
		c.statusLock.Unlock()

		c.freeLock.Lock()
		c.WorkingFree = make([]int, 0)
		c.freeLock.Unlock()

		c.workingLock.Lock()
		c.WorkingTasks = make([]*WorkingTask, 0)
		c.workingLock.Unlock()



	}
}

func (c *Coordinator) checkReduceDone() {
	c.countLock.Lock()
	defer c.countLock.Unlock()
	if c.NReduceTask == c.DoneCount {
		fmt.Println("all reduce done")

		//c.doReduceOutRename()

		c.DoneCount = 0
		c.statusLock.Lock()
		c.CurrentStatus = 0
		c.statusLock.Unlock()

		c.freeLock.Lock()
		c.WorkingFree = make([]int, 0)
		c.freeLock.Unlock()

		c.workingLock.Lock()
		c.WorkingTasks = make([]*WorkingTask, 0)
		c.workingLock.Unlock()



	}
}

func (c *Coordinator) recycleMapTask() {
	c.workingLock.Lock()
	defer c.workingLock.Unlock()
	for i := 0; i < len(c.WorkingTasks); i++ {
		workingTask := c.WorkingTasks[i]
		if !workingTask.free  {
			workingTask.cost++
			if workingTask.cost > 10 {
				fmt.Printf("map worker: %d crashed ! cost : %v , task %v recycle\n", workingTask.worker, workingTask.cost, workingTask.task.taskFiles)
				// mark the working task is dead
				workingTask.free = true
				// add into dead map
				c.mdl.Lock()
				c.MapWorkerDead[workingTask.worker] = true
				c.mdl.Unlock()
				// add into available maptask
				c.mapTaskLock.Lock()
				c.MapTasks = append(c.MapTasks, workingTask.task)
				c.mapTaskLock.Unlock()
				// mark the free space to reuse
				c.freeLock.Lock()
				c.WorkingFree = append(c.WorkingFree, i)
				c.freeLock.Unlock()
			}
		}
	}
}

func (c *Coordinator) recycleReduceTask() {
	c.workingLock.Lock()
	defer c.workingLock.Unlock()

	for i := 0; i < len(c.WorkingTasks); i++ {
		workingTask := c.WorkingTasks[i]
		if !workingTask.free  {
			workingTask.cost++
			if workingTask.cost > 10 {
				fmt.Printf("reduce worker: %d crashed ! cost : %v , task %v recycle\n", workingTask.worker, workingTask.cost, workingTask.task.taskFiles)
				// mark the working task is dead
				workingTask.free = true
				// add into dead map
				c.rdl.Lock()
				c.ReduceWorkerDead[workingTask.worker] = true
				c.rdl.Unlock()
				// add into available reducetask
				c.reduceTaskLock.Lock()
				c.ReduceTasks = append(c.ReduceTasks, workingTask.task)
				c.reduceTaskLock.Unlock()
				// mark the free space to reuse
				c.freeLock.Lock()
				c.WorkingFree = append(c.WorkingFree, i)
				c.freeLock.Unlock()
			}
		}
	}
}

func (c *Coordinator) AddWorkingTask(task *Task) {
	c.workingLock.Lock()
	c.freeLock.Lock()
	// if workingfree: replace else put back
	if len(c.WorkingFree) > 0 {
		pos := c.WorkingFree[0]
		c.WorkingFree = c.WorkingFree[1:]
		c.WorkingTasks[pos].task = task
		c.WorkingTasks[pos].free = false
		c.WorkingTasks[pos].cost = 0
	}else {
		c.WorkingTasks = append(c.WorkingTasks, &WorkingTask{
			free:   false,
			task:   task,
			cost:   0,
		})
	}
	c.freeLock.Unlock()
	c.workingLock.Unlock()
}

func (c *Coordinator) markTaskDone(doneTaskId int) {
	c.workingLock.Lock()
	for i, wt := range c.WorkingTasks {
		if wt.task.taskId == doneTaskId {
			wt.free = true
			c.freeLock.Lock()
			c.WorkingFree = append(c.WorkingFree, i)
			c.freeLock.Unlock()
			break
		}
	}
	c.workingLock.Unlock()
}

func (c *Coordinator) doMapOutRename() {
	c.reduceTaskLock.Lock()
	for i := 0; i < c.NReduce;i++ {
		for j := 0; j < c.NMapTask;j++ {
			oldname := c.ReduceTasks[i].taskFiles[j]
			newname := oldname[strings.Index(oldname,"-")+1:]
			c.ReduceTasks[i].taskFiles[j] = newname
			os.Rename(oldname, newname)
		}
	}
	c.reduceTaskLock.Unlock()
}

func (c *Coordinator) doReduceOutRename() {
	c.resLock.Lock()
	for i := 0; i < c.NReduce; i++ {
		oldname := c.resultFiles[i]
		newname :=  oldname[strings.Index(oldname,"-")+1:]
		os.Rename(oldname, newname)
	}
	c.resLock.Unlock()
}






