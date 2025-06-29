package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

const (
	NotStarted = iota
	Staged     = iota
	InProgress = iota
	Complete   = iota
)

const (
	Map    = iota
	Reduce = iota
)

type Coordinator struct {
	numMapTasks             int
	numReduceTasks          int
	numRemainingMapTasks    int
	numRemainingReduceTasks int
	mapTasks                SafeTaskMap
	reduceTasks             SafeTaskMap
}

type SafeTaskMap struct {
	taskMap map[int]TaskData
	mu      sync.Mutex
}

type TaskData struct {
	fileName string
	status   int
	taskType int // Map or Reduce
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (tasks *SafeTaskMap) findUnStartedTask() int {
	tasks.mu.Lock()
	defer tasks.mu.Unlock()
	for key, value := range tasks.taskMap {
		if value.status == NotStarted {
			value.status = Staged
			tasks.taskMap[key] = value
			return key
		}
	}
	return -1
}

func (tasks *SafeTaskMap) markInProgress(candidate int) error {
	tasks.mu.Lock()
	defer tasks.mu.Unlock()

	markTask := tasks.taskMap[candidate]
	if markTask.status == Staged {
		markTask.status = InProgress
		tasks.taskMap[candidate] = markTask
		return nil
	}
	return fmt.Errorf("task %d already in progress", candidate)
}

func (c *Coordinator) SendTask(args *AskForTaskArgs, reply *AskForTaskReply) error {
	assignedTask := c.mapTasks.findUnStartedTask()
	if c.mapTasks.markInProgress(assignedTask) == nil {
		reply.FileName = c.mapTasks.taskMap[assignedTask].fileName
		reply.TaskType = c.mapTasks.taskMap[assignedTask].taskType
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	if c.numRemainingMapTasks == 0 && c.numRemainingReduceTasks == 0 {
		return true
	}
	return false
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.numMapTasks = len(files)
	c.numReduceTasks = nReduce
	c.numRemainingMapTasks = c.numMapTasks
	c.numRemainingReduceTasks = c.numReduceTasks

	c.mapTasks.mu.Lock()
	c.mapTasks.taskMap = make(map[int]TaskData)
	c.mapTasks.mu.Unlock()

	c.reduceTasks.mu.Lock()
	c.reduceTasks.taskMap = make(map[int]TaskData)
	c.reduceTasks.mu.Unlock()

	// Initialize mapTasks and reduceTasks with NotStarted status.
	for i := range c.numMapTasks {
		log.Println("Initializing map task for file:", files[i])
		taskData := new(TaskData)
		taskData.fileName = files[i]
		taskData.status = NotStarted
		taskData.taskType = Map

		c.mapTasks.mu.Lock()
		c.mapTasks.taskMap[i] = *taskData // Not sure if this is supposed to be a pointer, check when debug
		c.mapTasks.mu.Unlock()
	}

	for i := range c.numReduceTasks {
		taskData := new(TaskData)
		taskData.fileName = "" // change this later to M*N files
		taskData.status = NotStarted
		taskData.taskType = Reduce

		c.reduceTasks.mu.Lock()
		c.reduceTasks.taskMap[i] = *taskData // can delegate init to another function later if we want
		c.reduceTasks.mu.Unlock()
	}

	c.server()
	return &c
}
