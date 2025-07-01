package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
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

func (tasks *SafeTaskMap) resetTaskStatus(candidate int) error {
	tasks.mu.Lock()
	defer tasks.mu.Unlock()

	markTask := tasks.taskMap[candidate]
	if markTask.status != Complete {
		markTask.status = NotStarted
		tasks.taskMap[candidate] = markTask
		return nil
	}
	return fmt.Errorf("task %d has been completed", candidate)
}

func (tasks *SafeTaskMap) setTaskStatus(taskID int, status int) error {
	tasks.mu.Lock()
	defer tasks.mu.Unlock()
	markTask, err := tasks.taskMap[taskID]
	if !err {
		markTask.status = status
		tasks.taskMap[taskID] = markTask
		return nil
	}
	return fmt.Errorf("task %d not found", taskID)
}

func (tasks *SafeTaskMap) checkTaskComplete(candidate int) bool {
	tasks.mu.Lock()
	defer tasks.mu.Unlock()

	if tasks.taskMap[candidate].status == Complete { // perhaps a little sus not sure
		return true
	}
	return false // also perhaps a little sus
}

func (c *Coordinator) pollTaskStatus(taskType int, taskID int) {
	time.Sleep(time.Second * 10)
	isComplete := c.mapTasks.checkTaskComplete(taskID)
	if !isComplete {
		if taskType == Map {
			c.mapTasks.resetTaskStatus(taskID)
		} else if taskType == Reduce {
			c.reduceTasks.resetTaskStatus(taskID)
		} else {
			log.Fatalf("Unknown task type: %d", taskType)
		}
	}
}

func (c *Coordinator) SendTask(args *AskForTaskArgs, reply *AskForTaskReply) error {
	assignedTask := c.mapTasks.findUnStartedTask()
	if err := c.mapTasks.markInProgress(assignedTask); err == nil {
		reply.TaskID = assignedTask
		reply.FileName = c.mapTasks.taskMap[assignedTask].fileName
		reply.TaskType = c.mapTasks.taskMap[assignedTask].taskType
		reply.NumReduce = c.numReduceTasks
		go c.pollTaskStatus(Map, assignedTask)
	}
	return nil
}

func (c *Coordinator) NotifyTaskComplete(args *NotifyTaskCompleteArgs, reply *NotifyTaskCompleteReply) error {
	taskID := args.TaskID
	taskType := args.TaskType

	if taskType == Map {
		c.mapTasks.mu.Lock()
		if task, exists := c.mapTasks.taskMap[taskID]; exists && task.status == InProgress {
			task.status = Complete
			c.mapTasks.taskMap[taskID] = task
			c.numRemainingMapTasks--
		}
		c.mapTasks.mu.Unlock()
	} else if taskType == Reduce {
		c.reduceTasks.mu.Lock()
		if task, exists := c.reduceTasks.taskMap[taskID]; exists && task.status == InProgress {
			task.status = Complete
			c.reduceTasks.taskMap[taskID] = task
			c.numRemainingReduceTasks--
		}
		c.reduceTasks.mu.Unlock()
	} else {
		log.Fatalf("Unknown task type: %d", taskType)
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
