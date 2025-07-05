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
	Quit   = iota
)

type Coordinator struct {
	mapTasks    *SafeTaskMap
	reduceTasks *SafeTaskMap
}

type SafeTaskMap struct {
	taskMap           map[int]*TaskData
	numTasks          int
	numRemainingTasks int
	mu                sync.Mutex
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

func (tasks *SafeTaskMap) markTaskComplete(taskID int) error {
	tasks.mu.Lock()
	defer tasks.mu.Unlock()

	markTask, err := tasks.taskMap[taskID]
	if !err {
		markTask.status = Complete
		tasks.taskMap[taskID] = markTask
		tasks.numRemainingTasks--
		return nil
	}
	return fmt.Errorf("task %d not found", taskID)
}

// func (tasks *SafeTaskMap) setTaskStatus(taskID int, status int) error {
// 	tasks.mu.Lock()
// 	defer tasks.mu.Unlock()
// 	markTask, err := tasks.taskMap[taskID]
// 	if !err {
// 		markTask.status = status
// 		tasks.taskMap[taskID] = markTask
// 		return nil
// 	}
// 	return fmt.Errorf("task %d not found", taskID)
// }

func (tasks *SafeTaskMap) checkTaskComplete(candidate int) bool {
	tasks.mu.Lock()
	defer tasks.mu.Unlock()

	if tasks.taskMap[candidate].status == Complete { // perhaps a little sus not sure
		return true
	}
	return false // also perhaps a little sus
}

func (tasks *SafeTaskMap) Init(taskType int, numTasks int, filenames []string) {
	tasks.mu.Lock()
	defer tasks.mu.Unlock()

	tasks.taskMap = make(map[int]*TaskData)
	tasks.numTasks = numTasks
	tasks.numRemainingTasks = numTasks

	for i := 0; i < numTasks; i++ {
		taskData := TaskData{
			fileName: filenames[i],
			status:   NotStarted,
			taskType: taskType,
		}
		tasks.taskMap[i] = &taskData
	}
}

func (tasks *SafeTaskMap) allTasksComplete() bool {
	tasks.mu.Lock()
	defer tasks.mu.Unlock()

	return tasks.numRemainingTasks == 0
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

	// First check if there are any map tasks available
	for !c.mapTasks.allTasksComplete() {
		assignedTask := c.mapTasks.findUnStartedTask()
		if assignedTask == -1 {
			time.Sleep(time.Second * 1) // No map tasks available, wait and retry
			continue
		}
		if err := c.mapTasks.markInProgress(assignedTask); err == nil {
			reply.TaskID = assignedTask
			reply.FileName = c.mapTasks.taskMap[assignedTask].fileName
			reply.TaskType = c.mapTasks.taskMap[assignedTask].taskType
			reply.NumReduce = c.reduceTasks.numTasks
			go c.pollTaskStatus(Map, assignedTask)
			return nil
		}
	}

	// If we reach here, it means all map tasks are complete
	// Now we can assign reduce tasks
	for !c.reduceTasks.allTasksComplete() {
		assignedTask := c.reduceTasks.findUnStartedTask()
		if assignedTask == -1 {
			time.Sleep(time.Second * 1)
			continue
		}
		if err := c.reduceTasks.markInProgress(assignedTask); err == nil {
			reply.TaskID = assignedTask
			reply.FileName = c.reduceTasks.taskMap[assignedTask].fileName
			reply.TaskType = c.reduceTasks.taskMap[assignedTask].taskType
			reply.NumReduce = c.reduceTasks.numTasks
			go c.pollTaskStatus(Reduce, assignedTask)
			return nil
		}
	}

	// If we reach here, it means all tasks are complete
	reply.TaskID = -1
	reply.FileName = ""
	reply.TaskType = Quit
	reply.NumReduce = 0
	return nil
}

func (c *Coordinator) NotifyTaskComplete(args *NotifyTaskCompleteArgs, reply *NotifyTaskCompleteReply) error {
	taskID := args.TaskID
	taskType := args.TaskType

	if taskType == Map {
		c.mapTasks.markTaskComplete(taskID)
	} else if taskType == Reduce {
		c.mapTasks.markTaskComplete(taskID)
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
	if c.mapTasks.numRemainingTasks == 0 && c.reduceTasks.numRemainingTasks == 0 {
		return true
	}
	return false
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	mapTasks := new(SafeTaskMap)
	mapTasks.Init(Map, len(files), files)
	c.mapTasks = mapTasks

	reduceTasks := new(SafeTaskMap)
	reduceFiles := make([]string, nReduce)
	for i := range reduceFiles {
		reduceFiles[i] = fmt.Sprintf("mr-out-%d", i) // TODO: Temporary file names, change later
	}
	reduceTasks.Init(Reduce, nReduce, reduceFiles)
	c.reduceTasks = reduceTasks

	c.server()
	return &c
}
