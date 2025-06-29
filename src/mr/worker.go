package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// Your worker implementation here.
	filename, taskType, err := AskForTask()
	if err != nil {
		// TODO: handle error better
		log.Fatalf("Failed to get task from coordinator: %v", err)
	}

	log.Println("Thing: ", filename, taskType)

	// if assignedTask.taskType == Map {
	// 	log.Println("Received Map task")
	// } else if assignedTask.taskType == Reduce {
	// 	log.Println("Received Reduce task")
	// } else {
	// 	log.Fatalf("Unknown task type: %v", assignedTask.taskType)
	// }
	// CallExample()

}

func AskForTask() (string, int, error) {
	args := AskForTaskArgs{}
	args.X = 0 // Example argument, can be used to pass additional info if needed
	reply := AskForTaskReply{}
	ok := call("Coordinator.SendTask", &args, &reply)
	if ok {
		filename := reply.FileName
		taskType := reply.TaskType
		return filename, taskType, nil
	} else {
		return "", -1, fmt.Errorf("failed to get task from coordinator")
	}
}

// func performMap(fileName string, mapf func(string, string) []KeyValue) error {
// 	return nil
// }

// func readMapInputFile(filename string) (string, error) {
// 	file, err := os.Open(filename)
// 	if err != nil {
// 		log.Fatalf("cannot open %v", filename)
// 	}
// 	content, err := ioutil.ReadAll(file)
// 	if err != nil {
// 		log.Fatalf("cannot read %v", filename)
// 	}
// 	file.Close()
// 	kva := mapf(filename, string(content))
// 	intermediate = append(intermediate, kva...)
// 	return "", nil
// }

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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

	fmt.Println("Call error: ", err)
	return false
}
