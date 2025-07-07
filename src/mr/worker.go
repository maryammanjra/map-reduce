package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
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
	for {
		taskID, fileName, taskType, numReduce, err := AskForTask()
		log.Println("Working on map task with ID:", taskID)

		if err != nil {
			// TODO: handle error better
			log.Fatalf("Failed to get task from coordinator: %v", err)
		}

		if taskType == Map {
			err = performMap(fileName, mapf, numReduce, taskID)
			if err != nil {
				log.Fatalf("Error during map") // TODO: print error
			}
			err = NotifyTaskComplete(taskID, taskType)
			if err != nil {
				log.Fatalf("Error notifying task completion: %v", err)
			}
		} else if taskType == Reduce {
			log.Println("Received Reduce task")
			err = performReduce(fileName, reducef, taskID)
			if err != nil {
				log.Fatalf("Error during reduce: %v", err)
			}
			err = NotifyTaskComplete(taskID, taskType)
			if err != nil {
				log.Fatalf("Error notifying task completion: %v", err)
			}
		} else if taskType == Quit {
			log.Println("Received Quit task, exiting")
			return
		} else {
			log.Fatalf("Unknown task type: %d", taskType)
		}
	}
	// CallExample()

}

func NotifyTaskComplete(taskID int, taskType int) error {
	args := NotifyTaskCompleteArgs{}
	args.TaskID = taskID
	args.TaskType = taskType
	reply := NotifyTaskCompleteReply{}
	ok := call("Coordinator.ReceiveTaskComplete", &args, &reply)
	if ok {
		log.Printf("Task %d of type %d completed successfully", taskID, taskType)
		return nil
	} else {
		return fmt.Errorf("failed to notify coordinator of task completion: %d", taskID)
	}
}

func AskForTask() (int, string, int, int, error) {
	args := AskForTaskArgs{}
	args.X = 0 // Example argument, can be used to pass additional info if needed
	reply := AskForTaskReply{}
	ok := call("Coordinator.SendTask", &args, &reply)
	if ok {
		taskID := reply.TaskID
		filename := reply.FileName
		taskType := reply.TaskType
		numReduce := reply.NumReduce
		return taskID, filename, taskType, numReduce, nil
	} else {
		return -1, "", -1, -1, fmt.Errorf("failed to get task from coordinator")
	}
}

func performMap(fileName string, mapf func(string, string) []KeyValue, numReduce int, taskID int) error {
	mapInput, err := readInputFile(fileName)
	intermediateFiles := make(map[int]string)
	if err == nil {
		kva := mapf(fileName, mapInput)

		for _, intermediate := range kva {
			partition := ihash(intermediate.Key) % numReduce

			if file, ok := intermediateFiles[partition]; ok {
				if err := writeKVToFile(file, intermediate); err != nil {
					log.Printf("Error writing to existing file: %v", err) // TODO: this chunk probably needs a good refactor
				}
			} else {
				file, err = createTempFile(partition)
				if err != nil {
					log.Fatalf("Failed to initialize temp file: %v", err)
				}
				intermediateFiles[partition] = file
				writeKVToFile(file, intermediate)
				log.Println("File DNE, creating and writing")
				print(file)
			}
		}
	}
	if err := setAtomicNames(intermediateFiles, taskID); err != nil {
		log.Fatalf("Error in renaming files: %v", err)
	}
	log.Println("Received Map task")
	return nil
}

func performReduce(fileName string, reducef func(string, []string) string, taskID int) error {

	// Read all intermediate files that match the pattern
	entries, err := os.ReadDir("../mr")
	re := regexp.MustCompile(fileName)
	if err != nil {
		log.Fatal(err)
	}
	kva := make([]KeyValue, 0)
	for _, entry := range entries {
		fmt.Println(entry.Name())
		if re.MatchString(entry.Name()) {
			fileContent, err := readInputFile("../mr/" + entry.Name())
			fmt.Println("Reading file: ", entry.Name())
			if err != nil {
				log.Fatalf("Failed to read file %s: %v", entry.Name(), err)
			}
			dec := json.NewDecoder(strings.NewReader(fileContent))
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					log.Println("Error decoding JSON")
					break
				}
				// fmt.Println("Decoded KeyValue: ", kv)
				kva = append(kva, kv)
			}
		}
	}

	fmt.Println("Grouping key/value pairs for reduce task")
	// Group the key/value pairs by key.
	reduceMap := make(map[string][]string)
	uniqueKeys := make([]string, 0)
	for _, kv := range kva {
		if _, exists := reduceMap[kv.Key]; !exists {
			uniqueKeys = append(uniqueKeys, kv.Key)
		}
		reduceMap[kv.Key] = append(reduceMap[kv.Key], kv.Value)
	}

	fmt.Println("Sorting keys for reduce task")
	// Sort the key/value pairs by key.
	slices.Sort(uniqueKeys)

	fmt.Println("Writing reduce output to file")
	// Write reduce output to file
	outputFileName := fmt.Sprintf("mr-out-%d.txt", taskID)
	outputFile, err := os.OpenFile(outputFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer outputFile.Close()

	if err != nil {
		log.Fatal(err)
	}
	for _, key := range uniqueKeys {
		output := reducef(key, reduceMap[key])
		_, err = outputFile.WriteString(fmt.Sprintf("%s %s\n", key, output))
		if err != nil {
			log.Fatalf("Failed to write to output file: %v", err)
		}
	}
	log.Println("Worker completed reduce task")
	return nil
}

func setAtomicNames(intermediateFiles map[int]string, taskID int) error {
	// There's probs a better way to do this
	for partition, file := range intermediateFiles {
		dir := filepath.Dir(file)
		finalName := "mr-" + strconv.Itoa(taskID) + "-" + strconv.Itoa(partition) + ".txt"
		newPath := filepath.Join(dir, finalName)
		err := os.Rename(file, newPath)

		if err != nil {
			return fmt.Errorf("failed to atomically rename file: %v", err)
		}
	}
	return nil
}

func readInputFile(filename string) (string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return "", fmt.Errorf("cannot open file %v: %v", filename, err)
	}
	defer file.Close()

	content, err := io.ReadAll(file)
	if err != nil {
		return "", fmt.Errorf("cannot read file %v: %v", filename, err)
	}
	return string(content), nil
}

func createTempFile(fileID int) (string, error) {
	fileNum := strconv.Itoa(fileID)
	fileName := "temp-file" + fileNum + "-*.txt"
	file, err := os.CreateTemp("../mr", fileName)

	if err == nil {
		defer file.Close()
		log.Println(file.Name())
		return file.Name(), nil // add
	}
	return "", fmt.Errorf("failed to create file for ID %v: %v", fileID, err)
}

func writeKVToFile(fileName string, kv KeyValue) error {
	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_WRONLY, 0644)

	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}

	defer file.Close()
	enc := json.NewEncoder(file)
	if err := enc.Encode(&kv); err != nil {
		return fmt.Errorf("failed to encode KV: %w", err)
	}

	return nil
}

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
func call(rpcname string, args any, reply any) bool {
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
