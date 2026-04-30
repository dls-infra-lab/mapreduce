package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
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

var coordSockName string // socket for coordinator

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname
	// looping forever waiting for tasks
	for {
		// Your worker implementation here.
		args := ReqArgs{}
		reply := ReqReply{}

		// send to rpc to the coordinator asking for a task
		CallMakeCoordinator(&args, &reply)
		
		// task information needed
		// - To know what task worker is dealing with
		// - To update the state of the correct task
		taskType := reply.taskType
		taskID := reply.taskID
		nReduce := reply.nReduce
		
		switch taskType{
			case MapT:
				// updates task state to in-progress
				inProgressTaskInfo := UpdateTaskStateArgs{
					taskID: taskID,
					taskType: MapT,
					updatedState: InProgress,
				}
				CallUpdateTaskState(&inProgressTaskInfo, &UpdateTaskStateReply{})

				// open file, read it and call map function
				filename := reply.filename
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", reply.filename)	
				}	

				content, err := io.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", filename)
				}

				file.Close()
				kva := mapf(filename, string(content))

				// using spread operator to add each of
				// key value pairs to intermediate arr
				// (that holds intermediate data)
				// TODO: instead of having this list
				// - we need to have it so that we drop
				// each element of kva in a specific bucket
				// - a bucket is represented by the files we'll
				// make
				// iterate through each kv pair
				// otherwise

				// storing file and encoder references
				// so we can access and then in the files
				// case close them when done at the end
				fileBucket := []*os.File{}
				encoderBucket := []*json.Encoder{}
				
				// create nReduce empty files
				for bucketNum := range nReduce {
					intermediateFilename := fmt.Sprintf("mr-%v-%v", reply.taskID, bucketNum)
					f, _ := os.Create(intermediateFilename)
					enc := json.NewEncoder(f)
					fileBucket = append(fileBucket, f)
					encoderBucket = append(encoderBucket, enc)
				}

				// iterating kv pairs list 
				for _, kv := range kva {
					bucket := ihash(kv.Key) % nReduce
					encoder := encoderBucket[bucket]
					err := encoder.Encode(&kv)

					if err != nil {
						log.Fatalf("cannot encode kv pair into file")
					}
				} 

				// indicate that the mapping task is complete
				completeTaskInfo := UpdateTaskStateArgs{
					taskID: taskID,
					taskType: MapT,
					updatedState: Completed,
				}
				
				CallUpdateTaskState(&completeTaskInfo, &UpdateTaskStateReply{})
				
				// closing all the files in the bucket
				for _, file := range fileBucket {
					file.Close()
				}
				
			
			case ReduceT:

				return	
				
		}
	}
}

// RPC call to update the state of a task
func CallUpdateTaskState (args *UpdateTaskStateArgs, reply *UpdateTaskStateReply) {
	// send RPC request, wait for reply write
	ok := call("Coordinator.RequestTask", args, reply)

	if ok && reply.updated {
		fmt.Printf("call succeeded!\n")
	} else {
		fmt.Printf("call failed!\n")
	}
}

// RPC call to initialize the coordinator
func CallMakeCoordinator(args *ReqArgs, reply *ReqReply) {

	// send RPC request, wait for reply write
	ok := call("Coordinator.RequestTask", args, reply)

	if ok {
		fmt.Printf("call succeeded!\n")
	} else {
		fmt.Printf("call failed!\n")
	}
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
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
