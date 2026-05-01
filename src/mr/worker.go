package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
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

// for sorting by key.
type ByKey []KeyValue

// for sorting by key - interface required to be defined for this 
// method (as its usually used for sorting custom data structures)
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
		nMap := reply.nMap
		
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
				
				// closing all the files in the bucket
				for _, file := range fileBucket {
					file.Close()
				}
				
				// updating mapping task state to complete
				completeTaskInfo := UpdateTaskStateArgs{
					taskID: taskID,
					taskType: MapT,
					updatedState: Completed,
				}

				CallUpdateTaskState(&completeTaskInfo, &UpdateTaskStateReply{})
			
			case ReduceT:
				// gets task bucket and updates states
				// updates task state to in-progress
				inProgressTaskInfo := UpdateTaskStateArgs{
					taskID: taskID,
					taskType: ReduceT,
					updatedState: InProgress,
				}
				CallUpdateTaskState(&inProgressTaskInfo, &UpdateTaskStateReply{})

				// from the task bucket we know which
				// files we will process however
				// reads the buffered data from local disk
				intermediate := []KeyValue{}
				for i := range nMap {
					fileName := fmt.Sprintf("mr-%v-%v", i, taskID)

					// open the intermediate file
					file, err := os.Open(fileName)
					if err != nil {
						log.Fatalf("cannot open %v", reply.filename)	
					}

					dec := json.NewDecoder(file)
					for {
						var kv KeyValue
						if err := dec.Decode(&kv); err != nil {
							break
						}
						intermediate = append(intermediate, kv)
					}

					// close the intermediate file
					file.Close()

				}

				// sorts it by the intermediate keys (all occcurrences
				// of the same key are next to each other together)
				sort.Sort(ByKey(intermediate))
				
				// output of reduce function appended to a final
				// output file for this reduce partition
				oname := fmt.Sprintf("mr-out-%v", taskID)
				ofile, _ := os.Create(oname)

				// reduce worker iterates over the sorted intermediate
				// data and passes each unique key and set of inter
				// mediate values to the user's reduce function 
				i := 0
				for i < len(intermediate) {
					j := i + 1
					// since the intermediate array is now sorted, what this is doing
					// is that it is getting the slice of the kv pairs in the array
					// that have the same key since in the reduce function we need to
					// pass a key with all of its values
					for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
						j++
					}

					// i = start of range in which key is same for all kv pairs
					// j - 1 = end of range in which key is same for all kv pairs
					values := []string{}
					for k := i; k < j; k++ {
						values = append(values, intermediate[k].Value)
					}

					// calling reduce function and should get back a string representing
					// a kv pair
					output := reducef(intermediate[i].Key, values)

					// writing output to output file - output is a string with a value
					// ex: in wc program, the output represents the total occurrences of the
					// key that we passed in
					fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

					// j represents the start of the next slice of kv pairs that share
					// the same key
					i = j
				}

				// close output file
				ofile.Close()

				// indicate that the mapping task is complete
				completeTaskInfo := UpdateTaskStateArgs{
					taskID: taskID,
					taskType: ReduceT,
					updatedState: Completed,
				}
				
				CallUpdateTaskState(&completeTaskInfo, &UpdateTaskStateReply{})				
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
