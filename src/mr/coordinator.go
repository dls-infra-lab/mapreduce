package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"


type TaskState int
type CoordinatorState int

const (
	// task states
	Idle TaskState = 0
	InProgress TaskState = 1
	Completed TaskState = 2

	// coordinator states
	Map CoordinatorState = 0
	Reduce CoordinatorState = 1
)

type MapTask struct {
	id int
	filename string
	state TaskState
}

type ReduceTask struct {
	id int
	state TaskState
}

type Coordinator struct {
	// Your definitions here.
	M []MapTask
	R []ReduceTask
	// needed so we know what phase
	// we're in and the coordinator
	// assigns certain tasks based on
	// phase we're on
	Phase CoordinatorState 
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.


	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	// Your code here.
	c := Coordinator{
		M: []MapTask{},
		R: []ReduceTask{},
	}

	mapTasks := []MapTask{}
	reduceTasks := []ReduceTask{}

	// Your code here.
	for i, file := range files {
		mapTasks = append(mapTasks, MapTask{
			id: i,
			filename: file,
			state: Idle,
		})
	}

	for j := 0; j < nReduce; j++ {
		reduceTasks = append(reduceTasks, ReduceTask{
			id: j,
			state: Idle,
		})
	}

	c.M = mapTasks
	c.R = reduceTasks
	c.Phase = Map

	c.server(sockname)
	return &c
}
