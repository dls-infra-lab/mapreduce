package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

type TaskState int
type CoordinatorPhase int

const (
	// task states
	Idle TaskState = 0
	InProgress TaskState = 1
	Completed TaskState = 2

	// coordinator states
	MapPhase CoordinatorPhase = 0
	ReducePhase CoordinatorPhase = 1
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
	nReduce int
	// needed so we know what phase
	// we're in and the coordinator
	// assigns certain tasks based on
	// phase we're on
	Phase CoordinatorPhase 
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//the RPC argument that the worker calls to request a new task
func (c *Coordinator) RequestTask(args *ReqArgs, reply *ReqReply) error {
	var phase CoordinatorPhase = c.Phase
	switch phase {
		case MapPhase:
			for _, task := range c.M {
				if task.state == Idle {
					// writing to reply
					reply.taskType = MapT
					reply.taskID = task.id
					reply.filename = task.filename
					reply.nReduce = c.nReduce
					return nil
				}
			}
		
		case ReducePhase:
			for _, task := range c.R {
				if task.state == Idle {
					reply.taskType = ReduceT
					reply.taskID = task.id
					// no filename since not needed
					reply.nReduce = c.nReduce
					return nil
				}
			}
	}
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

// updates a task's state to in-progress or completed
func (c *Coordinator) UpdateTaskState(args *UpdateTaskStateArgs, reply *UpdateTaskStateReply) error {
	// gets task and updates its state
	taskID := args.taskID
	taskType := args.taskType
	updatedState := args.updatedState
	if taskType == MapT {
		c.M[taskID].state = updatedState
		reply.updated = true
	} else if taskType == ReduceT {
		c.R[taskID].state = updatedState
		reply.updated = true
	} else {
		reply.updated = false
	}

	// checking if all map tasks are complete now to switch from map -> 
	// reduce phase
	for _, mapTask := range c.M {
		if mapTask.state != Completed {
			return nil
		}
	}
	
	// update coordinator phase to reduce if all map tasks are completed
	c.Phase = ReducePhase

	return nil
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// Checks if all of the tasks are done
	// both map and reduce tasks are considered one job
	for _, mTask := range c.M {
		if mTask.state != Completed {
			return false
		}
	}

	for _, rTask := range c.R {
		if rTask.state != Completed {
			return false
		}
	}

	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	// Your code here.
	c := Coordinator{
		M: []MapTask{},
		R: []ReduceTask{},
		nReduce: nReduce,
		Phase: MapPhase,
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

	c.server(sockname)
	return &c
}
