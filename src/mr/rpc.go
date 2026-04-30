package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here
type TaskType int
const (
	MapT TaskType = 0
	ReduceT TaskType = 1
)

// struct that worker sends out when requesting
// a task
type ReqArgs struct {}

// struct that worker expects to get back when
// coordinator provides a task
type ReqReply struct {
	taskType TaskType
	taskID int
	filename string
	nReduce int
}

// Structs to use if updating a task's status was
// successful
type UpdateTaskStateArgs struct {
	taskID int
	taskType TaskType
	updatedState TaskState
}

type UpdateTaskStateReply struct {
	updated bool
}

