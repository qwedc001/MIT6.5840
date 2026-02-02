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

// Add your RPC definitions here.

const (
	MapTask = "map"
	ReduceTask = "reduce"
	ExitTask = "exit"
	WaitTask = "wait"
)

const (
	Idle = "idle"
	InProgress = "in-progress"
	Completed = "completed"
)

type ApplyJobArgs struct {
	WorkerID int
}

type ApplyJobReply struct {
	TaskID int
	TaskType string
	Filename string
	MapTaskNum int
	ReduceTaskNum int
	NReduce int
}

type ReportJobArgs struct {
	WorkerID int
	TaskID int
	TaskType string
	Status string
}

type ReportJobReply struct {
	ACK bool
}