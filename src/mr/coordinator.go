package mr

import (
	"log"
	"net"
	"os"
	"net/rpc"
	"net/http"
	"sync"
	"time"
)

// Job status
// Job assignment
// RPC
// Timeout
type Coordinator struct {
	lock sync.Mutex
	mapTasks [] Task
	reduceTasks [] Task
	nReduce int
	mapFinished bool
	reduceFinished bool
	filenames []string
	nextTaskID int
}

type Task struct {
	filename string
	status string
	heartbeatTime time.Time
	taskID int
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) checkTimeout() {
	TIMEOUT_DURATION := 10 * time.Second
	now := time.Now()

	// map timeout
	if !c.mapFinished {
		allCompleted := true
		for i,task := range c.mapTasks {
			if task.status == InProgress && now.Sub(task.heartbeatTime) > TIMEOUT_DURATION {
				c.mapTasks[i].status = Idle
			}
			if task.status != Completed {
				allCompleted = false
			}
		}
		c.mapFinished = allCompleted
	}
	// reduce timeout
	allCompleted := true
	for i,task := range c.reduceTasks {
		if task.status == InProgress && now.Sub(task.heartbeatTime) > TIMEOUT_DURATION {
			c.reduceTasks[i].status = Idle
		}
		if task.status != Completed {
			allCompleted = false
		}
	}
	c.reduceFinished = allCompleted
}

// Assign Job to Worker
func (c *Coordinator) ApplyJob(args *ApplyJobArgs, reply *ApplyJobReply) error {
	
	// mutex lock
	c.lock.Lock()
	defer c.lock.Unlock()
	
	// Timeout check
	c.checkTimeout()

	if c.reduceFinished {
		// log.Printf("All tasks completed. Calling Worker %d to exit.\n", args.WorkerID)
		reply.TaskType = ExitTask
		return nil
	}
	
	if !c.mapFinished {
		// Assign Map Task
		for i,task := range c.mapTasks {
			if task.status == Idle {
				// log.Printf("Assigning Map Task %d to Worker %d\n", task.taskID, args.WorkerID)
				reply.TaskType = MapTask
				reply.TaskID = task.taskID
				reply.Filename = task.filename
				reply.NReduce = c.nReduce
				
				c.mapTasks[i].status = InProgress
				c.mapTasks[i].heartbeatTime = time.Now()
				return nil
			}
		}
		// No Map Task available & Map not finished
		reply.TaskType = WaitTask
		return nil
	}
	// Assign Reduce Task
	for i,task := range c.reduceTasks {
		if task.status == Idle {
			// log.Printf("Assigning Reduce Task %d to Worker %d\n", task.taskID, args.WorkerID)
			reply.TaskType = ReduceTask
			reply.TaskID = task.taskID
			reply.MapTaskNum = len(c.mapTasks)
			reply.ReduceTaskNum = i
			
			c.reduceTasks[i].status = InProgress
			c.reduceTasks[i].heartbeatTime = time.Now()
			return nil
		}
	}
	// No Reduce Task available
	reply.TaskType = WaitTask
	return nil
}

func (c *Coordinator) ReportJob(args *ReportJobArgs, reply *ReportJobReply) error {
	// mutex lock
	c.lock.Lock()
	defer c.lock.Unlock()

	if args.TaskType == MapTask {
		for i,task := range c.mapTasks {
			if task.taskID == args.TaskID {
				if args.Status == Completed{
					c.mapTasks[i].status = Completed

					allCompleted := true
					for _, tsk := range c.mapTasks {
						if tsk.status != Completed {
							allCompleted = false
							break
						}
					}
					c.mapFinished = allCompleted
					// log.Printf("Map Task %d completed by Worker %d\n", args.TaskID, args.WorkerID)
					reply.ACK = true
					return nil
				}else if args.Status == InProgress { // heartbeat
					c.mapTasks[i].heartbeatTime = time.Now()
					reply.ACK = true
					return nil
				}
			}
		}
	}else if args.TaskType == ReduceTask {
		for i,task := range c.reduceTasks {
			if task.taskID == args.TaskID {
				if args.Status == Completed{
					c.reduceTasks[i].status = Completed
					
					allCompleted := true
					for _, tsk := range c.reduceTasks {
						if tsk.status != Completed {
							allCompleted = false
							break
						}
					}
					c.reduceFinished = allCompleted
					// log.Printf("Reduce Task %d completed by Worker %d\n", args.TaskID, args.WorkerID)
					reply.ACK = true
					return nil
				}else if args.Status == InProgress { // heartbeat
					c.reduceTasks[i].heartbeatTime = time.Now()
					reply.ACK = true
					return nil
				}
			}
		}
	}
	reply.ACK = false
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
	c.lock.Lock()
	defer c.lock.Unlock()

	// Update task statuses based on heartbeat timeouts
	c.checkTimeout()

	// Consider job done when both map and reduce phases finished
	return c.mapFinished && c.reduceFinished
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{
		filenames:files,
		mapTasks: make([] Task, len(files)),
		reduceTasks: make([] Task, nReduce),
		nReduce: nReduce,
		mapFinished: false,
		reduceFinished: false,
		nextTaskID:0,
	}

	for i,filename := range c.filenames {
		c.mapTasks[i] = Task{
			filename: filename,
			status: Idle,
			taskID: i,
		}
	}

	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = Task{
			status: Idle,
			taskID: i,
		}
	}
	c.server(sockname)
	return &c
}
