package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "time"
import "encoding/json"
import "sort"


// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }


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

	workerID := os.Getpid()

	for {
		// Apply Job
		args := ApplyJobArgs{
			WorkerID: workerID,
		}
		reply := ApplyJobReply{}

		ok := call("Coordinator.ApplyJob", &args, &reply)
		if !ok {
			log.Fatalf("Worker %d: ApplyJob RPC failed", workerID)
		}
		switch reply.TaskType {
			case MapTask:
				// Handle Map Task (run with heartbeat)
				runWithHeartbeat(MapTask, reply.TaskID, workerID, func() {
					doMap(&reply, mapf, workerID)
				})
			case ReduceTask:
				// Handle Reduce Task (run with heartbeat)
				runWithHeartbeat(ReduceTask, reply.TaskID, workerID, func() {
					doReduce(&reply, reducef, workerID)
				})
			case ExitTask:
				// Exit
				fmt.Printf("Worker %d: Exiting\n", workerID)
				return
			case WaitTask:
				// Wait
				time.Sleep(500 * time.Millisecond)
			default:
				log.Fatalf("Worker %d: Unknown TaskType %s\n", workerID, reply.TaskType)
		}
	}
}

func runWithHeartbeat(taskType string, taskID int, workerID int, work func()) {
	stopCh := make(chan struct{})
	// heartbeat goroutine
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-stopCh:
				return
			case <-ticker.C:
				args := ReportJobArgs{
					WorkerID: workerID,
					TaskID:   taskID,
					TaskType: taskType,
					Status:   InProgress,
				}
				reply := ReportJobReply{}
				call("Coordinator.ReportJob", &args, &reply)
			}
		}
	}()

	// do the actual work
	work()

	// stop heartbeats
	close(stopCh)

	// final report: completed
	args := ReportJobArgs{
		WorkerID: workerID,
		TaskID:   taskID,
		TaskType: taskType,
		Status:   Completed,
	}
	reply := ReportJobReply{}
	ok := call("Coordinator.ReportJob", &args, &reply)
	if !ok {
		log.Printf("Worker %d: ReportJob Completed RPC failed for task %d", workerID, taskID)
	}
}

func doMap(reply *ApplyJobReply, mapf func(string, string) []KeyValue, workerID int) {
	// Read input file
	filename := reply.Filename
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Worker %d: cannot open %v", workerID, filename)
	}
	content, err := os.ReadFile(filename)
	if err != nil {
		log.Fatalf("Worker %d: cannot read %v", workerID, filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	intermediateFiles := make([][]KeyValue, reply.NReduce)
	for _, kv := range kva {
		bucket := ihash(kv.Key) % reply.NReduce
		intermediateFiles[bucket] = append(intermediateFiles[bucket], kv)
	}
	for i := 0; i < reply.NReduce; i++ {
		tempFile, err := os.CreateTemp(".", "mr-tmp-*")
		if err != nil {
			log.Fatalf("Worker %d: cannot create temp file", workerID)
		}
		enc := json.NewEncoder(tempFile)
		for _, kv := range intermediateFiles[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("Worker %d: cannot encode kv pair", workerID)
			}
		}
		tempFile.Close()
		finalName := fmt.Sprintf("mr-%d-%d", reply.TaskID, i)
		err = os.Rename(tempFile.Name(), finalName)
		if err != nil {
			log.Fatalf("Worker %d: cannot rename temp file", workerID)
		}
	}
	
	reportArgs := ReportJobArgs{
		WorkerID: workerID,
		TaskID:   reply.TaskID,
		TaskType: MapTask,
		Status:   Completed,
	}
	reportReply := ReportJobReply{}
	ok := call("Coordinator.ReportJob", &reportArgs, &reportReply)
	if !ok {
		log.Printf("Worker %d: ReportJob Completed RPC failed for map task %d", workerID, reply.TaskID)
	}
}

func doReduce(reply *ApplyJobReply, reducef func(string, []string) string, workerID int) {
	reduceTaskNum := reply.ReduceTaskNum
	mapTaskNum := reply.MapTaskNum
	intermediate := []KeyValue{}
	for i:=0; i<mapTaskNum; i++ {
		filename := fmt.Sprintf("mr-%d-%d", i, reduceTaskNum)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("Worker %d: cannot open %v", workerID, filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(intermediate))
	tempFile, err := os.CreateTemp(".", "mr-out-tmp-*")
	if err != nil {
		log.Fatalf("Worker %d: cannot create temp file for reduce output", workerID)
	}
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	tempFile.Close()
	os.Rename(tempFile.Name(), fmt.Sprintf("mr-out-%d", reduceTaskNum))
	
	reportArgs := ReportJobArgs{
		WorkerID: workerID,
		TaskID:   reply.TaskID,
		TaskType: ReduceTask,
		Status:   Completed,
	}
	reportReply := ReportJobReply{}
	ok := call("Coordinator.ReportJob", &reportArgs, &reportReply)
	if !ok {
		log.Printf("Worker %d: ReportJob Completed RPC failed for reduce task %d", workerID, reply.TaskID)
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
