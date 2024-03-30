package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	alive := true

	for alive {
		// fmt.Println("Start requiring a task")
		task := RequireTask()
		switch task.TaskType {
		case MapTask:
			{
				// fmt.Println("Get map task ", task.MapId)
				ExecuteMapTask(mapf, task)
				TaskIsDone(task)
			}
		case ReduceTask:
			{
				// fmt.Println("Get reduce task ", task.ReduceId)
				ExecuteReduceTask(reducef, task)
				TaskIsDone(task)
			}
		case NoTask:
			{
				// fmt.Println("Get no task ")
				time.Sleep(time.Second)
			}
		case ExitTask:
			{
				// fmt.Println("Get exit task")
				alive = false
			}
		}
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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

	fmt.Println(err)
	return false
}

// require a task from coordinator
func RequireTask() *Task {
	args := ExampleArgs{}
	reply := Task{}
	call("Coordinator.AssignTasks", &args, &reply)
	return &reply
}

// execute a map task
func ExecuteMapTask(mapf func(string, string) []KeyValue, task *Task) {
	// fmt.Println("Start executing map task ", task.TaskId)
	// fmt.Printf("inputfile length: %d\n", len(task.InputFiles))
	filename := task.InputFiles[0]
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	kva := mapf(filename, string(content))
	reduceNum := task.ReduceNum
	HashedKV := make([][]KeyValue, reduceNum)

	for _, kv := range kva {
		HashedKV[ihash(kv.Key)%reduceNum] = append(HashedKV[ihash(kv.Key)%reduceNum], kv)
	}

	for i := 0; i < reduceNum; i++ {
		dir, _ := os.Getwd()
		tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
		if err != nil {
			log.Fatal("Failed to create temp file", err)
		}
		enc := json.NewEncoder(tempFile)
		for _, kv := range HashedKV[i] {
			enc.Encode(kv)
		}
		tempFile.Close()
		oname := "mr-tmp-" + strconv.Itoa(task.MapId) + "-" + strconv.Itoa(i+1)
		os.Rename(tempFile.Name(), oname)
	}
	// fmt.Println("Finishing executing map task ", task.TaskId)
}

// execute a reduce task
func ExecuteReduceTask(reducef func(string, []string) string, task *Task) {
	// fmt.Println("Start executing reduce task ", task.TaskId)
	inputFiles := task.InputFiles
	var intermediate []KeyValue
	for _, filename := range inputFiles {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("Failed to open file %v", filename)
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

	dir, _ := os.Getwd()
	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("Failed to create temp file", err)
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

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	tempFile.Close()
	oname := "mr-out-" + strconv.Itoa(task.ReduceId)
	os.Rename(tempFile.Name(), oname)
	// fmt.Println("Finish executing reduce task ", task.TaskId)
}

// notice coordinator the task is done
func TaskIsDone(task *Task) {
	args := task
	reply := ExampleReply{}
	call("Coordinator.TaskIsDone", &args, &reply)
}
