package mr

import (
	// "fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var mu sync.Mutex

// Task type
const (
	ExitTask = iota
	MapTask
	ReduceTask
	NoTask
)

// Task status
const (
	Waiting = iota
	Doing
	Done
)

type Task struct {
	TaskId     int
	TaskType   int
	TaskStatus int
	InputFiles []string
	ReduceNum  int
	MapId      int
	ReduceId   int
	StartTime  time.Time
}

// Coordinator status
const (
	MapPhase = iota
	ReducePhase
	DonePhase
)

type Coordinator struct {
	// Your definitions here.
	MapTaskChannel    chan *Task
	ReduceTaskChannel chan *Task
	MapTaskNum        int
	ReduceTaskNum     int
	Status            int
	TaskMap           map[int]*Task
	CurrentTaskId     int
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AssignTasks(args *ExampleArgs, reply *Task) error {
	// fmt.Println("Start assigning task")
	mu.Lock()
	defer mu.Unlock()
	if c.Status == MapPhase {
		if len(c.MapTaskChannel) > 0 {
			*reply = *<-c.MapTaskChannel
			c.TaskMap[reply.TaskId].TaskStatus = Doing
			c.TaskMap[reply.TaskId].StartTime = time.Now()
		} else {
			reply.TaskType = NoTask
			mapTaskDoneNum := 0
			for _, task := range c.TaskMap {
				if task.TaskType == MapTask && task.TaskStatus == Done {
					mapTaskDoneNum++
				}
			}
			if mapTaskDoneNum == c.MapTaskNum {
				c.NextPhase()
			}
		}
	} else if c.Status == ReducePhase {
		if len(c.ReduceTaskChannel) > 0 {
			*reply = *<-c.ReduceTaskChannel
			c.TaskMap[reply.TaskId].TaskStatus = Doing
			c.TaskMap[reply.TaskId].StartTime = time.Now()
		} else {
			reply.TaskType = NoTask
			reduceTaskDoneNum := 0
			for _, task := range c.TaskMap {
				if task.TaskType == ReduceTask && task.TaskStatus == Done {
					reduceTaskDoneNum++
				}
			}
			if reduceTaskDoneNum == c.ReduceTaskNum {
				c.NextPhase()
			}
		}
	} else {
		reply.TaskType = ExitTask
	}
	// fmt.Println("Finish assigning task")
	return nil
}

func (c *Coordinator) TaskIsDone(args *Task, reply *ExampleReply) error {
	mu.Lock()
	defer mu.Unlock()
	taskId := args.TaskId
	c.TaskMap[taskId].TaskStatus = Done
	// fmt.Printf("Task %d is done...................", taskId)
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	mu.Lock()
	defer mu.Unlock()
	ret := false

	// Your code here.
	if c.Status == DonePhase {
		ret = true
	}
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		MapTaskChannel:    make(chan *Task, len(files)),
		ReduceTaskChannel: make(chan *Task, nReduce),
		MapTaskNum:        len(files),
		ReduceTaskNum:     nReduce,
		Status:            MapPhase,
		TaskMap:           make(map[int]*Task, len(files)+nReduce),
		CurrentTaskId:     0,
	}

	// Your code here.
	c.MakeMapTasks(files)

	c.server()

	go c.CrashHandler()

	return &c
}

func (c *Coordinator) MakeMapTasks(files []string) {
	mapID := 1
	// fmt.Println("Start making map tasks .............")
	for _, file := range files {
		id := c.getNewTaskId()
		task := Task{
			TaskId:     id,
			TaskType:   MapTask,
			TaskStatus: Waiting,
			InputFiles: []string{file},
			ReduceNum:  c.ReduceTaskNum,
			MapId:      mapID,
			ReduceId:   0,
		}
		c.TaskMap[id] = &task
		c.MapTaskChannel <- &task
		mapID++
	}
	// fmt.Println("Finish making map tasks ...........")

	// for i := 0; i < c.mapTaskNum; i++ {
	// 	task := c.taskMap[i]
	// 	fmt.Println(len(task.inputfiles), " ", task.mapId, " ", task.reduceId, " ", task.taskId, " ", task.taskStatus, " ", task.taskType)
	// }
}

func (c *Coordinator) makeReduceTasks() {
	// fmt.Println("Start making reduce tasks ..............")
	for i := 0; i < c.ReduceTaskNum; i++ {
		id := c.getNewTaskId()
		task := Task{
			TaskId:     id,
			TaskType:   ReduceTask,
			TaskStatus: Waiting,
			InputFiles: collectInputFilesNameForReduce(i + 1),
			ReduceNum:  c.ReduceTaskNum,
			MapId:      0,
			ReduceId:   i + 1,
		}
		c.TaskMap[id] = &task
		c.ReduceTaskChannel <- &task
	}
	// fmt.Println("Finish making reduce tasks ...............")
}

func collectInputFilesNameForReduce(reduceId int) []string {
	var result []string
	path, _ := os.Getwd()
	rd, _ := ioutil.ReadDir(path)
	for _, fi := range rd {
		if strings.HasPrefix(fi.Name(), "mr-tmp") && strings.HasSuffix(fi.Name(), strconv.Itoa(reduceId)) {
			result = append(result, fi.Name())
		}
	}
	return result
}

func (c *Coordinator) getNewTaskId() int {
	result := c.CurrentTaskId
	c.CurrentTaskId++
	return result
}

func (c *Coordinator) NextPhase() {
	if c.Status == MapPhase {
		// fmt.Println("MapPhase finishes , and move on to the ReducePhase")
		c.makeReduceTasks()
		c.Status = ReducePhase
	} else if c.Status == ReducePhase {
		// fmt.Println("ReducePhase finishes, and move on to the DonePhase")
		c.Status = DonePhase
	}
}

func (c *Coordinator) CrashHandler() {
	for {
		mu.Lock()
		if c.Status == DonePhase {
			mu.Unlock()
			break
		}
		for _, task := range c.TaskMap {
			if task.TaskStatus == Doing && time.Since(task.StartTime) > time.Second*10 {
				// fmt.Printf("task %d crashed\n", task.TaskId)
				task.TaskStatus = Waiting
				if task.TaskType == MapTask {
					c.MapTaskChannel <- task
				} else if task.TaskType == ReduceTask {
					c.ReduceTaskChannel <- task
				}
			}
		}
		mu.Unlock()
	}
}
