package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type TaskStat struct {
	fileName    string
	nReduce     int
	reduceIndex int
	fileIndex   int
	beginTime   time.Time
	nFiles      int
}

type MapTaskStat struct {
	TaskStat
}
type ReduceTaskStat struct {
	TaskStat
}
type TaskStatInterface interface {
	GenerateTaskInfo() TaskInfo
	OutOfTime() bool
	SetNow()
	GetFileIndex() int
	GetReduceIndex() int
}

func (this *MapTaskStat) GenerateTaskInfo() TaskInfo {
	return TaskInfo{
		State:       TaskMap,
		FileName:    this.fileName,
		FileIndex:   this.fileIndex,
		ReduceIndex: this.reduceIndex, //map task is 0
		NReduce:     this.nReduce,
		NFiles:      this.nFiles,
	}
}

func (this *ReduceTaskStat) GenerateTaskInfo() TaskInfo {
	return TaskInfo{
		State:       TaskReduce,
		FileName:    this.fileName,
		FileIndex:   this.fileIndex,
		ReduceIndex: this.reduceIndex,
		NReduce:     this.nReduce,
		NFiles:      this.nFiles,
	}
}

func (this *TaskStat) OutOfTime() bool {
	return time.Now().Sub(this.beginTime) > time.Second*60

}

func (this *TaskStat) SetNow() {
	this.beginTime = time.Now()
}

func (this *TaskStat) GetFileIndex() int {
	return this.fileIndex
}

func (this *TaskStat) GetReduceIndex() int {
	return this.reduceIndex
}

type TaskStatQueue struct {
	taskArray []TaskStatInterface
	mutex     sync.Mutex
}

func (this *TaskStatQueue) lock() {
	this.mutex.Lock()
}

func (this *TaskStatQueue) unlock() {
	this.mutex.Lock()
}

func (this *TaskStatQueue) Size() int {
	return len(this.taskArray)
}
func (this *TaskStatQueue) Pop() TaskStatInterface {
	this.lock()
	defer this.unlock()
	length := len(this.taskArray)
	if length == 0 {
		return nil
	}
	ts := this.taskArray[length-1]
	this.taskArray = this.taskArray[:length-1]
	return ts
}
func (this *TaskStatQueue) Push(taskStat TaskStatInterface) {
	this.lock()
	defer this.unlock()
	if taskStat == nil {
		this.unlock()
		return
	}
	this.taskArray = append([]TaskStatInterface{taskStat}, this.taskArray...)
}
func (this *TaskStatQueue) TimeOutQueue() []TaskStatInterface {
	outArray := make([]TaskStatInterface, 0)
	this.lock()
	defer this.unlock()
	for _, taskStat := range this.taskArray {
		if !taskStat.OutOfTime() {
			outArray = append(outArray, taskStat)
		}
	}
	return outArray

}

func (this *TaskStatQueue) MoveAppend(rhs []TaskStatInterface) {
	this.lock()
	this.taskArray = append(this.taskArray, rhs...)
	rhs = make([]TaskStatInterface, 0)
	this.unlock()
}

func (this *TaskStatQueue) RemoveTask(fileIndex int, partIndex int) {
	this.lock()
	for index := 0; index < len(this.taskArray); {
		task := this.taskArray[index]
		if fileIndex == task.GetFileIndex() && partIndex == task.GetReduceIndex() {
			this.taskArray = append(this.taskArray[:index], this.taskArray[index+1:]...)
		} else {
			index++
		}
	}
	this.unlock()
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//

type Master struct {
	filenames []string

	// reduce task queue
	reduceTaskWaiting TaskStatQueue
	reduceTaskRunning TaskStatQueue

	// map task statistics
	mapTaskWaiting TaskStatQueue
	mapTaskRunning TaskStatQueue

	// machine state
	isDone  bool
	nReduce int
}

func (this *Master) AskTask(args *ExampleArgs, reply *TaskInfo) error {
	if this.isDone {
		reply.State = TaskEnd
	}
	mapTask := this.mapTaskWaiting.Pop()
	if mapTask != nil {
		mapTask.SetNow()
		this.mapTaskRunning.Push(mapTask)
		*reply = mapTask.GenerateTaskInfo()
		fmt.Printf("Distributing map task on %vth file %v\n", reply.FileIndex, reply.FileName)
		return nil

	}
	reduceTask := this.reduceTaskWaiting.Pop()
	if reduceTask != nil {
		// an available reduce task
		// record task begin time
		reduceTask.SetNow()
		// note task is running
		this.reduceTaskRunning.Push(reduceTask)
		// setup a reply
		*reply = reduceTask.GenerateTaskInfo()
		fmt.Printf("Distributing reduce task on part %v %vth file %v\n", reply.ReduceIndex, reply.FileIndex, reply.FileName)
		return nil
	}
	if this.mapTaskRunning.Size() > 0 || this.reduceTaskRunning.Size() > 0 {
		reply.State = TaskWait
		return nil
	}
	reply.State = TaskEnd
	this.isDone = true
	return nil
}

func (this *Master) distributeReduce() {
	for reduceIndex := 0; reduceIndex < this.nReduce; reduceIndex++ {
		reduceTask := ReduceTaskStat{
			TaskStat{
				fileIndex:   0,
				reduceIndex: reduceIndex,
				nReduce:     this.nReduce,
				nFiles:      len(this.filenames),
			},
		}
		this.reduceTaskWaiting.Push(&reduceTask)
	}
}

func (this *Master) TaskDone(args *TaskInfo, reply *ExampleReply) error {
	switch args.State {
	case TaskMap:
		fmt.Printf("Map task on %vth file %v complete\n", args.FileIndex, args.FileName)
		this.mapTaskRunning.RemoveTask(args.FileIndex, args.ReduceIndex)
		if this.mapTaskRunning.Size() == 0 && this.mapTaskWaiting.Size() == 0 {
			// all map tasks done
			// can distribute reduce tasks
			this.distributeReduce()
		}
		break
	case TaskReduce:
		fmt.Printf("Reduce task on %vth part complete\n", args.ReduceIndex)
		this.reduceTaskRunning.RemoveTask(args.FileIndex, args.ReduceIndex)
		break
	default:
		panic("Task Done error")
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//

func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (this *Master) Done() bool {

	// Your code here.

	return this.isDone
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {

	mapArray := make([]TaskStatInterface, 0)
	for idx, file := range files {
		mapTask := MapTaskStat{
			TaskStat{
				fileName:    file,
				nReduce:     nReduce,
				fileIndex:   idx,
				reduceIndex: 0,
				nFiles:      len(files),
			}}

		mapArray = append(mapArray, &mapTask)

	}
	m := Master{mapTaskWaiting: TaskStatQueue{taskArray: mapArray},
		nReduce:   nReduce,
		filenames: files,
	}
	if _, err := os.Stat("mr-tmp"); os.IsNotExist(err) {
		if err = os.Mkdir("mr-tmp", os.ModePerm); err != nil {
			fmt.Printf("Create tmp directory failed... Error: %v\n", err)
			panic("Create tmp directory failed...")
		}

	}
	go m.collectOutOfTime()
	m.server()
	return &m
}

// Your code here.

func (this *Master) collectOutOfTime() {
	for {
		time.Sleep(time.Second * 5)
		timeouts := this.mapTaskRunning.TimeOutQueue()
		if len(timeouts) > 0 {
			this.mapTaskWaiting.MoveAppend(timeouts)
		}

		timeouts = this.reduceTaskRunning.TimeOutQueue()
		if len(timeouts) > 0 {
			this.reduceTaskWaiting.MoveAppend(timeouts)
		}

	}
}
