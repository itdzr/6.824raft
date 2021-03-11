package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

type Parallelism struct {
	mu  sync.Mutex
	now int32
	max int32
}

// Worker holds the state for a server waiting for DoTask or Shutdown RPCs
type Worker1 struct {
	sync.Mutex

	name        string
	Map         func(string, string) []KeyValue
	Reduce      func(string, []string) string
	nRPC        int // quit after this many RPCs; protected by mutex
	nTasks      int // total tasks executed; protected by mutex
	concurrent  int // number of parallel DoTasks in this worker; mutex
	l           net.Listener
	parallelism *Parallelism
}

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

	// uncomment to send the Example RPC to the master.
	// CallExample()
	for {
		taskInfo := CallAskTask()
		switch taskInfo.State {
		case TaskMap:
			workerMap(mapf, taskInfo)
			break
		case TaskReduce:
			workerReduce(reducef, taskInfo)
			break
		case TaskWait:
			// wait for 5 seconds to requeset again
			time.Sleep(time.Duration(time.Second * 5))
			break
		case TaskEnd:
			fmt.Println("Master all tasks complete. Nothing to do...")
			// exit worker process
			return
		default:
			panic("Invalid Task state received by worker")
		}
	}

}
func CallAskTask() *TaskInfo {
	args := ExampleArgs{}
	reply := TaskInfo{}
	call("Master.AskTask", &args, &reply)
	return &reply
}

func CallTaskDone(taskInfo *TaskInfo) {
	reply := ExampleReply{}
	call("Master.TaskDone", taskInfo, &reply)
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func workerMap(mapf func(string, string) []KeyValue, taskInfo *TaskInfo) {
	fmt.Printf("Got assigned map task on %vth file %v\n", taskInfo.FileIndex, taskInfo.FileName)
	filename := taskInfo.FileName
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
		return
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
		return
	}
	file.Close()
	kva := mapf(filename, string(content))
	kmap := make(map[string][]string, len(kva))
	for _, kv := range kva {
		key := kv.Key
		kmap[key] = append(kmap[key], kv.Value)
	}
	nReduce := taskInfo.NReduce
	outprefix := "mr-tmp/mr-" + strconv.Itoa(taskInfo.FileIndex) + "-"
	fmt.Print(outprefix)
	outFiles := make([]*os.File, nReduce)
	outEncFiles := make([]*json.Encoder, nReduce)

	for idx := 0; idx < nReduce; idx++ {
		outFiles[idx], _ = ioutil.TempFile("mr-tmp", "mr-tmp-*")
		outEncFiles[idx] = json.NewEncoder(outFiles[idx])
	}

	for k, v := range kmap {
		outindex := ihash(k) % nReduce
		enc := outEncFiles[outindex]
		kv := fmt.Sprintf("%v:%v", k, v)
		err := enc.Encode(kv)
		if err != nil {
			fmt.Printf("File %v Key %v Value %v Error: %v\n", taskInfo.FileName, k, v, err)
			panic("Json encode failed")
		}
	}

	for outindex, file := range outFiles {
		outname := outprefix + strconv.Itoa(outindex)
		os.Rename(file.Name(), outname)
		file.Close()
	}
	CallTaskDone(taskInfo)

}
func workerReduce(reducef func(string, []string) string, taskInfo *TaskInfo) {
	fmt.Printf("Got assigned reduce task on part %v\n", taskInfo.ReduceIndex)
	outname := "mr-out-" + strconv.Itoa(taskInfo.ReduceIndex)
	//fmt.Printf("%v\n", taskInfo)

	// read from output files from map tasks

	innameprefix := "mr-tmp/mr-"
	innamesuffix := "-" + strconv.Itoa(taskInfo.ReduceIndex)

	// read in all files as a kv array
	intermediate := []KeyValue{}
	for index := 0; index < taskInfo.NFiles; index++ {
		inname := innameprefix + strconv.Itoa(index) + innamesuffix
		file, err := os.Open(inname)
		if err != nil {
			fmt.Printf("Open intermediate file %v failed: %v\n", inname, err)
			panic("Open file error")
		}
		mapTaskOutput, err := ioutil.ReadAll(file)
		if err != nil {
			fmt.Printf("read intermediate file %v failed: %v\n", inname, err)
			panic("read file error")
		}
		currKeyValuePairs := make([]KeyValue, 0)
		if err := json.Unmarshal(mapTaskOutput, &currKeyValuePairs); err != nil {
			fmt.Printf("unmarshal file %v failed: %v\n", inname, err)
			panic("unmarshal  error")
		}
		intermediate = append(intermediate, currKeyValuePairs...)
	}

	kmap := make(map[string][]string, len(intermediate))
	for _, kv := range intermediate {
		key := kv.Key
		kmap[key] = append(kmap[key], kv.Value)
	}

	//ofile, err := os.Create(outname)
	ofile, err := ioutil.TempFile("mr-tmp", "mr-*")
	if err != nil {
		fmt.Printf("Create output file %v failed: %v\n", outname, err)
		panic("Create file error")
	}
	//fmt.Printf("%v\n", intermediate)

	for k, v := range kmap {
		output := reducef(k, v)
		fmt.Fprintf(ofile, "%v %v\n", k, output)
	}

	os.Rename(filepath.Join(ofile.Name()), outname)
	ofile.Close()
	// acknowledge master
	CallTaskDone(taskInfo)

}
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
