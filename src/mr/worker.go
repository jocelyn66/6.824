package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv" // mark
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
	"io/ioutil"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//mrak
// for sorting by key.
type ByKey []KeyValue

// for sorting by key. 
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a() // mark
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) { // mark reduce第二个参数[]

	// Your worker implementation here.
	// done
	for {
		task := getTask()
		switch task.TaskType {
		case MapTask:
			doMapTask(mapf, task)
			callDone(task)
		case ReduceTask:
			doReduceTask(reducef, task)
			callDone(task)
		case WaitTask:
			fmt.Println("All tasks are in progress, wait...")
			time.Sleep(time.Second) // mark
		case ExitTask:
			fmt.Println("all tasks are completed, worker exit now")
			os.Exit(0) // todo or return
		default:
			fmt.Fprintf(os.Stderr, "undefined task type %v\n", task.TaskType)
		}
	}

}

// done
func doMapTask(mapf func(string, string) []KeyValue, task *Task) {
	filename := task.FileName[0]
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open file %s", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close() // note 别忘

	intermediate := mapf(filename, string(content)) // note []byte -> string

	// 保存
	rn := task.ReduceNum
	kvs := make([][]KeyValue, rn) // mark 类型不是string
	for _, kv := range intermediate {
		kvs[ihash(kv.Key)%rn] = append(kvs[ihash(kv.Key)%rn], kv) // mark %rn
	}

	// var res []string

	for i := 0; i < rn; i++ {
		oname := "mr-tmp-" + strconv.Itoa(task.TaskId) + "-" + strconv.Itoa(i)
		ofile, err := os.Create(oname)
		if err != nil {
			log.Fatalf("Could not create file %s: %v", oname, err)
		}
		enc := json.NewEncoder(ofile) // mark json
		for _, kv := range kvs[i] {
			enc.Encode(kv)
		}
		ofile.Close()
		// res = append(res,oname)
	}

	// return res

}

// done
func doReduceTask(reducef func(string, []string) string, task *Task) {

	taskId := task.TaskId
	intermediate := shuffle(task.FileName)
	// fmt.Printf("intermediate size: %d\n", len(intermediate))

	dir, _ := os.Getwd()
	tmpFile, err := ioutil.TempFile(dir, fmt.Sprintf("mr-tmp-%d", taskId)) // tag 临时文件 原子命名
	if err != nil {
		log.Fatal("failed to create temporary file", err)
	}

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value) // 相同key的value集合
		}
		output := reducef(intermediate[i].Key, values)//对每个key调一次reduce函数

		// this is the correct format for each line of Reduce output.
		// fmt.Printf("output result %v %v\n", intermediate[i].Key, output)
		fmt.Fprintf(tmpFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	tmpFile.Close()
	
	oname := fmt.Sprintf("mr-out-%d", task.TaskId)
	os.Rename(tmpFile.Name(), oname) // 原子重命名

}

func shuffle(filenames []string) []KeyValue {
	res := []KeyValue{}
	for _, name := range filenames {
		file, err := os.Open(name)
		if err != nil {
			log.Fatalf("Could not open file %s: %v", name, err)
		}
		decoder := json.NewDecoder(file)
		
		var kv KeyValue

		for {
			if err := decoder.Decode(&kv); err == nil { // note
				res = append(res, kv)
			} else {
				break
			}
		}

		file.Close()
	}
	sort.Sort(ByKey(res))
	return res
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() { // tag rpc

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// done
func getTask() * Task{

	args := TaskArgs{}
	reply := Task{}

	ok := call("Coordinator.PollTask", &args, &reply)

	if ok {
		fmt.Printf("by rpc get task: %v\n", reply)
	} else {
		fmt.Println("getTask call failed!")
	}

	return &reply
}

// done
func callDone(task *Task) {

	reply := TaskArgs{}

	ok := call("Coordinator.CloseTask", &task, &reply)

	if ok {
		fmt.Printf("task finished successfully: %v\n", task)
	} else {
		fmt.Println("callDone failed!")
	}

}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool { // tag rpc
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234") // todo
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply) // 通过上面的socket调用rpcname(Coordinate.函数名)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
