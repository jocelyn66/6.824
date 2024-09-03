package mr

import (
	"fmt"
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

type Coordinator struct {
	nReduce int
	mapTaskChan chan *Task
	reduceTaskChan chan *Task
	taskMetaHolder taskMetaHolder
	jobPhase jobPhase
	taskIdCounter int // 计数器，用于生成task id
	// intermediate []string
}

type taskState int

type jobPhase int 

type taskMetaHolder map[int]*taskMetaInfo // mark 不同

const (
	Idle taskState = iota
	Running
	Done
)

const (
	MapPhase jobPhase = iota
	ReducePhase
	AllDone
)

type taskMetaInfo struct {
	task *Task
	state taskState
	startTime time.Time // tag 心跳
}

var mutex sync.Mutex // tag 锁

// Your code here -- RPC handlers for the worker to call.
// done
func (c *Coordinator) PollTask(args *TaskArgs, reply *Task) error {
	
	mutex.Lock()
  	defer mutex.Unlock()

	switch c.jobPhase {
		case MapPhase:
			if len(c.mapTaskChan) > 0 { // note
				*reply = *<- c.mapTaskChan // note
				if c.wakeIdleTask(reply.TaskId){
					fmt.Printf("map task %d is running...\n", reply.TaskId)
				}
			} else {
				reply.TaskType = WaitTask
				if c.checkTaskFinish() {
					c.toNextPhase()
				}
			}
		case ReducePhase:
			if len(c.reduceTaskChan) > 0 {
				*reply = *<- c.reduceTaskChan
				if c.wakeIdleTask(reply.TaskId){
					fmt.Printf("reduce task %d is running...\n", reply.TaskId)
				}
			} else {
				reply.TaskType = WaitTask
				if c.checkTaskFinish() {
					c.toNextPhase()
				}
			}
		default:
			*reply = Task{
				TaskType: ExitTask,
			} // reply.TaskType = ExitTask
	}
	return nil
}

func (c *Coordinator) wakeIdleTask(taskId int) bool{ // 是否idle
	task := c.taskMetaHolder[taskId]
	if  task!= nil && task.state == Idle{
		task.state = Running
		task.startTime = time.Now() // tag 心跳
		return true
	}
	return false
}

// done
func (c *Coordinator) CloseTask(args *Task, reply *TaskArgs) error {

	mutex.Lock()
  	defer mutex.Unlock()

	task, ok := c.taskMetaHolder[args.TaskId]

	switch args.TaskType {
	case MapTask:
		if ok && task.state == Running {
			task.state = Done
			fmt.Printf("Map task Id[%d] is finished.\n", args.TaskId)
		}
	case ReduceTask:
		if ok && task.state == Running {
			task.state = Done
			fmt.Printf("Reduce task Id[%d] is finished.\n", args.TaskId)
		}
	default:
		log.Fatal("undefined task type")
	}

	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error { // mark 远程调用方法签名
	reply.Y = args.X + 1
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

	mutex.Lock() // mark
	defer mutex.Unlock()

	// Your code here.
	if c.jobPhase == AllDone {
		fmt.Println("All tasks completed successfully! Coordinator will exit.")
		return true
	}
	return false
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	nMap := len(files)

	c := Coordinator{
		nReduce: nReduce,
		mapTaskChan: make(chan *Task, nMap),
		reduceTaskChan: make(chan *Task, nReduce),
		taskMetaHolder: make(map[int]*taskMetaInfo, nMap + nReduce), // mark size
		jobPhase: MapPhase,
		taskIdCounter: 0,
	}

	// 构造map任务
	c.makeMapTasks(files)
	go c.crashDetector()

	c.server()
	return &c
}

func (c *Coordinator) makeMapTasks(files []string) {
	for _, file := range files {
		task := &Task{
			TaskId: c.generateTaskId(),
			TaskType: MapTask,
			ReduceNum: c.nReduce,
			FileName: []string{file},
		}
		c.taskMetaHolder.add(&taskMetaInfo{
			task: task,
			state: Idle,
		})
		c.mapTaskChan <- task
		fmt.Printf("make a new map task %v\n", task)
	}
}

func (t taskMetaHolder) add(taskInfo *taskMetaInfo) bool { // mark 接收对象
	taskId := taskInfo.task.TaskId
	_, ok := t[taskId]
	if ok {
		fmt.Println("taskMetaHolder already contained task= ", taskId) // todo error
		return false
	} else {
		t[taskId] = taskInfo
		fmt.Println("meta accepts task id = ", taskId)
	}
	return true
}

func (c *Coordinator) makeReduceTasks() {

	for i := 0; i < c.nReduce; i++{

		filenames := c.getReduceFiles(i)

		if len(filenames) == 0 {
			continue
		}

		task := &Task{
			TaskId: c.generateTaskId(),
			TaskType: ReduceTask,
			ReduceNum: c.nReduce,
			FileName: filenames,
		}
		c.taskMetaHolder.add(&taskMetaInfo{
			task: task,
			state: Idle,
		})
		c.reduceTaskChan <- task

	}

}

func (c *Coordinator) getReduceFiles(reduceNum int) []string {
	var res []string
	path, _ := os.Getwd()
	intermediate, _ := ioutil.ReadDir(path)
	for _, fi := range intermediate {
		if strings.HasPrefix(fi.Name(), "mr-tmp") && strings.HasSuffix(fi.Name(), strconv.Itoa(reduceNum)) {
			res = append(res, fi.Name()) // note
		}
	}
	// fmt.Printf("reduce num %d need to process %d files\n", reduceNum, len(res))
	return res
}

func (c *Coordinator) generateTaskId() int {
	res := c.taskIdCounter
	c.taskIdCounter++
	return res
}

func (c *Coordinator) toNextPhase() {
	
	switch c.jobPhase {
	case MapPhase:
		c.makeReduceTasks()
		fmt.Println("move to map phase")
		c.jobPhase = ReducePhase // mark 顺序！
	case ReducePhase:
		fmt.Println("move to alldone phase")
		c.jobPhase = AllDone
	}

}

func (c *Coordinator) checkTaskFinish() bool {

	nMapDone := 0
	nMapUndone := 0
	nReduceDone := 0
	nReduceUndone := 0

	for _, task := range c.taskMetaHolder {
		if task.task.TaskType == MapTask {
			if task.state ==  Done{
				nMapDone++
			} else {
				nMapUndone++
			}
		} else {
			if task.state ==  Done{
				nReduceDone++
			} else {
				nReduceUndone++
			}
		}
	}

	if nMapDone > 0 && nMapUndone == 0 && nReduceDone == 0 && nReduceUndone == 0 {
		return true
	} else if nReduceDone > 0 && nReduceUndone == 0 {
		return true
	}

	return false

}

// done
func (c *Coordinator) crashDetector() {
	for {
		time.Sleep(time.Second * 2) // mark 轮转的作用

		mutex.Lock()
		// defer mutex.Unlock() // note 函数退出时执行，相当于这个协程关闭时

		if c.jobPhase == AllDone {
			mutex.Unlock() // mark
			break
		}

		for _, v  := range c.taskMetaHolder {
			if v.state == Running && time.Since(v.startTime) > 9 * time.Second {
				
				fmt.Printf("the task[%d] is crash, take [%d] s\n", v.task.TaskId, time.Since(v.startTime))

				switch v.task.TaskType {
				case MapTask:
					c.mapTaskChan <- v.task
					v.state = Idle
				case ReduceTask:
					c.reduceTaskChan <- v.task
					v.state = Idle

				}
			}
		}

		mutex.Unlock()
	}
}
