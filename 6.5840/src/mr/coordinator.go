package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"
import "sync"
import "fmt"
import "errors"

type TaskPhase int //任务当前类型 map or reduce
type TaskStatus int //任务当前状态

const (//任务类型定义
	TaskPhase_Map	 TaskPhase = 0//map类型
	TaskPhase_Reduce TaskPhase = 1//reduce类型
)

const (//任务状态定义
	TaskStatus_New 		TaskStatus = 0//未创建
	TaskStatus_Ready	TaskStatus = 1//进入队列
	TaskStatus_Running	TaskStatus = 2//运行
	TaskStatus_Finished	TaskStatus = 3//结束
	TaskStatus_Error	TaskStatus = 4//错误
)

const (
	ScheduleInterval = time.Millisecond * 500//扫描间隔
	MaxTaskRunningTime = time.Second * 5//最大运行时长
)

type Task struct {
	FileName 	string//文件名
	Phase 		TaskPhase//类型
	Seq 		int//序列
	NMap 		int//有几个file
	NReduce 	int//有几个分区
	Alive 		bool//是否存活
}

type TaskState struct {
	Status 		TaskStatus//状态
	WorkerId 	int//执行当前task的worker的id
	StartTime 	time.Time//开始时间
}

type Coordinator struct {
	// Your definitions here.
	files 		[]string//要处理的文件列表
	nReduce 	int//分区数量
	taskPhase 	TaskPhase//类型 
	taskStates 	[]TaskState//状态
	taskChan 	chan Task//任务队列
	workerSeq 	int//worker最大序列
	done 		bool//是否完成
	muLock 		sync.Mutex//锁
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
// func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
// 	reply.Y = args.X + 1
// 	return nil
// }


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
	c.muLock.Lock()
	defer c.muLock.Unlock()

	return c.done
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files: 		files,
		nReduce: 	nReduce,
		taskPhase: 	TaskPhase_Map,
		taskStates: make([]TaskState, len(files)),
		workerSeq: 	0,
		done: 		false,
	}
	if len(files) > nReduce {
		c.taskChan = make(chan Task, len(files))
	} else{
		c.taskChan = make(chan Task, nReduce)
	}


	// Your code here.

	go c.schedule()
	c.server()
	fmt.Printf("Coordinator init\n")

	return &c
}


func (c *Coordinator) NewTask(seq int) Task {
	task := Task{
		FileName: 	"",
		Phase: 		c.taskPhase,
		Seq: 		seq,
		NMap: 		len(c.files),
		NReduce: 	c.nReduce,
		Alive: 		true,
	}

	if task.Phase == TaskPhase_Map {
		task.FileName = c.files[seq]
	}
	//fmt.Printf("new a task alive:%v\n", task.Alive)
	return task
}

func (c *Coordinator) scanTaskStat() {
	c.muLock.Lock()
	defer c.muLock.Unlock()

	if c.done {
		return
	}

	allDone := true

	for k, v := range c.taskStates {
		switch v.Status {
			case TaskStatus_New:
				allDone = false
				c.taskStates[k].Status = TaskStatus_Ready
				c.taskChan <- c.NewTask(k)
			case TaskStatus_Ready:
				allDone = false
			case TaskStatus_Running:
				allDone = false
				if time.Now().Sub(v.StartTime) > MaxTaskRunningTime {
					c.taskStates[k].Status = TaskStatus_Ready
					c.taskChan <- c.NewTask(k)
				}
			case TaskStatus_Finished:
			case TaskStatus_Error:
				allDone = false
				c.taskStates[k].Status = TaskStatus_Ready
				c.taskChan <- c.NewTask(k)
			default:
				panic("t. status err in schedule")
		}
	}

	if allDone {
		if c.taskPhase == TaskPhase_Map {
			c.taskPhase = TaskPhase_Reduce
			c.taskStates = make([]TaskState, c.nReduce)
		} else {
			log.Println("finish all tasks")
			c.done = true
		}
	}
}

func (c *Coordinator) schedule() {
	for !c.Done() {
		c.scanTaskStat()
		time.Sleep(ScheduleInterval)
	}
}

func (c *Coordinator) GetTask(args *TaskArgs, reply *TaskReply) error {
	task := <-c.taskChan
	reply.Task = &task

	if task.Alive {
		c.muLock.Lock()
		if task.Phase != c.taskPhase {
			c.muLock.Unlock()
			return errors.New("GetTask Task phase neq")
		}
		c.taskStates[task.Seq].WorkerId = args.WorkerId
		c.taskStates[task.Seq].Status = TaskStatus_Running
		c.taskStates[task.Seq].StartTime = time.Now()
		c.muLock.Unlock()
	}

	return nil
}

func (c *Coordinator) RegistWorker(args *RegistArgs, reply *RegistReply) error {
	c.muLock.Lock()
	defer c.muLock.Unlock()
	c.workerSeq++
	reply.WorkerId = c.workerSeq
	return nil
}

func (c *Coordinator) ReportTask(args *ReportArgs, reply *ReportReply) error {
	c.muLock.Lock()
	defer c.muLock.Unlock()
	if c.taskPhase != args.Phase || c.taskStates[args.Seq].WorkerId != args.WorkerId {
		fmt.Printf("in report task,workerId=%v report a useless task=%v, c.Phase=%v, args.Phase=%v\n", args.WorkerId, args.Seq, c.taskPhase, args.Phase)
		return nil
	}
	if args.Done {
		c.taskStates[args.Seq].Status = TaskStatus_Finished
	} else {
		c.taskStates[args.Seq].Status = TaskStatus_Error
	}

	go c.scanTaskStat()
	return nil
}