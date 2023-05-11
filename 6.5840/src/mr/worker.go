package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "io/ioutil"
import "os"
import "encoding/json"
import "strings"
import "errors"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type worker struct {
	workerId	int
	mapF 		func(string, string) []KeyValue
	reduceF 	func(string, []string) string
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
	worker := worker{
		mapF: 		mapf,
		reduceF: 	reducef,
	}
	worker.register()
	worker.run()

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func (w *worker) run() {
	for {
		task, err := w.getTask()
		if err != nil {
			fmt.Printf("getTask failed!\n")
			continue
		}
		if !task.Alive {
			fmt.Printf("task not alive!\n")
			return
		}
		w.doTask(*task)
	}
}

func (w *worker) doTask(task Task) {
	switch task.Phase {
		case TaskPhase_Map:
			w.doMapTask(task)
		case TaskPhase_Reduce:
			w.doReduceTask(task)
		default:
			panic(fmt.Sprintf("task phase err: %v", task.Phase))
	}
}

func (w *worker) getReduceName(mapId, partitionId int) string {
	return fmt.Sprintf("mr-kv-%d-%d", mapId, partitionId)
}

func (w *worker) getMergeName(partitionId int) string {
	return fmt.Sprintf("mr-out-%d", partitionId)
}

func (w *worker) doMapTask(task Task) {
	cont, err := ioutil.ReadFile(task.FileName)
	if err != nil {
		fmt.Printf("read file failed!\n")
		w.reportTask(task, false)
		return
	}
	kvs := w.mapF(task.FileName, string(cont))
	partions := make([][]KeyValue, task.NReduce)
	for _, kv := range kvs {
		pid := ihash(kv.Key) % task.NReduce
		partions[pid] = append(partions[pid], kv)
	}

	for k, v := range partions {
		filename := w.getReduceName(task.Seq, k)
		file, err := os.Create(filename)
		if err != nil {
			fmt.Printf("create file failed!\n")
			w.reportTask(task, false)
			return
		}
		encoder := json.NewEncoder(file)
		for _, kv := range v {
			err := encoder.Encode(&kv)
			if err != nil {
				fmt.Printf("encode kvs to file failed!\n")
				w.reportTask(task, false)
			}
		}
		err1 := file.Close()
		if err1 != nil {
			fmt.Printf("close file failed!\n")
			w.reportTask(task, false)
		}
	}
	w.reportTask(task, true)
}

func (w *worker) doReduceTask(task Task) {
	maps := make(map[string][]string)
	for i := 0; i < task.NMap; i++ {
		fileName := w.getReduceName(i, task.Seq)
		file, err := os.Open(fileName)
		if err != nil {
			fmt.Printf("open file failed!\n")
			w.reportTask(task, false)
			return
		}
		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			if _, ok := maps[kv.Key]; !ok {
				maps[kv.Key] = make([]string, 0)
			}
			maps[kv.Key] = append(maps[kv.Key], kv.Value)
		}
	}
	res := make([]string, 0)
	for k, v := range maps {
		len := w.reduceF(k, v)
		res = append(res, fmt.Sprintf("%v %v\n", k, len))
	}
	
	fileName := w.getMergeName(task.Seq)
	if err := ioutil.WriteFile(fileName, []byte(strings.Join(res, "")), 0600); err != nil {
		fmt.Printf("write file in reduce failed!\n")
		w.reportTask(task, false)
	}

	w.reportTask(task, true)
}
//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
// func CallExample() {

// 	// declare an argument structure.
// 	args := ExampleArgs{}

// 	// fill in the argument(s).
// 	args.X = 99

// 	// declare a reply structure.
// 	reply := ExampleReply{}

// 	// send the RPC request, wait for the reply.
// 	// the "Coordinator.Example" tells the
// 	// receiving server that we'd like to call
// 	// the Example() method of struct Coordinator.
// 	ok := call("Coordinator.Example", &args, &reply)
// 	if ok {
// 		// reply.Y should be 100.
// 		fmt.Printf("reply.Y %v\n", reply.Y)
// 	} else {
// 		fmt.Printf("call failed!\n")
// 	}
// }

func (w *worker) register() {
	args := &RegistArgs{}
	reply := &RegistReply{}

	ok := call("Coordinator.RegistWorker", args, reply)
	if ok {
		w.workerId = reply.WorkerId
	} else {
		fmt.Printf("call RegistWorker failed!\n")
	}
}

func (w *worker) getTask() (*Task, error) {
	args := TaskArgs{WorkerId: w.workerId}
	reply := TaskReply{}

	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		return reply.Task, nil
	} else {
		fmt.Printf("call GetTask failed!\n")
		return nil, errors.New("worker getTask error!")
	}

}

func (w *worker) reportTask(task Task, done bool) {
	args := ReportArgs{
		Phase: 		task.Phase,
		WorkerId: 	w.workerId,
		Seq: 		task.Seq,
		Done: 		done,
	}
	reply := ReportReply{}

	ok := call("Coordinator.ReportTask", &args, &reply)

	if !ok {
		fmt.Printf("call ReportTask failed!\n")
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
