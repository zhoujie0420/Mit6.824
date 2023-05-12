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
	"strings"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

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

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for { //无限循环
		args := GetTaskRequest{0}
		rep := GetTaskResponse{}
		call("Master.GetTask", &args, &rep)

		if rep.TaskType == Map { //Map任务，处理
			filenames := HandleMap(mapf, rep.MFileName, rep.ReduceNumber, rep.TaskName)
			rargs := ReportStatusRequest{filenames, rep.TaskName}
			rreply := ReportStatusResponse{0}
			//上报任务完成
			call("Master.Report", &rargs, &rreply)
		} else if rep.TaskType == Reduce { //Reduce任务，处理
			HandleReduce(reducef, rep.RFileName)
			rargs := ReportStatusRequest{make([]string, 0), rep.TaskName}
			rreply := ReportStatusResponse{0}
			call("Master.Report", &rargs, &rreply)
		} else if rep.TaskType == Sleep { // 没有任务，休眠
			time.Sleep(time.Millisecond * 10)
		} else {
			log.Fatal("get task is not map sleep and reduce")
		}
	}
}

func HandleMap(mapf func(string, string) []KeyValue, filename string, filenum int, tasknum string) []string {
	intermediate := []KeyValue{}
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal("cannot open %v", filename)
	}
	context, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatal("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(context)) //调用用户设置的map函数
	intermediate = append(intermediate, kva...)
	filenames := make([]string, filenum)
	files := make([]*os.File, filenum)

	for i := 0; i < filenum; i++ {
		oname := "mr"
		oname = oname + "_" + tasknum + "_" + strconv.Itoa(i)
		ofile, _ := os.Create(oname)
		files[i] = ofile
		filenames[i] = oname
	}

	for _, kv := range intermediate {
		index := ihash(kv.Key) % filenum
		enc := json.NewEncoder(files[index])
		enc.Encode(&kv)
	}
	return filenames
}

func HandleReduce(reduce func(string, []string) string, filenames []string) string {
	files := make([]*os.File, len(filenames))
	intermediate := []KeyValue{}
	for i := 0; i < len(filenames); i++ {
		files[i], _ = os.Open(filenames[i])
		kv := KeyValue{}
		dec := json.NewDecoder(files[i])
		for {
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	sort.Sort(ByKey(intermediate)) //将读到的所有键值对排序
	oname := "mr-out-"

	index := filenames[0][strings.LastIndex(filenames[0], "_")+1:]
	oname = oname + index
	ofile, _ := os.Create(oname)

	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
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
		output := reduce(intermediate[i].Key, values)
		fmt.Fprint(ofile, "%v %v/n", intermediate[i].Key, output)

		i = j
	}
	return oname

}

// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
