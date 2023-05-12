package main

//
// simple sequential MapReduce.
//
// go run mrsequential.go wc.so pg*.txt
//

import "fmt"
import "6.824-golabs-2020/src/mr"
import "plugin"
import "os"
import "log"
import "io/ioutil"
import "sort"

// ByKey for sorting by key.
// 用于将中间集进行排序，方便reduce的入参
type ByKey []mr.KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func main() {

	// 运行时的参数个数若少于3则报错并退出程序
	// 在go run mrsequential.go ../mrapps/wc.so pg*.txt中
	// 参数0：mrsequential.go
	// 参数1：wc.so
	// 参数2：pg*.txt（这里其实输入文件算是多个参数）
	if len(os.Args) < 3 {
		fmt.Fprintf(os.Stderr, "Usage: mrsequential xxx.so inputfiles...\n")
		os.Exit(1)
	}
	// --------------------------map任务开始--------------------------
	// 从参数1（wc.so）中读取map程序和reduce程序
	mapf, reducef := loadPlugin(os.Args[1])

	//
	// read each input file,
	// pass it to Map,
	// accumulate the intermediate Map output.
	// 读取每一个文件作为map程序的入参，并输出中间结果
	intermediate := []mr.KeyValue{}
	for _, filename := range os.Args[2:] {
		//打开文件
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		//读取文件内容
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}

		file.Close()
		// 输入map程序
		kva := mapf(filename, string(content))
		// 中间结果
		intermediate = append(intermediate, kva...)
	}
	// --------------------------map任务结束--------------------------
	sort.Sort(ByKey(intermediate))

	// 创建输出文件mr-out-0
	oname := "mr-out-0"
	ofile, _ := os.Create(oname)

	// --------------------------reduce任务开始--------------------------
	// 由于中间结果集是有序的，所以相同的key/value对会连续放置在一起
	// 只需要将key相同的中间结果集作为reduce程序的输入即可
	i := 0
	for i < len(intermediate) {
		// i表示key相同的单词的第一个的位置
		// j表示key相同的单词的最后一个的后一位
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		// 遍历从i到j之间的key/value对，全部都是同样的key并且value都为1
		// 并作为reduce的入参
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	// --------------------------reduce任务结束--------------------------
	ofile.Close()
}

// 加载插件中的方法
// 入参插件文件名
// 返回值为map方法和reduce方法

func loadPlugin(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []mr.KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}
