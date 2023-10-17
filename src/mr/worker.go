package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

const tmpFileDir = "tmp-out"
const retryInterval = 50 * time.Millisecond

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type MapFunc = func(string, string) []KeyValue
type ReduceFunc = func(string, []string) string

// main/mrworker.go calls this function.
func Worker(
	mapFunc MapFunc,
	reduceFunc ReduceFunc,
) {
	os.Mkdir(tmpFileDir, os.ModePerm)

	for {
		requestTaskReply := RequestTaskReply{}
		succeed, err := callRequestTask(&requestTaskReply)
		if err != nil {
			log.Println("Shutdown worker")
			return
		}

		if succeed {
			switch requestTaskReply.TaskType {
			case MapTaskType:
				log.Printf("execute map task: taskID=%v mapTask=%+v\n", requestTaskReply.TaskID, requestTaskReply.MapTask)
				intermediateFilePaths, err := executeMapTask(
					requestTaskReply.TaskID,
					requestTaskReply.MapTask,
					mapFunc)
				if err != nil {
					log.Println(err)
					continue
				}

				succeed, err = callReportMapTaskComplete(&ReportMapCompleteArgs{
					TaskID:                requestTaskReply.TaskID,
					IntermediateFilePaths: intermediateFilePaths,
				})
				if err != nil {
					log.Println("Shutdown worker")
					return
				}

				if succeed {
					continue
				} else {
					log.Println("fail to report map task complete")
				}
			case ReduceTaskType:
				log.Printf("execute reduce task: taskID=%v reduce=%+v\n", requestTaskReply.TaskID, requestTaskReply.ReduceTask)
				err = executeReduceTask(
					requestTaskReply.TaskID,
					requestTaskReply.ReduceTask,
					reduceFunc)
				if err != nil {
					log.Println(err)
					continue
				}

				succeed, err = callReportReduceTaskComplete(&ReportReduceCompleteArgs{
					TaskID: requestTaskReply.TaskID,
				})
				if err != nil {
					log.Println("Shutdown worker")
					return
				}

				if succeed {
					continue
				} else {
					log.Println("fail to report reduce task complete")
				}
			case ExitTaskType:
				log.Println("Shutdown worker")
				return
			default:
				log.Printf("Unknown task type: %v\n", requestTaskReply.TaskType)
			}
		}

		log.Printf("Retry after %v\n", retryInterval)
		time.Sleep(retryInterval)
	}
}

func executeMapTask(
	taskID uint64,
	mapTaskReply MapTaskReply,
	mapFunc MapFunc,
) ([]string, error) {
	buf, err := os.ReadFile(mapTaskReply.InputFilePath)
	if err != nil {
		return nil, err
	}

	pairs := mapFunc(mapTaskReply.InputFilePath, string(buf))
	reducePartitionFiles := make([]*os.File, 0)
	reducePartitionEncoder := make([]*json.Encoder, 0)
	intermediateFilePaths := make([]string, 0)

	for reducePartitionIndex := 0; reducePartitionIndex < mapTaskReply.ReducePartitions; reducePartitionIndex++ {
		file, err := os.CreateTemp(tmpFileDir, intermediateFileName(taskID, reducePartitionIndex))
		if err != nil {
			closeFiles(reducePartitionFiles)
			return nil, err
		}

		reducePartitionFiles = append(reducePartitionFiles, file)
		reducePartitionEncoder = append(reducePartitionEncoder, json.NewEncoder(file))
		intermediateFilePaths = append(intermediateFilePaths, file.Name())
	}

	for _, pair := range pairs {
		reducePartitionIndex := ihash(pair.Key) % mapTaskReply.ReducePartitions
		err = reducePartitionEncoder[reducePartitionIndex].Encode(pair)
		if err != nil {
			closeFiles(reducePartitionFiles)
			return nil, err
		}
	}

	closeFiles(reducePartitionFiles)
	return intermediateFilePaths, nil
}

func intermediateFileName(mapTaskID uint64, reducePartitionIndex int) string {
	return fmt.Sprintf("intermediate-%d-%d.json", mapTaskID, reducePartitionIndex)
}

func executeReduceTask(
	taskID uint64,
	reduceTaskReply ReduceTaskReply,
	reduceFunc ReduceFunc,
) error {
	keyValueCh := fetchKeyValuePairsForReduceTask(reduceTaskReply.IntermediateFilePaths)
	keyValues := make([]KeyValue, 0)
	for kv := range keyValueCh {
		keyValues = append(keyValues, kv)
	}

	outputTmpFile, err := os.CreateTemp(tmpFileDir, fmt.Sprintf("mr-out-%d.txt", taskID))
	if err != nil {
		return err
	}

	defer outputTmpFile.Close()

	if len(keyValues) > 0 {
		sort.Slice(keyValues, func(i, j int) bool {
			return keyValues[i].Key < keyValues[j].Key
		})

		key := keyValues[0].Key
		values := []string{
			keyValues[0].Value,
		}

		for index := 1; index <= len(keyValues); index++ {
			if index < len(keyValues) && keyValues[index].Key == key {
				values = append(values, keyValues[index].Value)
				continue
			}

			output := reduceFunc(key, values)
			_, err = outputTmpFile.WriteString(fmt.Sprintf("%v %v\n", key, output))
			if err != nil {
				return err
			}

			if index < len(keyValues) {
				key = keyValues[index].Key
				values = []string{keyValues[index].Value}
			}
		}
	}

	outFileFile := filepath.Join(".", fmt.Sprintf("mr-out-%d.txt", taskID))
	err = os.Rename(outputTmpFile.Name(), outFileFile)
	if err != nil {
		return err
	}

	for _, filePath := range reduceTaskReply.IntermediateFilePaths {
		os.Remove(filePath)
	}

	return nil
}

func fetchKeyValuePairsForReduceTask(intermediateFilePaths []string) <-chan KeyValue {
	keyValueCh := make(chan KeyValue)
	go func() {
		var syncErr error
		once := sync.Once{}
		wg := sync.WaitGroup{}
		for _, filePath := range intermediateFilePaths {
			wg.Add(1)
			go func(filePath string) {
				defer wg.Done()
				file, err := os.Open(filePath)
				if err != nil {
					once.Do(func() {
						syncErr = err
					})
					return
				}

				defer file.Close()
				decoder := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := decoder.Decode(&kv); err != nil {
						if err == io.EOF {
							break
						} else {
							once.Do(func() {
								syncErr = err
							})
							return
						}
					}

					keyValueCh <- kv
				}
			}(filePath)
		}

		wg.Wait()
		close(keyValueCh)
		if syncErr != nil {
			log.Println(syncErr)
		}
	}()
	return keyValueCh
}

func callRequestTask(reply *RequestTaskReply) (bool, error) {
	return call("Coordinator.RequestTask", &Empty{}, reply)
}

func callReportMapTaskComplete(args *ReportMapCompleteArgs) (bool, error) {
	return call("Coordinator.ReportMapTaskComplete", args, &Empty{})
}

func callReportReduceTaskComplete(args *ReportReduceCompleteArgs) (bool, error) {
	return call("Coordinator.ReportReduceTaskComplete", args, &Empty{})
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) (bool, error) {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Println("dialing:", err)
		return false, err
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true, nil
	}

	log.Println(err)
	return false, nil
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func closeFiles(files []*os.File) {
	for _, file := range files {
		file.Close()
	}
}
