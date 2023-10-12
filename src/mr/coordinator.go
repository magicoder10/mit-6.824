package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Phase string

const (
	MapPhase       = "Map"
	ReducePhase    = "Reduce"
	CompletedPhase = "Completed"
)

type TaskStatus string

const (
	IdleTaskStatus       = "Idle"
	InProgressTaskStatus = "InProgress"
	CompletedTaskStatus  = "Completed"
)

type MapTask struct {
	inputFilePath         string
	intermediateFilePaths []string
	status                TaskStatus
	startedAt             time.Time
}

func (m *MapTask) isTimedOut() bool {
	if m.status != InProgressTaskStatus {
		return false
	}

	timeoutAt := m.startedAt.Add(10 * time.Second)
	return !time.Now().Before(timeoutAt)
}

func (m *MapTask) markStarted() {
	m.startedAt = time.Now()
	m.status = InProgressTaskStatus
}

func (m *MapTask) markCompleted() {
	m.status = CompletedTaskStatus
}

type ReduceTask struct {
	intermediateFilePaths []string
	status                TaskStatus
	startedAt             time.Time
}

func (r *ReduceTask) markStarted() {
	r.startedAt = time.Now()
	r.status = InProgressTaskStatus
}

func (r *ReduceTask) markCompleted() {
	r.status = CompletedTaskStatus
}

func (r *ReduceTask) isTimedOut() bool {
	if r.status != InProgressTaskStatus {
		return false
	}

	timeoutAt := r.startedAt.Add(10 * time.Second)
	return !time.Now().Before(timeoutAt)
}

type Coordinator struct {
	nextTaskID            uint64
	phase                 Phase
	reducePartitions      int
	mapTasks              map[uint64]*MapTask
	reduceTasks           map[uint64]*ReduceTask
	remainMapTaskCount    int
	remainReduceTaskCount int
	mut                   sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) RequestTask(args *Empty, reply *RequestTaskReply) error {
	c.mut.Lock()
	defer c.mut.Unlock()
	for {
		switch c.phase {
		case MapPhase:
			for taskID, mapTask := range c.mapTasks {
				if mapTask.status == IdleTaskStatus || mapTask.isTimedOut() {
					log.Printf("Assign map task: %+v\n", mapTask)
					mapTask.markStarted()
					reply.TaskID = taskID
					reply.TaskType = MapTaskType
					reply.MapTask = MapTaskReply{
						InputFilePath:    mapTask.inputFilePath,
						ReducePartitions: c.reducePartitions,
					}
					return nil
				}
			}

			if c.remainMapTaskCount > 0 {
				return fmt.Errorf("waiting for map tasks to finish")
			}
		case ReducePhase:
			for taskID, reduceTask := range c.reduceTasks {
				if reduceTask.status == IdleTaskStatus || reduceTask.isTimedOut() {
					log.Printf("Assign reduce task: %+v\n", reduceTask)
					reduceTask.markStarted()
					reply.TaskID = taskID
					reply.TaskType = ReduceTaskType
					reply.ReduceTask = ReduceTaskReply{
						IntermediateFilePaths: reduceTask.intermediateFilePaths,
					}
					return nil
				}
			}

			if c.remainReduceTaskCount > 0 {
				return fmt.Errorf("waiting for reduce tasks to finish")
			}
		case CompletedPhase:
			reply.TaskType = ExitTaskType
			return nil
		default:
			return fmt.Errorf("unknown phase: %v", c.phase)
		}
	}
}

func (c *Coordinator) ReportMapTaskComplete(args *ReportMapCompleteArgs, reply *Empty) error {
	c.mut.Lock()
	defer c.mut.Unlock()
	log.Printf("ReportMapTaskComplete args=%+v\n", args)

	if c.phase != MapPhase {
		return fmt.Errorf("not in map phase: phase=%v", c.phase)
	}

	if c.mapTasks[args.TaskID].status == IdleTaskStatus {
		return fmt.Errorf("map task is not started, skipping: taskID=%v", args.TaskID)
	}

	if c.mapTasks[args.TaskID].status == CompletedTaskStatus {
		return fmt.Errorf("map task is already completed, skipping: taskID=%v", args.TaskID)
	}

	c.mapTasks[args.TaskID].status = CompletedTaskStatus
	c.mapTasks[args.TaskID].intermediateFilePaths = args.IntermediateFilePaths
	c.remainMapTaskCount--

	if c.remainMapTaskCount == 0 {
		c.phase = ReducePhase

		for partitionIndex := 0; partitionIndex < c.reducePartitions; partitionIndex++ {
			reduceTask := c.newReduceTask(partitionIndex)
			c.reduceTasks[c.nextTaskID] = &reduceTask
			c.nextTaskID++
		}

		c.remainReduceTaskCount = len(c.reduceTasks)
	}

	return nil
}

func (c *Coordinator) ReportReduceTaskComplete(args *ReportReduceCompleteArgs, reply *Empty) error {
	c.mut.Lock()
	defer c.mut.Unlock()
	log.Printf("ReportReduceTaskComplete args=%+v\n", args)

	if c.phase != ReducePhase {
		return fmt.Errorf("not in reduce phase: phase=%v", c.phase)
	}

	if c.reduceTasks[args.TaskID].status == IdleTaskStatus {
		return fmt.Errorf("reduce task is not started, skipping: taskID=%v", args.TaskID)
	}

	if c.reduceTasks[args.TaskID].status == CompletedTaskStatus {
		return fmt.Errorf("reduce task is already completed, skipping: taskID=%v", args.TaskID)
	}

	c.reduceTasks[args.TaskID].status = CompletedTaskStatus
	c.remainReduceTaskCount--

	if c.remainReduceTaskCount == 0 {
		c.phase = CompletedPhase
	}

	return nil
}

func (c *Coordinator) newReduceTask(partitionIndex int) ReduceTask {
	intermediateFilePaths := make([]string, 0)
	for _, mapTask := range c.mapTasks {
		intermediateFilePaths = append(intermediateFilePaths, mapTask.intermediateFilePaths[partitionIndex])
	}

	return ReduceTask{
		intermediateFilePaths: intermediateFilePaths,
		status:                IdleTaskStatus,
	}
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mut.Lock()
	defer c.mut.Unlock()
	return c.phase == CompletedPhase
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	var nextTaskID uint64 = 1
	mapTasks := make(map[uint64]*MapTask, 0)
	for _, file := range files {
		mapTask := &MapTask{
			inputFilePath: file,
			status:        IdleTaskStatus,
		}
		mapTasks[nextTaskID] = mapTask
		nextTaskID++
	}

	c := Coordinator{
		nextTaskID:         0,
		phase:              MapPhase,
		reducePartitions:   nReduce,
		mapTasks:           mapTasks,
		reduceTasks:        map[uint64]*ReduceTask{},
		remainMapTaskCount: len(mapTasks),
	}
	c.server()
	return &c
}
