package main

type Stats struct {
	MaxGoroutines int
	MaxTaskCount  map[string]int
}

type Goroutines struct {
	Count int
}

type RunningTasks struct {
	TaskCount map[string]int
}

type TaskCount struct {
	TaskName string
	Count    int
}
