package main

import (
	"encoding/json"
)

type Stats struct {
	MaxGoroutines int
	MaxTaskCount  map[string]int
}

var _ json.Marshaler = (*Stats)(nil)

func (s Stats) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		MaxGoroutines int
		MaxTaskCount  map[string]int
	}{
		MaxGoroutines: s.MaxGoroutines,
		MaxTaskCount:  s.MaxTaskCount,
	})
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
