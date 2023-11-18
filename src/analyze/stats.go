package main

import (
	"encoding/json"
	"time"
)

type Stats struct {
	MaxGoroutines     int
	MaxTaskCount      map[string]int
	TotalLockDuration time.Duration
}

var _ json.Marshaler = (*Stats)(nil)

func (s Stats) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		MaxGoroutines     int
		MaxTaskCount      map[string]int
		TotalLockDuration string
	}{
		MaxGoroutines:     s.MaxGoroutines,
		MaxTaskCount:      s.MaxTaskCount,
		TotalLockDuration: s.TotalLockDuration.String(),
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
