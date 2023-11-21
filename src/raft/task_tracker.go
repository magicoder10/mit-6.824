package raft

import (
	"fmt"
	"path/filepath"
	"runtime"
	"sync"
)

type Finisher struct {
	taskKey     string
	taskTracker *TaskTracker
}

func (f Finisher) Done(logContext LogContext) {
	f.taskTracker.done(logContext, f.taskKey, 1)
}

type TaskTracker struct {
	runningTasks map[string]int
	mu           sync.Mutex
}

func (t *TaskTracker) Add(logContext LogContext, delta int) Finisher {
	t.mu.Lock()
	defer t.mu.Unlock()

	taskKey := newTaskKey(1)
	t.runningTasks[taskKey] += delta
	LogAndSkipCallers(logContext, InfoLevel, 1, "TaskTracker:Add(delta=%v, task=%v, runningTasks=%v)", delta, taskKey, t.runningTasks)
	return Finisher{
		taskKey:     taskKey,
		taskTracker: t,
	}
}

func (t *TaskTracker) done(logContext LogContext, taskKey string, skipCallers int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.runningTasks[taskKey]--
	LogAndSkipCallers(logContext, InfoLevel, skipCallers+1, "TaskTracker:Done(task=%v, runningTasks=%v)", taskKey, t.runningTasks)
}

func newTaskTracker() *TaskTracker {
	return &TaskTracker{
		runningTasks: make(map[string]int),
	}
}

func newTaskKey(skipCallers int) string {
	_, filePath, line, _ := runtime.Caller(skipCallers + 1)
	_, fileName := filepath.Split(filePath)
	return fmt.Sprintf("%v:%v", fileName, line)
}
