package raft

import (
	"fmt"
	"path/filepath"
	"runtime"
	"sync"
)

type Finisher struct {
	taskKey       string
	cancelContext *CancelContext
}

func (f Finisher) Done(logContext LogContext) {
	f.cancelContext.done(logContext, f.taskKey, 1)
}

type CancelContext struct {
	id           uint64
	name         string
	isCanceled   bool
	cancelCh     chan struct{}
	waitGroup    *sync.WaitGroup
	runningTasks map[string]int
	mu           sync.Mutex
}

func (cc *CancelContext) TryCancel(logContext LogContext) {
	LogAndSkipCallers(logContext, InfoLevel, 1, "%v:TryCancel(id=%v)", cc.name, cc.id)
	cc.isCanceled = true
	close(cc.cancelCh)
}

func (cc *CancelContext) IsCanceled(logContext LogContext) bool {
	LogAndSkipCallers(logContext, InfoLevel, 1, "%v:IsCanceled(id=%v)", cc.name, cc.id)
	return cc.isCanceled
}

func (cc *CancelContext) OnCancel(logContext LogContext) chan struct{} {
	LogAndSkipCallers(logContext, InfoLevel, 1, "%v:OnCancel(id=%v)", cc.name, cc.id)
	return cc.cancelCh
}

func (cc *CancelContext) Add(logContext LogContext, delta int) Finisher {
	cc.waitGroup.Add(delta)
	cc.mu.Lock()
	defer cc.mu.Unlock()

	taskKey := newTaskKey(1)
	cc.runningTasks[taskKey] += delta
	LogAndSkipCallers(logContext, InfoLevel, 1, "%v:Add(id=%v, delta=%v, task=%v)", cc.name, cc.id, delta, taskKey)
	return Finisher{
		taskKey:       taskKey,
		cancelContext: cc,
	}
}

func (cc *CancelContext) done(logContext LogContext, taskKey string, skipCallers int) {
	cc.waitGroup.Done()
	cc.mu.Lock()
	defer cc.mu.Unlock()
	cc.runningTasks[taskKey]--
	LogAndSkipCallers(logContext, InfoLevel, skipCallers+1, "%v:Done(id=%v, task=%v, runningTasks=%v)", cc.name, cc.id, taskKey, cc.runningTasks)
}

func (cc *CancelContext) Wait(logContext LogContext) {
	cc.mu.Lock()
	LogAndSkipCallers(logContext, InfoLevel, 1, "%v:Wait(id=%v, runningTasks=%v)", cc.name, cc.id, cc.runningTasks)
	cc.mu.Unlock()
	cc.waitGroup.Wait()
}

func newTaskKey(skipCallers int) string {
	_, filePath, line, _ := runtime.Caller(skipCallers + 1)
	_, fileName := filepath.Split(filePath)
	return fmt.Sprintf("%v:%v", fileName, line)
}

func newCancelContext(cancelContextID uint64, name string) *CancelContext {
	return &CancelContext{
		id:           cancelContextID,
		name:         name,
		isCanceled:   false,
		cancelCh:     make(chan struct{}),
		waitGroup:    &sync.WaitGroup{},
		runningTasks: make(map[string]int),
	}
}
