package raft

import (
	"fmt"
	"path/filepath"
	"runtime"
	"sync"

	"6.5840/telemetry"
)

type Finisher struct {
	taskKey       string
	cancelContext *CancelContext
}

func (f Finisher) Done(logContext LogContext) {
	f.cancelContext.done(logContext, f.taskKey, 1)
}

type CancelContext struct {
	trace        telemetry.Trace
	isCanceled   bool
	cancelCh     chan struct{}
	waitGroup    *sync.WaitGroup
	runningTasks map[string]int
	mu           sync.Mutex
}

func (cc *CancelContext) TryCancel(logContext LogContext) {
	LogAndSkipCallers(logContext, InfoLevel, 1, "CancelContext[trace:(%v)]:TryCancel", cc.trace)
	cc.isCanceled = true
	close(cc.cancelCh)
}

func (cc *CancelContext) IsCanceled(logContext LogContext) bool {
	LogAndSkipCallers(logContext, InfoLevel, 1, "CancelContext[trace:(%v)]:IsCanceled", cc.trace)
	return cc.isCanceled
}

func (cc *CancelContext) OnCancel(logContext LogContext) chan struct{} {
	LogAndSkipCallers(logContext, InfoLevel, 1, "CancelContext[trace:(%v)]:OnCancel", cc.trace)
	return cc.cancelCh
}

func (cc *CancelContext) Add(logContext LogContext, delta int) Finisher {
	cc.waitGroup.Add(delta)
	cc.mu.Lock()
	defer cc.mu.Unlock()

	taskKey := newTaskKey(1)
	cc.runningTasks[taskKey] += delta
	LogAndSkipCallers(logContext, InfoLevel, 1, "CancelContext[trace:(%v)]:Add(delta=%v, task=%v)", cc.trace, delta, taskKey)
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
	LogAndSkipCallers(logContext, InfoLevel, skipCallers+1, "CancelContext[trace:(%v)]:Done(task=%v, runningTasks=%v)", cc.trace, taskKey, cc.runningTasks)
}

func (cc *CancelContext) Wait(logContext LogContext) {
	cc.mu.Lock()
	LogAndSkipCallers(logContext, InfoLevel, 1, "CancelContext[trace:(%v)]:Wait(runningTasks=%v)", cc.trace, cc.runningTasks)
	cc.mu.Unlock()
	cc.waitGroup.Wait()
}

func newTaskKey(skipCallers int) string {
	_, filePath, line, _ := runtime.Caller(skipCallers + 1)
	_, fileName := filepath.Split(filePath)
	return fmt.Sprintf("%v:%v", fileName, line)
}

func newCancelContext(trace telemetry.Trace) *CancelContext {
	return &CancelContext{
		trace:        trace,
		isCanceled:   false,
		cancelCh:     make(chan struct{}),
		waitGroup:    &sync.WaitGroup{},
		runningTasks: make(map[string]int),
	}
}

type WithCancel[Value any] struct {
	CancelContext *CancelContext
	Value         Value
}
