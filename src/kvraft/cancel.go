package kvraft

import (
	"sync"

	"6.5840/telemetry"
)

type CancelContext struct {
	trace      telemetry.Trace
	isCanceled bool
	cancelCh   chan struct{}
	mu         sync.Mutex
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

func newCancelContext(trace telemetry.Trace) *CancelContext {
	return &CancelContext{
		trace:      trace,
		isCanceled: false,
		cancelCh:   make(chan struct{}),
	}
}

type WithCancel[Value any] struct {
	CancelContext *CancelContext
	Value         Value
}
