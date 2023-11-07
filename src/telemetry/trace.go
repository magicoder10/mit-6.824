package telemetry

import (
	"fmt"
)

type Trace struct {
	ServerID int
	TraceID  uint64
}

var _ fmt.Stringer = (*Trace)(nil)

func (t Trace) String() string {
	return fmt.Sprintf("%v/%v", t.ServerID, t.TraceID)
}

type WithTrace[Value any] struct {
	Trace Trace
	Value Value
}

var _ fmt.Stringer = (*WithTrace[int])(nil)

func (w WithTrace[Value]) String() string {
	return fmt.Sprintf("[trace:(%v), value:%v]", w.Trace, w.Value)
}
