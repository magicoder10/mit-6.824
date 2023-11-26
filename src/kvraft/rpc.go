package kvraft

import (
	"fmt"

	"6.5840/telemetry"
)

type Err string

const (
	OkErr             Err = "Ok"
	ErrNoKey          Err = "ErrNoKey"
	ErrWrongLeaderErr Err = "ErrWrongLeader"

	ErrCancelledErr Err = "ErrCancelled"
)

type OperationContext struct {
	Trace       telemetry.Trace
	ClientID    int
	OperationID uint64
}

type PutArgs struct {
	OperationContext OperationContext
	Key              string
	Value            string
}

var _ fmt.Stringer = (*PutArgs)(nil)

func (p PutArgs) String() string {
	return fmt.Sprintf("[PutArgs OperationContext:%+v Key:%v, Value:%v]", p.OperationContext, p.Key, p.Value)
}

type PutReply struct {
	Err Err
}

type AppendArgs struct {
	OperationContext OperationContext
	Key              string
	Arg              string
}

var _ fmt.Stringer = (*AppendArgs)(nil)

func (a AppendArgs) String() string {
	return fmt.Sprintf("[AppendArgs OperationContext:%+v Key:%v, Arg:%v]", a.OperationContext, a.Key, a.Arg)
}

type AppendReply struct {
	Err Err
}

type GetArgs struct {
	OperationContext OperationContext
	Key              string
}

var _ fmt.Stringer = (*GetArgs)(nil)

func (g GetArgs) String() string {
	return fmt.Sprintf("[GetArgs OperationContext:%+v Key:%v]", g.OperationContext, g.Key)
}

type GetReply struct {
	Err   Err
	Value string
}
