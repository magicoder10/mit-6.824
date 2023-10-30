package raft

import (
	"fmt"
)

type RPCResult[Reply any] struct {
	succeed bool
	reply   *Reply
}

var _ fmt.Stringer = (*RPCResult[int])(nil)

func (R RPCResult[Reply]) String() string {
	return fmt.Sprintf("[RPCResult succeed:%v, reply:%+v]", R.succeed, *R.reply)
}
