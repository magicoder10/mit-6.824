package rpc

import (
	"fmt"
)

type Result[Reply any] struct {
	Succeed bool
	Reply   *Reply
}

var _ fmt.Stringer = (*Result[int])(nil)

func (r Result[Reply]) String() string {
	return fmt.Sprintf("[RPCResult succeed:%v, reply:%+v]", r.Succeed, *r.Reply)
}
