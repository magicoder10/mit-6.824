package shardctrler

//
// Shardctrler clerk.
//

import (
	"6.5840/labrpc"
	"6.5840/telemetry"
)
import "time"

const retryInterval = 50 * time.Millisecond

type Clerk struct {
	id               int
	servers          []*labrpc.ClientEnd
	orderedServerIDs []int
	nextTraceID      uint64
	nextOperationID  uint64
}

func (ck *Clerk) Query(num int) Config {
	trace := ck.newTrace()
	operationID := ck.nextOperationID
	ck.nextOperationID++
	Log(ck.logContext(trace, ExecuteOpFlow), InfoLevel, "Query(operationID=%v, num=%v)", operationID, num)

	for {
		for index, serverID := range ck.orderedServerIDs {
			loopTrace := ck.newTrace()
			Log(ck.logContext(trace, ExecuteOpFlow), InfoLevel, "[loop-trace:(%v)] start new Query loop", loopTrace)

			var reply QueryReply
			args := QueryArgs{
				OperationContext: OperationContext{
					Trace:       loopTrace,
					ClientID:    ck.id,
					OperationID: operationID,
				},
				Num: num,
			}
			mc := labrpc.MessageContext{
				Trace:      loopTrace,
				SenderID:   ck.id,
				ReceiverID: serverID,
			}

			Log(ck.logContext(loopTrace, ExecuteOpFlow), InfoLevel, "send Query(operationID=%v, num=%v)", operationID, num)
			rpcSucceed := ck.servers[serverID].Call("ShardCtrler.Query", mc, &args, &reply)
			if !rpcSucceed {
				Log(ck.logContext(loopTrace, ExecuteOpFlow), InfoLevel, "RPC failed, retrying")
				continue
			}

			if reply.Err == ErrCancelledErr {
				Log(ck.logContext(loopTrace, ExecuteOpFlow), InfoLevel, "request cancelled")
				continue
			}

			if reply.Err == ErrWrongLeaderErr {
				Log(ck.logContext(loopTrace, ExecuteOpFlow), InfoLevel, "wrong leader")
				continue
			}

			if reply.Err == OkErr {
				Log(
					ck.logContext(loopTrace, ExecuteOpFlow), InfoLevel, "request succeed: Query(operationID=%v, num=%v) => %+v",
					operationID,
					num,
					reply.Config)
				swap(ck.orderedServerIDs, 0, index)
				return reply.Config
			}
		}

		Log(ck.logContext(trace, ExecuteOpFlow), InfoLevel, "no leader found, retry after %v", retryInterval)
		<-time.After(retryInterval)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	trace := ck.newTrace()
	operationID := ck.nextOperationID
	ck.nextOperationID++
	Log(ck.logContext(trace, ExecuteOpFlow), InfoLevel, "Join(operationID=%v, servers=%v)", operationID, servers)

	for {
		for index, serverID := range ck.orderedServerIDs {
			loopTrace := ck.newTrace()
			Log(ck.logContext(trace, ExecuteOpFlow), InfoLevel, "[loop-trace:(%v)] start new Join loop", loopTrace)

			var reply JoinReply
			args := JoinArgs{
				OperationContext: OperationContext{
					Trace:       loopTrace,
					ClientID:    ck.id,
					OperationID: operationID,
				},
				Servers: servers,
			}
			mc := labrpc.MessageContext{
				Trace:      loopTrace,
				SenderID:   ck.id,
				ReceiverID: serverID,
			}

			Log(ck.logContext(loopTrace, ExecuteOpFlow), InfoLevel, "send Join(operationID=%v, servers=%v)", operationID, servers)
			rpcSucceed := ck.servers[serverID].Call("ShardCtrler.Join", mc, &args, &reply)
			if !rpcSucceed {
				Log(ck.logContext(loopTrace, ExecuteOpFlow), InfoLevel, "RPC failed, retrying")
				continue
			}

			if reply.Err == ErrCancelledErr {
				Log(ck.logContext(loopTrace, ExecuteOpFlow), InfoLevel, "request cancelled")
				continue
			}

			if reply.Err == ErrWrongLeaderErr {
				Log(ck.logContext(loopTrace, ExecuteOpFlow), InfoLevel, "wrong leader")
				continue
			}

			if reply.Err == OkErr {
				Log(
					ck.logContext(loopTrace, ExecuteOpFlow), InfoLevel, "request succeed: Join(operationID=%v, servers=%v)",
					operationID,
					servers)
				swap(ck.orderedServerIDs, 0, index)
				return
			}
		}

		Log(ck.logContext(trace, ExecuteOpFlow), InfoLevel, "no leader found, retry after %v", retryInterval)
		<-time.After(retryInterval)
	}
}

func (ck *Clerk) Leave(gids []int) {
	trace := ck.newTrace()
	operationID := ck.nextOperationID
	ck.nextOperationID++
	Log(ck.logContext(trace, ExecuteOpFlow), InfoLevel, "Leave(operationID=%v, gids=%v)", operationID, gids)

	for {
		for index, serverID := range ck.orderedServerIDs {
			loopTrace := ck.newTrace()
			Log(ck.logContext(trace, ExecuteOpFlow), InfoLevel, "[loop-trace:(%v)] start new Leave loop", loopTrace)

			var reply LeaveReply
			args := LeaveArgs{
				OperationContext: OperationContext{
					Trace:       loopTrace,
					ClientID:    ck.id,
					OperationID: operationID,
				},
				GIDs: gids,
			}
			mc := labrpc.MessageContext{
				Trace:      loopTrace,
				SenderID:   ck.id,
				ReceiverID: serverID,
			}

			Log(ck.logContext(loopTrace, ExecuteOpFlow), InfoLevel, "send Leave(operationID=%v, gids=%v)", operationID, gids)
			rpcSucceed := ck.servers[serverID].Call("ShardCtrler.Leave", mc, &args, &reply)
			if !rpcSucceed {
				Log(ck.logContext(loopTrace, ExecuteOpFlow), InfoLevel, "RPC failed, retrying")
				continue
			}

			if reply.Err == ErrCancelledErr {
				Log(ck.logContext(loopTrace, ExecuteOpFlow), InfoLevel, "request cancelled")
				continue
			}

			if reply.Err == ErrWrongLeaderErr {
				Log(ck.logContext(loopTrace, ExecuteOpFlow), InfoLevel, "wrong leader")
				continue
			}

			if reply.Err == OkErr {
				Log(
					ck.logContext(loopTrace, ExecuteOpFlow), InfoLevel, "request succeed: Leave(operationID=%v, gids=%v)",
					operationID,
					gids)
				swap(ck.orderedServerIDs, 0, index)
				return
			}
		}

		Log(ck.logContext(trace, ExecuteOpFlow), InfoLevel, "no leader found, retry after %v", retryInterval)
		<-time.After(retryInterval)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	trace := ck.newTrace()
	operationID := ck.nextOperationID
	ck.nextOperationID++
	Log(ck.logContext(trace, ExecuteOpFlow), InfoLevel, "Move(operationID=%v, shard=%v, gid=%v)", operationID, shard, gid)

	for {
		for index, serverID := range ck.orderedServerIDs {
			loopTrace := ck.newTrace()
			Log(ck.logContext(trace, ExecuteOpFlow), InfoLevel, "[loop-trace:(%v)] start new Move loop", loopTrace)

			var reply MoveReply
			args := MoveArgs{
				OperationContext: OperationContext{
					Trace:       loopTrace,
					ClientID:    ck.id,
					OperationID: operationID,
				},
				Shard: shard,
				GID:   gid,
			}
			mc := labrpc.MessageContext{
				Trace:      loopTrace,
				SenderID:   ck.id,
				ReceiverID: serverID,
			}

			Log(ck.logContext(loopTrace, ExecuteOpFlow), InfoLevel, "send Move(operationID=%v, shard=%v, gid=%v)", operationID, shard, gid)
			rpcSucceed := ck.servers[serverID].Call("ShardCtrler.Move", mc, &args, &reply)
			if !rpcSucceed {
				Log(ck.logContext(loopTrace, ExecuteOpFlow), InfoLevel, "RPC failed, retrying")
				continue
			}

			if reply.Err == ErrCancelledErr {
				Log(ck.logContext(loopTrace, ExecuteOpFlow), InfoLevel, "request cancelled")
				continue
			}

			if reply.Err == ErrWrongLeaderErr {
				Log(ck.logContext(loopTrace, ExecuteOpFlow), InfoLevel, "wrong leader")
				continue
			}

			if reply.Err == OkErr {
				Log(
					ck.logContext(loopTrace, ExecuteOpFlow), InfoLevel, "request succeed: Move(operationID=%v, shard=%v, gid=%v)",
					operationID,
					shard,
					gid)
				swap(ck.orderedServerIDs, 0, index)
				return
			}
		}

		Log(ck.logContext(trace, ExecuteOpFlow), InfoLevel, "no leader found, retry after %v", retryInterval)
		<-time.After(retryInterval)
	}
}

func (ck *Clerk) logContext(trace telemetry.Trace, flow Flow) LogContext {
	return LogContext{
		EndpointNamespace: clientNamespace,
		EndpointID:        ck.id,
		Flow:              flow,
		Trace:             &trace,
	}
}

func (ck *Clerk) newTrace() telemetry.Trace {
	nextTraceID := ck.nextTraceID
	ck.nextTraceID++
	trace := telemetry.Trace{
		Namespace:  clientNamespace,
		EndpointID: ck.id,
		TraceID:    nextTraceID,
	}
	return trace
}

func MakeClerk(clientID int, servers []*labrpc.ClientEnd) *Clerk {
	orderedServerIDs := make([]int, 0)
	for serverID := 0; serverID < len(servers); serverID++ {
		orderedServerIDs = append(orderedServerIDs, serverID)
	}

	return &Clerk{
		id:               clientID,
		servers:          servers,
		orderedServerIDs: orderedServerIDs,
		nextTraceID:      0,
		nextOperationID:  0,
	}
}

func swap[Item any](items []Item, index1 int, index2 int) {
	items[index1], items[index2] = items[index2], items[index1]
}
