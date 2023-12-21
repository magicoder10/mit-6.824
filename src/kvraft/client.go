package kvraft

import (
	"time"

	"6.5840/labrpc"
	"6.5840/telemetry"
)

const retryInterval = 50 * time.Millisecond

type Clerk struct {
	id               int
	servers          []*labrpc.ClientEnd
	orderedServerIDs []int
	nextTraceID      uint64
	nextOperationID  uint64
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	trace := ck.newTrace()
	operationID := ck.nextOperationID
	ck.nextOperationID++
	Log(ck.logContext(trace, ExecuteOpFlow), InfoLevel, "Get(operationID=%v, key=%v)", operationID, key)

	for {
		for index, serverID := range ck.orderedServerIDs {
			loopTrace := ck.newTrace()
			Log(ck.logContext(trace, ExecuteOpFlow), InfoLevel, "[loop-trace:(%v)] start new Get loop", loopTrace)

			var reply GetReply
			args := GetArgs{
				OperationContext: OperationContext{
					Trace:       loopTrace,
					ClientID:    ck.id,
					OperationID: operationID,
				},
				Key: key,
			}
			mc := labrpc.MessageContext{
				Trace:      loopTrace,
				SenderID:   ck.id,
				ReceiverID: serverID,
			}
			Log(ck.logContext(loopTrace, ExecuteOpFlow), InfoLevel, "send Get(operationID=%v, key=%v)", operationID, key)
			rpcSucceed := ck.servers[serverID].Call("KVServer.Get", mc, &args, &reply)
			if !rpcSucceed {
				Log(ck.logContext(loopTrace, ExecuteOpFlow), InfoLevel, "RPC failed, retrying")
				continue
			}

			if reply.Err == ErrCancelledErr {
				Log(ck.logContext(loopTrace, ExecuteOpFlow), InfoLevel, "request cancelled")
				continue
			}

			if reply.Err == ErrWrongLeaderErr {
				Log(ck.logContext(loopTrace, ExecuteOpFlow), InfoLevel, "wrong leader %v", serverID)
				continue
			}

			if reply.Err == OkErr {
				Log(ck.logContext(loopTrace, ExecuteOpFlow), InfoLevel, "request succeed: Get(operationID=%v, key=%v) => %v",
					operationID,
					key,
					reply.Value)
				swap(ck.orderedServerIDs, 0, index)
				return reply.Value
			}
		}

		Log(ck.logContext(trace, ExecuteOpFlow), InfoLevel, "no leader found, retry after %v", retryInterval)
		<-time.After(retryInterval)
	}
}

// // you can send an RPC with code like this:
// // ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
// //
// // the types of args and reply (including whether they are pointers)
// // must match the declared types of the RPC handler function's
// // arguments. and reply must be passed as a pointer.
func (ck *Clerk) Put(key string, value string) {
	trace := ck.newTrace()
	operationID := ck.nextOperationID
	ck.nextOperationID++
	Log(ck.logContext(trace, ExecuteOpFlow), InfoLevel, "Put(operationID=%v, key=%v, value=%v)", operationID, key, value)

	for {
		for index, serverID := range ck.orderedServerIDs {
			loopTrace := ck.newTrace()
			Log(ck.logContext(trace, ExecuteOpFlow), InfoLevel, "[loop-trace:(%v)] start new Put loop", loopTrace)

			var reply PutReply
			args := PutArgs{
				OperationContext: OperationContext{
					Trace:       loopTrace,
					ClientID:    ck.id,
					OperationID: operationID,
				},
				Key:   key,
				Value: value,
			}
			mc := labrpc.MessageContext{
				Trace:      loopTrace,
				SenderID:   ck.id,
				ReceiverID: serverID,
			}

			Log(ck.logContext(loopTrace, ExecuteOpFlow), InfoLevel, "send Put(operationID=%v, key=%v, value=%v)", operationID, key, value)
			rpcSucceed := ck.servers[serverID].Call("KVServer.Put", mc, &args, &reply)
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
					ck.logContext(loopTrace, ExecuteOpFlow), InfoLevel, "request succeed: Put(operationID=%v, key=%v, value=%v)",
					operationID,
					key,
					value)
				swap(ck.orderedServerIDs, 0, index)
				return
			}
		}

		Log(ck.logContext(trace, ExecuteOpFlow), InfoLevel, "no leader found, retry after %v", retryInterval)
		<-time.After(retryInterval)
	}
}

func (ck *Clerk) Append(key string, arg string) {
	trace := ck.newTrace()
	operationID := ck.nextOperationID
	ck.nextOperationID++
	Log(ck.logContext(trace, ExecuteOpFlow), InfoLevel, "Append(operationID=%v, key=%v, arg=%v)", operationID, key, arg)

	for {
		for index, serverID := range ck.orderedServerIDs {
			loopTrace := ck.newTrace()
			Log(ck.logContext(trace, ExecuteOpFlow), InfoLevel, "[loop-trace:(%v)] start new Append loop", loopTrace)

			var reply AppendReply
			args := AppendArgs{
				OperationContext: OperationContext{
					Trace:       loopTrace,
					ClientID:    ck.id,
					OperationID: operationID,
				},
				Key: key,
				Arg: arg,
			}
			mc := labrpc.MessageContext{
				Trace:      loopTrace,
				SenderID:   ck.id,
				ReceiverID: serverID,
			}
			Log(ck.logContext(loopTrace, ExecuteOpFlow), InfoLevel, "send Append(operationID=%v, key=%v, arg=%v)", operationID, key, arg)
			rpcSucceed := ck.servers[serverID].Call("KVServer.Append", mc, &args, &reply)
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
				Log(ck.logContext(loopTrace, ExecuteOpFlow), InfoLevel, "request succeed: Append(operationID=%v, key=%v, arg=%v)",
					operationID,
					key,
					arg)
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
