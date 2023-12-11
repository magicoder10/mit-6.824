package kvraft

import (
	"encoding/json"
	"sync"
	"time"

	"6.5840/labgob"
	//"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/telemetry"
)

const monitorRaftStateInterval = 50 * time.Millisecond

type OpType string

const (
	GetOpType    OpType = "Get"
	PutOpType    OpType = "Put"
	AppendOpType OpType = "Append"
)

type GetOp struct {
	Key string
}

type PutOp struct {
	Key   string
	Value string
}

type AppendOp struct {
	Key string
	Arg string
}

type Command struct {
	LeaderID    int
	ClientID    int
	OperationID uint64
	OpType      OpType
	GetOp       GetOp
	PutOp       PutOp
	AppendOp    AppendOp
}

type Operations[Result any] struct {
	AppliedOperationID uint64
	Result             Result
}

type GetWaiterResult struct {
	Value string
	Err   Err
}

type Operation struct {
	ClientID    int
	OperationID uint64
}

type Snapshot struct {
	KeyValueMap      map[string]string
	GetOperations    map[int]Operations[string]
	PutOperations    map[int]Operations[any]
	AppendOperations map[int]Operations[any]
}

type KVServer struct {
	mu        sync.Mutex
	serverID  int
	rf        *raft.Raft
	persister *raft.Persister
	applyCh   chan raft.ApplyMsg

	lastKnownRaftCommitIndex int
	lastKnownLeaderTerm      int
	maxRaftState             int // snapshot if log grows this big

	keyValueMap map[string]string

	getWaiters    map[Operation]telemetry.WithTrace[chan GetWaiterResult]
	putWaiters    map[Operation]telemetry.WithTrace[chan Err]
	appendWaiters map[Operation]telemetry.WithTrace[chan Err]

	getOperations    map[int]Operations[string]
	putOperations    map[int]Operations[any]
	appendOperations map[int]Operations[any]

	serverCancelContext *CancelContext
	serverWaitGroup     *sync.WaitGroup
	taskTracker         *TaskTracker

	nextLockID  uint64
	nextTraceID uint64
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	unlocker := kv.lock(ExecuteOpFlow)
	Log(kv.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "Get enter: args=%+v", args)
	operations, ok := kv.getOperations[args.OperationContext.ClientID]
	if ok {
		if args.OperationContext.OperationID <= operations.AppliedOperationID {
			Log(kv.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "skip duplicated operation, Get exit")
			reply.Value = operations.Result
			reply.Err = OkErr
			unlocker.unlock(ExecuteOpFlow)
			return
		}
	}

	if kv.serverCancelContext.IsCanceled(kv.logContext(args.OperationContext.Trace, ExecuteOpFlow)) {
		Log(kv.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "server canceled, Get exit")
		reply.Err = ErrCancelledErr
		unlocker.unlock(ExecuteOpFlow)
		return
	}

	asyncWaiter := make(chan GetWaiterResult)
	operation := Operation{
		ClientID:    args.OperationContext.ClientID,
		OperationID: args.OperationContext.OperationID,
	}
	kv.getWaiters[operation] = telemetry.WithTrace[chan GetWaiterResult]{
		Trace: args.OperationContext.Trace,
		Value: asyncWaiter,
	}

	command := Command{
		LeaderID:    kv.serverID,
		ClientID:    args.OperationContext.ClientID,
		OperationID: args.OperationContext.OperationID,
		OpType:      GetOpType,
		GetOp: GetOp{
			Key: args.Key,
		},
	}

	_, term, isLeader := kv.rf.Start(telemetry.WithTrace[Command]{
		Trace: args.OperationContext.Trace,
		Value: command,
	})
	Log(kv.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "wait for async waiter")
	if !isLeader {
		Log(kv.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "not leader, close and remove waiter")
		delete(kv.getWaiters, operation)
		close(asyncWaiter)
		reply.Err = ErrWrongLeaderErr
		Log(kv.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "not leader, Get exit: term=%v, reply=%+v", term, reply)
		unlocker.unlock(ExecuteOpFlow)
		return
	}

	Log(kv.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "Accept Get at term %v", term)
	kv.lastKnownLeaderTerm = term
	logContext := kv.logContext(args.OperationContext.Trace, ExecuteOpFlow)
	unlocker.unlock(ExecuteOpFlow)

	select {
	case result, ok := <-asyncWaiter:
		unlocker = kv.lock(ExecuteOpFlow)
		defer unlocker.unlock(ExecuteOpFlow)
		if !ok {
			Log(kv.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "async waiter closed, Get exit")
			return
		}

		Log(kv.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "async waiter finished")

		if result.Err == ErrWrongLeaderErr {
			Log(kv.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "not leader, Get exit")
			reply.Err = ErrWrongLeaderErr
			return
		}

		Log(kv.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "Get done")
		reply.Err = OkErr
		reply.Value = result.Value

		Log(kv.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "Get exit: reply=%+v", reply)
	case <-kv.serverCancelContext.OnCancel(logContext):
		unlocker = kv.lock(ExecuteOpFlow)
		defer unlocker.unlock(ExecuteOpFlow)
		reply.Err = ErrCancelledErr
		Log(kv.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "Get canceled, exit: reply=%+v", reply)
	}
}

func (kv *KVServer) Put(args *PutArgs, reply *PutReply) {
	unlocker := kv.lock(ExecuteOpFlow)
	Log(kv.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "Put enter: args=%+v", args)
	operations, ok := kv.putOperations[args.OperationContext.ClientID]
	if ok {
		if args.OperationContext.OperationID <= operations.AppliedOperationID {
			Log(kv.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "skip duplicated operation, Put exit")
			reply.Err = OkErr
			unlocker.unlock(ExecuteOpFlow)
			return
		}
	}

	if kv.serverCancelContext.IsCanceled(kv.logContext(args.OperationContext.Trace, ExecuteOpFlow)) {
		Log(kv.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "server canceled, Put exit")
		reply.Err = ErrCancelledErr
		unlocker.unlock(ExecuteOpFlow)
		return
	}

	asyncWaiter := make(chan Err)
	operation := Operation{
		ClientID:    args.OperationContext.ClientID,
		OperationID: args.OperationContext.OperationID,
	}
	kv.putWaiters[operation] = telemetry.WithTrace[chan Err]{
		Trace: args.OperationContext.Trace,
		Value: asyncWaiter,
	}

	command := Command{
		LeaderID:    kv.serverID,
		ClientID:    args.OperationContext.ClientID,
		OperationID: args.OperationContext.OperationID,
		OpType:      PutOpType,
		PutOp: PutOp{
			Key:   args.Key,
			Value: args.Value,
		},
	}

	_, term, isLeader := kv.rf.Start(telemetry.WithTrace[Command]{
		Trace: args.OperationContext.Trace,
		Value: command,
	})
	Log(kv.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "wait for async waiter")
	if !isLeader {
		Log(kv.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "not leader, close and remove waiter")
		delete(kv.putWaiters, operation)
		close(asyncWaiter)

		reply.Err = ErrWrongLeaderErr
		Log(kv.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "not leader, Put exit: term=%v, reply=%+v", term, reply)
		unlocker.unlock(ExecuteOpFlow)
		return
	}

	Log(kv.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "accept Put at term %v", term)
	kv.lastKnownLeaderTerm = term
	logContext := kv.logContext(args.OperationContext.Trace, ExecuteOpFlow)
	unlocker.unlock(ExecuteOpFlow)

	select {
	case err, ok := <-asyncWaiter:
		unlocker = kv.lock(ExecuteOpFlow)
		defer unlocker.unlock(ExecuteOpFlow)
		if !ok {
			Log(kv.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "async waiter closed, Put exit")
			return
		}

		Log(kv.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "async waiter finished")
		if err == ErrWrongLeaderErr {
			Log(kv.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "not leader, Put exit")
			reply.Err = ErrWrongLeaderErr
			return
		}

		Log(kv.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "Put done")
		reply.Err = OkErr
		Log(kv.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "Put exit: reply=%+v", reply)
	case <-kv.serverCancelContext.OnCancel(logContext):
		unlocker = kv.lock(ExecuteOpFlow)
		defer unlocker.unlock(ExecuteOpFlow)
		reply.Err = ErrCancelledErr
		Log(kv.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "Put canceled, exit: reply=%+v", reply)
	}
}

func (kv *KVServer) Append(args *AppendArgs, reply *AppendReply) {
	unlocker := kv.lock(ExecuteOpFlow)
	Log(kv.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "Append enter: args=%+v", args)
	operations, ok := kv.appendOperations[args.OperationContext.ClientID]
	if ok {
		if args.OperationContext.OperationID <= operations.AppliedOperationID {
			Log(kv.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "skip duplicated operation, Append exit")
			reply.Err = OkErr
			unlocker.unlock(ExecuteOpFlow)
			return
		}
	}

	if kv.serverCancelContext.IsCanceled(kv.logContext(args.OperationContext.Trace, ExecuteOpFlow)) {
		Log(kv.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "server canceled, Append exit")
		reply.Err = ErrCancelledErr
		unlocker.unlock(ExecuteOpFlow)
		return
	}

	asyncWaiter := make(chan Err)
	operation := Operation{
		ClientID:    args.OperationContext.ClientID,
		OperationID: args.OperationContext.OperationID,
	}
	kv.appendWaiters[operation] = telemetry.WithTrace[chan Err]{
		Trace: args.OperationContext.Trace,
		Value: asyncWaiter,
	}

	command := Command{
		LeaderID:    kv.serverID,
		ClientID:    args.OperationContext.ClientID,
		OperationID: args.OperationContext.OperationID,
		OpType:      AppendOpType,
		AppendOp: AppendOp{
			Key: args.Key,
			Arg: args.Arg,
		},
	}

	_, term, isLeader := kv.rf.Start(telemetry.WithTrace[Command]{
		Trace: args.OperationContext.Trace,
		Value: command,
	})
	Log(kv.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "wait for async waiter")

	if !isLeader {
		Log(kv.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "not leader, close and remove waiter")
		delete(kv.appendWaiters, operation)
		close(asyncWaiter)
		reply.Err = ErrWrongLeaderErr
		Log(kv.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "not leader, Append exit: term=%v, reply=%+v", term, reply)
		unlocker.unlock(ExecuteOpFlow)
		return
	}

	Log(kv.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "Accept Append at term %v: %+v", term, command)
	kv.lastKnownLeaderTerm = term
	logContext := kv.logContext(args.OperationContext.Trace, ExecuteOpFlow)
	unlocker.unlock(ExecuteOpFlow)

	select {
	case err, ok := <-asyncWaiter:
		unlocker = kv.lock(ExecuteOpFlow)
		defer unlocker.unlock(ExecuteOpFlow)
		if !ok {
			Log(kv.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "async waiter closed, Append exit")
			return
		}

		Log(kv.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "async waiter finished")

		if err == ErrWrongLeaderErr {
			Log(kv.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "not leader, Append exit")
			reply.Err = ErrWrongLeaderErr
			return
		}

		Log(kv.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "Append done")
		reply.Err = OkErr
		Log(kv.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "Append exit: reply=%+v", reply)
	case <-kv.serverCancelContext.OnCancel(logContext):
		unlocker = kv.lock(ExecuteOpFlow)
		defer unlocker.unlock(ExecuteOpFlow)
		reply.Err = ErrCancelledErr
		Log(kv.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "Append canceled, exit: reply=%+v", reply)
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	unlocker := kv.lock(TerminationFlow)
	trace := kv.newTrace()
	Log(kv.logContext(trace, TerminationFlow), InfoLevel, "Kill enter")
	defer Log(kv.logContext(trace, TerminationFlow), InfoLevel, "Kill exit")
	kv.serverCancelContext.TryCancel(kv.logContext(trace, TerminationFlow))
	logContext := kv.logContext(trace, TerminationFlow)
	unlocker.unlock(TerminationFlow)

	kv.rf.Kill()

	Log(logContext, InfoLevel, "wait for goroutines to finish")
	kv.serverWaitGroup.Wait()
	Log(logContext, InfoLevel, "all goroutines finished")

	unlocker = kv.lock(TerminationFlow)
	defer unlocker.unlock(TerminationFlow)

	Log(kv.logContext(trace, TerminationFlow), InfoLevel, "close applyCh")
	close(kv.applyCh)

	for _, waiterWithTrace := range kv.getWaiters {
		Log(kv.logContext(waiterWithTrace.Trace, RaftStateFlow), InfoLevel, "cancelled waiter")
		close(waiterWithTrace.Value)
	}

	for _, waiterWithTrace := range kv.putWaiters {
		Log(kv.logContext(waiterWithTrace.Trace, RaftStateFlow), InfoLevel, "cancelled waiter")
		close(waiterWithTrace.Value)
	}

	for _, waiterWithTrace := range kv.appendWaiters {
		Log(kv.logContext(waiterWithTrace.Trace, RaftStateFlow), InfoLevel, "cancelled waiter")
		close(waiterWithTrace.Value)
	}
}

func (kv *KVServer) applyCommandAndSnapshot(trace telemetry.Trace) {
	for {
		unlocker := kv.lock(ApplyFlow)
		loopTrace := kv.newTrace()
		logContext := kv.logContext(trace, ApplyFlow)
		Log(kv.logContext(trace, ApplyFlow), InfoLevel, "[loop-trace:(%v)] enter new applyCommandAndSnapshot loop", loopTrace)
		unlocker.unlock(ApplyFlow)
		var applyMsg raft.ApplyMsg
		var ok bool
		select {
		case applyMsg, ok = <-kv.applyCh:
			if !ok {
				logUnlocker := kv.lock(ApplyFlow)
				Log(kv.logContext(loopTrace, SnapshotFlow), InfoLevel, "applyCh closed, exit applyCommandAndSnapshot")
				logUnlocker.unlock(ApplyFlow)
				return
			}
		case <-kv.serverCancelContext.OnCancel(logContext):
			logUnlocker := kv.lock(ApplyFlow)
			Log(kv.logContext(loopTrace, ApplyFlow), InfoLevel, "canceled, exit applyCommandAndSnapshot")
			logUnlocker.unlock(ApplyFlow)
			return
		}

		unlocker = kv.lock(ApplyFlow)
		if kv.serverCancelContext.IsCanceled(kv.logContext(loopTrace, ApplyFlow)) {
			Log(kv.logContext(loopTrace, ApplyFlow), InfoLevel, "canceled, exit applyCommandAndSnapshot")
			unlocker.unlock(ApplyFlow)
			return
		}

		Log(kv.logContext(loopTrace, ApplyFlow), InfoLevel, "received applyMsg: %+v", applyMsg)

		logContext = kv.logContext(loopTrace, ApplyFlow)
		unlocker.unlock(ApplyFlow)

		if applyMsg.CommandValid {
			commandWithTrace := applyMsg.Command.(telemetry.WithTrace[Command])
			Log(logContext, InfoLevel, "[command-trace:(%v)] before tryApplyCommand", commandWithTrace.Trace)
			kv.tryApplyCommand(loopTrace, commandWithTrace.Value, applyMsg.CommandIndex)
			continue
		}

		if applyMsg.SnapshotValid {
			kv.tryApplySnapshot(loopTrace, applyMsg.Snapshot)
			continue
		}

		logUnlocker := kv.lock(ApplyFlow)
		Log(kv.logContext(loopTrace, ApplyFlow), ErrorLevel, "invalid applyMsg: %+v", applyMsg)
		logUnlocker.unlock(ApplyFlow)
	}
}

func (kv *KVServer) tryApplyCommand(
	trace telemetry.Trace,
	command Command,
	commandIndex int,
) {
	unlocker := kv.lock(ApplyCommandFlow)
	logContext := kv.logContext(trace, ApplyCommandFlow)
	Log(logContext, InfoLevel, "tryApplyCommand enter: command=%+v", command)
	kv.lastKnownRaftCommitIndex = commandIndex

	startAt := time.Now()
	operation := Operation{
		ClientID:    command.ClientID,
		OperationID: command.OperationID,
	}

	switch command.OpType {
	case GetOpType:
		operations, ok := kv.getOperations[command.ClientID]
		Log(logContext, InfoLevel, "before dedup Get: command=%+v, ok=%+v, operations=%+v", command, ok, operations)

		if ok && command.OperationID <= operations.AppliedOperationID {
			Log(logContext, InfoLevel, "operation is already applied, skipping: duration=%v", time.Since(startAt))
			unlocker.unlock(ApplyCommandFlow)
			return
		}

		value := kv.applyGet(trace, command.GetOp)
		operations = Operations[string]{
			AppliedOperationID: command.OperationID,
			Result:             value,
		}
		kv.getOperations[command.ClientID] = operations
		asyncWaiterWithTrace, ok := kv.getWaiters[operation]
		delete(kv.getWaiters, operation)
		Log(kv.logContext(trace, ApplyFlow), InfoLevel, "before try notifying waiter")
		unlocker.unlock(ApplyCommandFlow)

		if ok {
			result := GetWaiterResult{
				Value: value,
				Err:   OkErr,
			}
			select {
			case asyncWaiterWithTrace.Value <- result:
				Log(kv.logContext(trace, ApplyCommandFlow), InfoLevel, "notified async waiter")
			case <-kv.serverCancelContext.OnCancel(logContext):
				Log(kv.logContext(trace, ApplyCommandFlow), InfoLevel, "canceled, exit tryApplyCommand")
			}

			close(asyncWaiterWithTrace.Value)
		}

		unlocker = kv.lock(ApplyCommandFlow)
		defer unlocker.unlock(ApplyCommandFlow)
		Log(kv.logContext(trace, ApplyCommandFlow), InfoLevel, "tryApplyCommand exit: duration=%+v", time.Since(startAt))
	case PutOpType:
		operations, ok := kv.putOperations[command.ClientID]
		Log(logContext, InfoLevel, "before dedup Put: command=%+v, ok=%+v, operations=%+v", command, ok, operations)

		if ok && command.OperationID <= operations.AppliedOperationID {
			Log(logContext, InfoLevel, "operation is already applied, skipping: duration=%v", time.Since(startAt))
			unlocker.unlock(ApplyCommandFlow)
			return
		}

		kv.applyPut(trace, command.PutOp)
		operations = Operations[any]{
			AppliedOperationID: command.OperationID,
		}
		kv.putOperations[command.ClientID] = operations

		asyncWaiterWithTrace, ok := kv.putWaiters[operation]
		delete(kv.putWaiters, operation)
		Log(kv.logContext(trace, ApplyFlow), InfoLevel, "before try notifying waiter")
		unlocker.unlock(ApplyCommandFlow)

		if ok {
			select {
			case asyncWaiterWithTrace.Value <- OkErr:
				Log(kv.logContext(trace, ApplyCommandFlow), InfoLevel, "notified async waiter")
			case <-kv.serverCancelContext.OnCancel(logContext):
				Log(kv.logContext(trace, ApplyCommandFlow), InfoLevel, "canceled, exit tryApplyCommand")
			}

			close(asyncWaiterWithTrace.Value)
		}

		unlocker = kv.lock(ApplyCommandFlow)
		defer unlocker.unlock(ApplyCommandFlow)
		Log(kv.logContext(trace, ApplyCommandFlow), InfoLevel, "tryApplyCommand exit: duration=%+v", time.Since(startAt))
	case AppendOpType:
		operations, ok := kv.appendOperations[command.ClientID]
		Log(logContext, InfoLevel, "before dedup Append: command=%+v, ok=%+v, operations=%+v", command, ok, operations)

		if ok && command.OperationID <= operations.AppliedOperationID {
			Log(logContext, InfoLevel, "operation is already applied, skipping: duration=%v", time.Since(startAt))
			unlocker.unlock(ApplyCommandFlow)
			return
		}

		kv.applyAppend(trace, command.AppendOp)
		operations = Operations[any]{
			AppliedOperationID: command.OperationID,
		}
		kv.appendOperations[command.ClientID] = operations

		asyncWaiterWithTrace, ok := kv.appendWaiters[operation]
		delete(kv.appendWaiters, operation)
		Log(kv.logContext(trace, ApplyFlow), InfoLevel, "before try notifying waiter")
		unlocker.unlock(ApplyCommandFlow)

		if ok {
			select {
			case asyncWaiterWithTrace.Value <- OkErr:
				Log(kv.logContext(trace, ApplyCommandFlow), InfoLevel, "notified async waiter")
			case <-kv.serverCancelContext.OnCancel(logContext):
				Log(kv.logContext(trace, ApplyCommandFlow), InfoLevel, "canceled, exit tryApplyCommand")
			}

			close(asyncWaiterWithTrace.Value)
		}

		unlocker = kv.lock(ApplyCommandFlow)
		defer unlocker.unlock(ApplyCommandFlow)
		Log(kv.logContext(trace, ApplyCommandFlow), InfoLevel, "tryApplyCommand exit: duration=%+v", time.Since(startAt))
	default:
		Log(kv.logContext(trace, ApplyCommandFlow), ErrorLevel, "invalid command.OpType: %+v", command.OpType)
		unlocker.unlock(ApplyCommandFlow)
	}
}

func (kv *KVServer) applyPut(trace telemetry.Trace, putOp PutOp) {
	logContext := kv.logContext(trace, ApplyCommandFlow)
	Log(logContext, InfoLevel, "applyPut: putOp=%+v, keyValueMap=%+v", putOp, kv.keyValueMap)
	kv.keyValueMap[putOp.Key] = putOp.Value
}

func (kv *KVServer) applyAppend(trace telemetry.Trace, appendOp AppendOp) {
	logContext := kv.logContext(trace, ApplyCommandFlow)
	Log(logContext, InfoLevel, "applyAppend: appendOp=%+v, keyValueMap=%+v", appendOp, kv.keyValueMap)
	kv.keyValueMap[appendOp.Key] += appendOp.Arg
}

func (kv *KVServer) applyGet(trace telemetry.Trace, getOp GetOp) string {
	Log(kv.logContext(trace, ApplyCommandFlow), InfoLevel, "applyGet: getOp=%+v, keyValueMap=%+v", getOp, kv.keyValueMap)
	return kv.keyValueMap[getOp.Key]
}

func (kv *KVServer) tryApplySnapshot(
	trace telemetry.Trace,
	snapshotData []byte,
) {
	snapshot, err := unmarshalSnapshot(snapshotData)
	if err != nil {
		Log(kv.logContext(trace, ApplySnapshotFlow), ErrorLevel, "failed to unmarshal snapshot: err=%v", err)
		return
	}

	unlocker := kv.lock(ApplySnapshotFlow)
	defer unlocker.unlock(ApplySnapshotFlow)
	kv.keyValueMap = snapshot.KeyValueMap
	kv.getOperations = snapshot.GetOperations
	kv.putOperations = snapshot.PutOperations
	kv.appendOperations = snapshot.AppendOperations
}

func (kv *KVServer) monitorRaftState(trace telemetry.Trace) {
	for {
		unlocker := kv.lock(RaftStateFlow)
		loopTrace := kv.newTrace()
		Log(kv.logContext(trace, RaftStateFlow), DebugLevel, "[loop-trace:(%v)] enter new monitorRaftState loop", loopTrace)

		if kv.serverCancelContext.IsCanceled(kv.logContext(loopTrace, RaftStateFlow)) {
			Log(kv.logContext(loopTrace, RaftStateFlow), DebugLevel, "canceled, exit monitorRaftState")
			unlocker.unlock(RaftStateFlow)
			return
		}

		newTerm, _ := kv.rf.GetState()
		if newTerm > kv.lastKnownLeaderTerm {
			Log(kv.logContext(loopTrace, RaftStateFlow), DebugLevel, "lost leadership: lastKnownTerm=%v, newTerm=%v",
				kv.lastKnownLeaderTerm,
				newTerm)
			kv.lastKnownLeaderTerm = newTerm
			unlocker.unlock(RaftStateFlow)
			kv.notifyLostLeadership()

			unlocker = kv.lock(RaftStateFlow)
		}

		logContext := kv.logContext(loopTrace, RaftStateFlow)

		if kv.maxRaftState >= 0 {
			if kv.persister.RaftStateSize() >= kv.maxRaftState {
				Log(logContext, InfoLevel, "taking snapshot: raftStateSize=%v, maxRaftState=%v", kv.maxRaftState, kv.persister.RaftStateSize())
				snapshotData, err := kv.marshalSnapshot()
				if err != nil {
					Log(logContext, ErrorLevel, "failed to marshal snapshot: err=%v", err)
				} else {
					kv.rf.Snapshot(kv.lastKnownRaftCommitIndex, snapshotData)
				}
			}
		}

		unlocker.unlock(RaftStateFlow)

		Log(logContext, DebugLevel, "check raft state again after %v", monitorRaftStateInterval)

		select {
		case <-time.After(monitorRaftStateInterval):
		case <-kv.serverCancelContext.OnCancel(logContext):
			unlocker = kv.lock(RaftStateFlow)
			Log(kv.logContext(loopTrace, RaftStateFlow), DebugLevel, "canceled, exit monitorRaftState")
			unlocker.unlock(RaftStateFlow)
			return
		}
	}
}

func (kv *KVServer) marshalSnapshot() ([]byte, error) {
	snapshot := Snapshot{
		KeyValueMap:      kv.keyValueMap,
		GetOperations:    kv.getOperations,
		PutOperations:    kv.putOperations,
		AppendOperations: kv.appendOperations,
	}
	return json.Marshal(snapshot)
}

func (kv *KVServer) notifyLostLeadership() {
	unlocker := kv.lock(RaftStateFlow)
	getWaiters := kv.getWaiters
	putWaiters := kv.putWaiters
	appendWaiters := kv.appendWaiters
	kv.getWaiters = make(map[Operation]telemetry.WithTrace[chan GetWaiterResult])
	kv.putWaiters = make(map[Operation]telemetry.WithTrace[chan Err])
	kv.appendWaiters = make(map[Operation]telemetry.WithTrace[chan Err])
	unlocker.unlock(RaftStateFlow)

	for _, waiterWithTrace := range getWaiters {
		Log(kv.logContext(waiterWithTrace.Trace, RaftStateFlow), InfoLevel, "not leader anymore, notify waiter")
		waiterWithTrace.Value <- GetWaiterResult{
			Err: ErrWrongLeaderErr,
		}
		close(waiterWithTrace.Value)
	}

	for _, waiterWithTrace := range putWaiters {
		Log(kv.logContext(waiterWithTrace.Trace, RaftStateFlow), InfoLevel, "not leader anymore, notify waiter")
		waiterWithTrace.Value <- ErrWrongLeaderErr
		close(waiterWithTrace.Value)
	}

	for _, waiterWithTrace := range appendWaiters {
		Log(kv.logContext(waiterWithTrace.Trace, RaftStateFlow), InfoLevel, "not leader anymore, notify waiter")
		waiterWithTrace.Value <- ErrWrongLeaderErr
		close(waiterWithTrace.Value)
	}
}

func (kv *KVServer) logContext(trace telemetry.Trace, flow Flow) LogContext {
	return LogContext{
		EndpointNamespace: serverNamespace,
		EndpointID:        kv.serverID,
		Flow:              flow,
		Trace:             &trace,
	}
}

func (kv *KVServer) newTrace() telemetry.Trace {
	nextTraceID := kv.nextTraceID
	kv.nextTraceID++
	trace := telemetry.Trace{
		Namespace:  serverNamespace,
		EndpointID: kv.serverID,
		TraceID:    nextTraceID,
	}
	return trace
}

func (kv *KVServer) lock(flow Flow) *Unlocker {
	kv.mu.Lock()
	nextLockID := kv.nextLockID
	kv.nextLockID++
	LogAndSkipCallers(
		LogContext{
			EndpointID: kv.serverID,
			Flow:       flow,
		}, DebugLevel, 1, "lock(%v)", nextLockID)
	return &Unlocker{
		lockID:     nextLockID,
		unlockFunc: kv.unlock,
	}
}

func (kv *KVServer) unlock(lockID uint64, flow Flow, skipCallers int) {
	LogAndSkipCallers(LogContext{
		EndpointID: kv.serverID,
		Flow:       flow,
	}, DebugLevel, skipCallers+1, "unlock(%v)", lockID)
	kv.mu.Unlock()
}

func unmarshalSnapshot(snapshotData []byte) (Snapshot, error) {
	snapshot := Snapshot{}
	err := json.Unmarshal(snapshotData, &snapshot)
	return snapshot, err
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(
	servers []*labrpc.ClientEnd,
	serverID int,
	persister *raft.Persister,
	maxRaftState int,
) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(telemetry.WithTrace[Command]{})

	applyCh := make(chan raft.ApplyMsg)
	rf := raft.Make(servers, serverID, persister, applyCh)
	defaultTrace := telemetry.Trace{
		Namespace:  serverNamespace,
		EndpointID: serverID,
		TraceID:    0,
	}
	kv := KVServer{
		serverID:            serverID,
		rf:                  rf,
		persister:           persister,
		applyCh:             applyCh,
		maxRaftState:        maxRaftState,
		keyValueMap:         make(map[string]string),
		serverCancelContext: newCancelContext(defaultTrace),
		serverWaitGroup:     new(sync.WaitGroup),
		taskTracker:         newTaskTracker(),
		getWaiters:          make(map[Operation]telemetry.WithTrace[chan GetWaiterResult]),
		putWaiters:          make(map[Operation]telemetry.WithTrace[chan Err]),
		appendWaiters:       make(map[Operation]telemetry.WithTrace[chan Err]),
		getOperations:       make(map[int]Operations[string]),
		putOperations:       make(map[int]Operations[any]),
		appendOperations:    make(map[int]Operations[any]),
	}

	applyFinisher := kv.taskTracker.Add(kv.logContext(defaultTrace, InitFlow), 1)
	kv.serverWaitGroup.Add(1)
	go func() {
		defer kv.serverWaitGroup.Done()
		unlocker := kv.lock(InitFlow)
		logContext := kv.logContext(defaultTrace, InitFlow)
		defer applyFinisher.Done(logContext)
		unlocker.unlock(InitFlow)

		kv.applyCommandAndSnapshot(defaultTrace)
	}()

	monitorFinisher := kv.taskTracker.Add(kv.logContext(defaultTrace, InitFlow), 1)
	kv.serverWaitGroup.Add(1)
	go func() {
		defer kv.serverWaitGroup.Done()
		unlocker := kv.lock(InitFlow)
		logContext := kv.logContext(defaultTrace, InitFlow)
		defer monitorFinisher.Done(logContext)
		unlocker.unlock(InitFlow)

		kv.monitorRaftState(defaultTrace)
	}()
	return &kv
}
