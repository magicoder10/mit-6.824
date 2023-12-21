package shardctrler

import (
	"6.5840/raft"
	"6.5840/telemetry"
	"sort"
	"time"
)
import "6.5840/labrpc"
import "sync"
import "6.5840/labgob"

const monitorRaftStateInterval = 50 * time.Millisecond

type OpType string

const (
	JoinOpType  OpType = "Join"
	LeaveOpType OpType = "Leave"
	MoveOpType  OpType = "Move"
	QueryOpType OpType = "Query"
)

type JoinOp struct {
	Servers map[int][]string
}

type LeaveOp struct {
	GIDs []int
}

type MoveOp struct {
	Shard int
	GID   int
}

type QueryOp struct {
	Num int
}

type Command struct {
	LeaderID    int
	ClientID    int
	OperationID uint64
	OpType      OpType
	JoinOp      JoinOp
	LeaveOp     LeaveOp
	MoveOp      MoveOp
	QueryOp     QueryOp
}

type Operations[Result any] struct {
	AppliedOperationID uint64
	Result             Result
}

type QueryWaiterResult struct {
	Config Config
	Err    Err
}

type Operation struct {
	ClientID    int
	OperationID uint64
}

type ShardCtrler struct {
	mu       sync.Mutex
	serverID int
	rf       *raft.Raft
	applyCh  chan raft.ApplyMsg

	lastKnownLeaderTerm int
	configs             []Config // indexed by config num

	joinWaiters  map[Operation]telemetry.WithTrace[chan Err]
	leaveWaiters map[Operation]telemetry.WithTrace[chan Err]
	moveWaiters  map[Operation]telemetry.WithTrace[chan Err]
	queryWaiters map[Operation]telemetry.WithTrace[chan QueryWaiterResult]

	joinOperations  map[int]Operations[any]
	leaveOperations map[int]Operations[any]
	moveOperations  map[int]Operations[any]
	queryOperations map[int]Operations[Config]

	serverCancelContext *CancelContext
	serverWaitGroup     *sync.WaitGroup
	taskTracker         *TaskTracker

	nextLockID  uint64
	nextTraceID uint64
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	unlocker := sc.lock(ExecuteOpFlow)
	Log(sc.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "Join enter: args=%+v", args)
	operations, ok := sc.joinOperations[args.OperationContext.ClientID]
	if ok {
		if args.OperationContext.OperationID <= operations.AppliedOperationID {
			Log(sc.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "skip duplicated operation, Join exit")
			reply.Err = OkErr
			unlocker.unlock(ExecuteOpFlow)
			return
		}
	}

	if sc.serverCancelContext.IsCanceled(sc.logContext(args.OperationContext.Trace, ExecuteOpFlow)) {
		Log(sc.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "server canceled, Join exit")
		reply.Err = ErrCancelledErr
		unlocker.unlock(ExecuteOpFlow)
		return
	}

	asyncWaiter := make(chan Err)
	operation := Operation{
		ClientID:    args.OperationContext.ClientID,
		OperationID: args.OperationContext.OperationID,
	}
	sc.joinWaiters[operation] = telemetry.WithTrace[chan Err]{
		Trace: args.OperationContext.Trace,
		Value: asyncWaiter,
	}

	command := Command{
		LeaderID:    sc.serverID,
		ClientID:    args.OperationContext.ClientID,
		OperationID: args.OperationContext.OperationID,
		OpType:      JoinOpType,
		JoinOp: JoinOp{
			Servers: args.Servers,
		},
	}

	_, term, isLeader := sc.rf.Start(telemetry.WithTrace[Command]{
		Trace: args.OperationContext.Trace,
		Value: command,
	})
	Log(sc.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "wait for async waiter")
	if !isLeader {
		Log(sc.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "not leader, close and remove waiter")
		delete(sc.joinWaiters, operation)
		close(asyncWaiter)

		reply.Err = ErrWrongLeaderErr
		Log(sc.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "not leader, Join exit: term=%v, reply=%+v", term, reply)
		unlocker.unlock(ExecuteOpFlow)
		return
	}

	Log(sc.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "accept Join at term %v: %+v", term, command)
	sc.lastKnownLeaderTerm = term
	logContext := sc.logContext(args.OperationContext.Trace, ExecuteOpFlow)
	unlocker.unlock(ExecuteOpFlow)

	select {
	case err, ok := <-asyncWaiter:
		unlocker = sc.lock(ExecuteOpFlow)
		defer unlocker.unlock(ExecuteOpFlow)
		if !ok {
			Log(sc.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "async waiter closed, Join exit")
			return
		}

		Log(sc.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "async waiter finished")
		if err == ErrWrongLeaderErr {
			Log(sc.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "not leader, Join exit")
			reply.Err = ErrWrongLeaderErr
			return
		}

		Log(sc.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "Join done")
		reply.Err = OkErr
		Log(sc.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "Join exit: reply=%+v", reply)
	case <-sc.serverCancelContext.OnCancel(logContext):
		unlocker = sc.lock(ExecuteOpFlow)
		defer unlocker.unlock(ExecuteOpFlow)
		reply.Err = ErrCancelledErr
		Log(sc.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "Join canceled, exit: reply=%+v", reply)
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	unlocker := sc.lock(ExecuteOpFlow)
	Log(sc.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "Leave enter: args=%+v", args)
	operations, ok := sc.leaveOperations[args.OperationContext.ClientID]
	if ok {
		if args.OperationContext.OperationID <= operations.AppliedOperationID {
			Log(sc.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "skip duplicated operation, Leave exit")
			reply.Err = OkErr
			unlocker.unlock(ExecuteOpFlow)
			return
		}
	}

	if sc.serverCancelContext.IsCanceled(sc.logContext(args.OperationContext.Trace, ExecuteOpFlow)) {
		Log(sc.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "server canceled, Leave exit")
		reply.Err = ErrCancelledErr
		unlocker.unlock(ExecuteOpFlow)
		return
	}

	asyncWaiter := make(chan Err)
	operation := Operation{
		ClientID:    args.OperationContext.ClientID,
		OperationID: args.OperationContext.OperationID,
	}
	sc.leaveWaiters[operation] = telemetry.WithTrace[chan Err]{
		Trace: args.OperationContext.Trace,
		Value: asyncWaiter,
	}

	command := Command{
		LeaderID:    sc.serverID,
		ClientID:    args.OperationContext.ClientID,
		OperationID: args.OperationContext.OperationID,
		OpType:      LeaveOpType,
		LeaveOp: LeaveOp{
			GIDs: args.GIDs,
		},
	}

	_, term, isLeader := sc.rf.Start(telemetry.WithTrace[Command]{
		Trace: args.OperationContext.Trace,
		Value: command,
	})
	Log(sc.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "wait for async waiter")
	if !isLeader {
		Log(sc.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "not leader, close and remove waiter")
		delete(sc.leaveWaiters, operation)
		close(asyncWaiter)

		reply.Err = ErrWrongLeaderErr
		Log(sc.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "not leader, Leave exit: term=%v, reply=%+v", term, reply)
		unlocker.unlock(ExecuteOpFlow)
		return
	}

	Log(sc.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "accept Leave at term %v: %+v", term, command)
	sc.lastKnownLeaderTerm = term
	logContext := sc.logContext(args.OperationContext.Trace, ExecuteOpFlow)
	unlocker.unlock(ExecuteOpFlow)

	select {
	case err, ok := <-asyncWaiter:
		unlocker = sc.lock(ExecuteOpFlow)
		defer unlocker.unlock(ExecuteOpFlow)
		if !ok {
			Log(sc.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "async waiter closed, Leave exit")
			return
		}

		Log(sc.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "async waiter finished")
		if err == ErrWrongLeaderErr {
			Log(sc.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "not leader, Leave exit")
			reply.Err = ErrWrongLeaderErr
			return
		}

		Log(sc.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "Leave done")
		reply.Err = OkErr
		Log(sc.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "Leave exit: reply=%+v", reply)
	case <-sc.serverCancelContext.OnCancel(logContext):
		unlocker = sc.lock(ExecuteOpFlow)
		defer unlocker.unlock(ExecuteOpFlow)
		reply.Err = ErrCancelledErr
		Log(sc.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "Leave canceled, exit: reply=%+v", reply)
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	unlocker := sc.lock(ExecuteOpFlow)
	Log(sc.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "Move enter: args=%+v", args)
	operations, ok := sc.moveOperations[args.OperationContext.ClientID]
	if ok {
		if args.OperationContext.OperationID <= operations.AppliedOperationID {
			Log(sc.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "skip duplicated operation, Move exit")
			reply.Err = OkErr
			unlocker.unlock(ExecuteOpFlow)
			return
		}
	}

	if sc.serverCancelContext.IsCanceled(sc.logContext(args.OperationContext.Trace, ExecuteOpFlow)) {
		Log(sc.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "server canceled, Move exit")
		reply.Err = ErrCancelledErr
		unlocker.unlock(ExecuteOpFlow)
		return
	}

	asyncWaiter := make(chan Err)
	operation := Operation{
		ClientID:    args.OperationContext.ClientID,
		OperationID: args.OperationContext.OperationID,
	}
	sc.moveWaiters[operation] = telemetry.WithTrace[chan Err]{
		Trace: args.OperationContext.Trace,
		Value: asyncWaiter,
	}

	command := Command{
		LeaderID:    sc.serverID,
		ClientID:    args.OperationContext.ClientID,
		OperationID: args.OperationContext.OperationID,
		OpType:      MoveOpType,
		MoveOp: MoveOp{
			Shard: args.Shard,
			GID:   args.GID,
		},
	}

	_, term, isLeader := sc.rf.Start(telemetry.WithTrace[Command]{
		Trace: args.OperationContext.Trace,
		Value: command,
	})
	Log(sc.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "wait for async waiter")
	if !isLeader {
		Log(sc.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "not leader, close and remove waiter")
		delete(sc.moveWaiters, operation)
		close(asyncWaiter)

		reply.Err = ErrWrongLeaderErr
		Log(sc.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "not leader, Move exit: term=%v, reply=%+v", term, reply)
		unlocker.unlock(ExecuteOpFlow)
		return
	}

	Log(sc.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "accept Move at term %v: %+v", term, command)
	sc.lastKnownLeaderTerm = term
	logContext := sc.logContext(args.OperationContext.Trace, ExecuteOpFlow)
	unlocker.unlock(ExecuteOpFlow)

	select {
	case err, ok := <-asyncWaiter:
		unlocker = sc.lock(ExecuteOpFlow)
		defer unlocker.unlock(ExecuteOpFlow)
		if !ok {
			Log(sc.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "async waiter closed, Move exit")
			return
		}

		Log(sc.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "async waiter finished")
		if err == ErrWrongLeaderErr {
			Log(sc.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "not leader, Move exit")
			reply.Err = ErrWrongLeaderErr
			return
		}

		Log(sc.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "Move done")
		reply.Err = OkErr
		Log(sc.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "Move exit: reply=%+v", reply)
	case <-sc.serverCancelContext.OnCancel(logContext):
		unlocker = sc.lock(ExecuteOpFlow)
		defer unlocker.unlock(ExecuteOpFlow)
		reply.Err = ErrCancelledErr
		Log(sc.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "Move canceled, exit: reply=%+v", reply)
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	unlocker := sc.lock(ExecuteOpFlow)
	Log(sc.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "Query enter: args=%+v", args)
	operations, ok := sc.queryOperations[args.OperationContext.ClientID]
	if ok {
		if args.OperationContext.OperationID <= operations.AppliedOperationID {
			Log(sc.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "skip duplicated operation, Query exit")
			reply.Config = operations.Result
			reply.Err = OkErr
			unlocker.unlock(ExecuteOpFlow)
			return
		}
	}

	if sc.serverCancelContext.IsCanceled(sc.logContext(args.OperationContext.Trace, ExecuteOpFlow)) {
		Log(sc.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "server canceled, Query exit")
		reply.Err = ErrCancelledErr
		unlocker.unlock(ExecuteOpFlow)
		return
	}

	asyncWaiter := make(chan QueryWaiterResult)
	operation := Operation{
		ClientID:    args.OperationContext.ClientID,
		OperationID: args.OperationContext.OperationID,
	}
	sc.queryWaiters[operation] = telemetry.WithTrace[chan QueryWaiterResult]{
		Trace: args.OperationContext.Trace,
		Value: asyncWaiter,
	}

	command := Command{
		LeaderID:    sc.serverID,
		ClientID:    args.OperationContext.ClientID,
		OperationID: args.OperationContext.OperationID,
		OpType:      QueryOpType,
		QueryOp: QueryOp{
			Num: args.Num,
		},
	}

	_, term, isLeader := sc.rf.Start(telemetry.WithTrace[Command]{
		Trace: args.OperationContext.Trace,
		Value: command,
	})
	Log(sc.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "wait for async waiter")
	if !isLeader {
		Log(sc.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "not leader, close and remove waiter")
		delete(sc.queryWaiters, operation)
		close(asyncWaiter)
		reply.Err = ErrWrongLeaderErr
		Log(sc.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "not leader, Query exit: term=%v, reply=%+v", term, reply)
		unlocker.unlock(ExecuteOpFlow)
		return
	}

	Log(sc.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "accept Query at term %v: %+v", term, command)
	sc.lastKnownLeaderTerm = term
	logContext := sc.logContext(args.OperationContext.Trace, ExecuteOpFlow)
	unlocker.unlock(ExecuteOpFlow)

	select {
	case result, ok := <-asyncWaiter:
		unlocker = sc.lock(ExecuteOpFlow)
		defer unlocker.unlock(ExecuteOpFlow)
		if !ok {
			Log(sc.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "async waiter closed, Query exit")
			return
		}

		Log(sc.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "async waiter finished")

		if result.Err == ErrWrongLeaderErr {
			Log(sc.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "not leader, Query exit")
			reply.Err = ErrWrongLeaderErr
			return
		}

		Log(sc.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "Query done")
		reply.Err = OkErr
		reply.Config = result.Config

		Log(sc.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "Query exit: reply=%+v", reply)
	case <-sc.serverCancelContext.OnCancel(logContext):
		unlocker = sc.lock(ExecuteOpFlow)
		defer unlocker.unlock(ExecuteOpFlow)
		reply.Err = ErrCancelledErr
		Log(sc.logContext(args.OperationContext.Trace, ExecuteOpFlow), InfoLevel, "Query canceled, exit: reply=%+v", reply)
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	unlocker := sc.lock(TerminationFlow)
	trace := sc.newTrace()
	Log(sc.logContext(trace, TerminationFlow), InfoLevel, "Kill enter")
	defer Log(sc.logContext(trace, TerminationFlow), InfoLevel, "Kill exit")
	sc.serverCancelContext.TryCancel(sc.logContext(trace, TerminationFlow))
	logContext := sc.logContext(trace, TerminationFlow)
	unlocker.unlock(TerminationFlow)

	sc.rf.Kill()

	Log(logContext, InfoLevel, "wait for goroutines to finish")
	sc.serverWaitGroup.Wait()
	Log(logContext, InfoLevel, "all goroutines finished")

	unlocker = sc.lock(TerminationFlow)
	defer unlocker.unlock(TerminationFlow)

	Log(sc.logContext(trace, TerminationFlow), InfoLevel, "close applyCh")
	close(sc.applyCh)

	for _, waiterWithTrace := range sc.joinWaiters {
		Log(sc.logContext(waiterWithTrace.Trace, RaftStateFlow), InfoLevel, "cancelled waiter")
		close(waiterWithTrace.Value)
	}

	for _, waiterWithTrace := range sc.leaveWaiters {
		Log(sc.logContext(waiterWithTrace.Trace, RaftStateFlow), InfoLevel, "cancelled waiter")
		close(waiterWithTrace.Value)
	}

	for _, waiterWithTrace := range sc.moveWaiters {
		Log(sc.logContext(waiterWithTrace.Trace, RaftStateFlow), InfoLevel, "cancelled waiter")
		close(waiterWithTrace.Value)
	}

	for _, waiterWithTrace := range sc.queryWaiters {
		Log(sc.logContext(waiterWithTrace.Trace, RaftStateFlow), InfoLevel, "cancelled waiter")
		close(waiterWithTrace.Value)
	}
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) applyCommand(trace telemetry.Trace) {
	for {
		unlocker := sc.lock(ApplyFlow)
		loopTrace := sc.newTrace()
		logContext := sc.logContext(trace, ApplyFlow)
		Log(sc.logContext(trace, ApplyFlow), InfoLevel, "[loop-trace:(%v)] enter new applyCommand loop", loopTrace)
		unlocker.unlock(ApplyFlow)
		var applyMsg raft.ApplyMsg
		var ok bool
		select {
		case applyMsg, ok = <-sc.applyCh:
			if !ok {
				logUnlocker := sc.lock(ApplyFlow)
				Log(sc.logContext(loopTrace, ApplyFlow), InfoLevel, "applyCh closed, exit applyCommand")
				logUnlocker.unlock(ApplyFlow)
				return
			}
		case <-sc.serverCancelContext.OnCancel(logContext):
			logUnlocker := sc.lock(ApplyFlow)
			Log(sc.logContext(loopTrace, ApplyFlow), InfoLevel, "canceled, exit applyCommand")
			logUnlocker.unlock(ApplyFlow)
			return
		}

		unlocker = sc.lock(ApplyFlow)
		if sc.serverCancelContext.IsCanceled(sc.logContext(loopTrace, ApplyFlow)) {
			Log(sc.logContext(loopTrace, ApplyFlow), InfoLevel, "canceled, exit applyCommand")
			unlocker.unlock(ApplyFlow)
			return
		}

		Log(sc.logContext(loopTrace, ApplyFlow), InfoLevel, "received applyMsg: %+v", applyMsg)

		logContext = sc.logContext(loopTrace, ApplyFlow)
		unlocker.unlock(ApplyFlow)

		commandWithTrace := applyMsg.Command.(telemetry.WithTrace[Command])
		Log(logContext, InfoLevel, "[command-trace:(%v)] before tryApplyCommand", commandWithTrace.Trace)
		sc.tryApplyCommand(loopTrace, commandWithTrace.Value)
	}
}

func (sc *ShardCtrler) tryApplyCommand(
	trace telemetry.Trace,
	command Command,
) {
	unlocker := sc.lock(ApplyCommandFlow)
	logContext := sc.logContext(trace, ApplyCommandFlow)
	Log(logContext, InfoLevel, "tryApplyCommand enter: command=%+v", command)

	startAt := time.Now()
	operation := Operation{
		ClientID:    command.ClientID,
		OperationID: command.OperationID,
	}

	switch command.OpType {
	case JoinOpType:
		operations, ok := sc.joinOperations[command.ClientID]
		Log(logContext, InfoLevel, "before dedup Join: command=%+v, ok=%+v, operations=%+v", command, ok, operations)

		if ok && command.OperationID <= operations.AppliedOperationID {
			Log(logContext, InfoLevel, "operation is already applied, skipping: duration=%v", time.Since(startAt))
			unlocker.unlock(ApplyCommandFlow)
			return
		}

		sc.applyJoin(trace, command.JoinOp)
		operations = Operations[any]{
			AppliedOperationID: command.OperationID,
		}
		sc.joinOperations[command.ClientID] = operations

		asyncWaiterWithTrace, ok := sc.joinWaiters[operation]
		delete(sc.joinWaiters, operation)
		Log(sc.logContext(trace, ApplyFlow), InfoLevel, "before try notifying waiter")
		unlocker.unlock(ApplyCommandFlow)

		if ok {
			select {
			case asyncWaiterWithTrace.Value <- OkErr:
				Log(sc.logContext(trace, ApplyCommandFlow), InfoLevel, "notified async waiter")
			case <-sc.serverCancelContext.OnCancel(logContext):
				Log(sc.logContext(trace, ApplyCommandFlow), InfoLevel, "canceled, exit tryApplyCommand")
			}

			close(asyncWaiterWithTrace.Value)
		}

		unlocker = sc.lock(ApplyCommandFlow)
		defer unlocker.unlock(ApplyCommandFlow)
		Log(sc.logContext(trace, ApplyCommandFlow), InfoLevel, "tryApplyCommand exit: duration=%+v", time.Since(startAt))
	case LeaveOpType:
		operations, ok := sc.leaveOperations[command.ClientID]
		Log(logContext, InfoLevel, "before dedup Leave: command=%+v, ok=%+v, operations=%+v", command, ok, operations)

		if ok && command.OperationID <= operations.AppliedOperationID {
			Log(logContext, InfoLevel, "operation is already applied, skipping: duration=%v", time.Since(startAt))
			unlocker.unlock(ApplyCommandFlow)
			return
		}

		sc.applyLeave(trace, command.LeaveOp)
		operations = Operations[any]{
			AppliedOperationID: command.OperationID,
		}
		sc.leaveOperations[command.ClientID] = operations

		asyncWaiterWithTrace, ok := sc.leaveWaiters[operation]
		delete(sc.leaveWaiters, operation)
		Log(sc.logContext(trace, ApplyFlow), InfoLevel, "before try notifying waiter")
		unlocker.unlock(ApplyCommandFlow)

		if ok {
			select {
			case asyncWaiterWithTrace.Value <- OkErr:
				Log(sc.logContext(trace, ApplyCommandFlow), InfoLevel, "notified async waiter")
			case <-sc.serverCancelContext.OnCancel(logContext):
				Log(sc.logContext(trace, ApplyCommandFlow), InfoLevel, "canceled, exit tryApplyCommand")
			}

			close(asyncWaiterWithTrace.Value)
		}

		unlocker = sc.lock(ApplyCommandFlow)
		defer unlocker.unlock(ApplyCommandFlow)
		Log(sc.logContext(trace, ApplyCommandFlow), InfoLevel, "tryApplyCommand exit: duration=%+v", time.Since(startAt))
	case MoveOpType:
		operations, ok := sc.moveOperations[command.ClientID]
		Log(logContext, InfoLevel, "before dedup Move: command=%+v, ok=%+v, operations=%+v", command, ok, operations)

		if ok && command.OperationID <= operations.AppliedOperationID {
			Log(logContext, InfoLevel, "operation is already applied, skipping: duration=%v", time.Since(startAt))
			unlocker.unlock(ApplyCommandFlow)
			return
		}

		sc.applyMove(trace, command.MoveOp)
		operations = Operations[any]{
			AppliedOperationID: command.OperationID,
		}
		sc.moveOperations[command.ClientID] = operations

		asyncWaiterWithTrace, ok := sc.moveWaiters[operation]
		delete(sc.moveWaiters, operation)
		Log(sc.logContext(trace, ApplyFlow), InfoLevel, "before try notifying waiter")
		unlocker.unlock(ApplyCommandFlow)

		if ok {
			select {
			case asyncWaiterWithTrace.Value <- OkErr:
				Log(sc.logContext(trace, ApplyCommandFlow), InfoLevel, "notified async waiter")
			case <-sc.serverCancelContext.OnCancel(logContext):
				Log(sc.logContext(trace, ApplyCommandFlow), InfoLevel, "canceled, exit tryApplyCommand")
			}

			close(asyncWaiterWithTrace.Value)
		}

		unlocker = sc.lock(ApplyCommandFlow)
		defer unlocker.unlock(ApplyCommandFlow)
		Log(sc.logContext(trace, ApplyCommandFlow), InfoLevel, "tryApplyCommand exit: duration=%+v", time.Since(startAt))
	case QueryOpType:
		operations, ok := sc.queryOperations[command.ClientID]
		Log(logContext, InfoLevel, "before dedup Query: command=%+v, ok=%+v, operations=%+v", command, ok, operations)

		if ok && command.OperationID <= operations.AppliedOperationID {
			Log(logContext, InfoLevel, "operation is already applied, skipping: duration=%v", time.Since(startAt))
			unlocker.unlock(ApplyCommandFlow)
			return
		}

		cfg := sc.applyQuery(trace, command.QueryOp)
		operations = Operations[Config]{
			AppliedOperationID: command.OperationID,
			Result:             cfg,
		}
		sc.queryOperations[command.ClientID] = operations

		asyncWaiterWithTrace, ok := sc.queryWaiters[operation]
		delete(sc.queryWaiters, operation)
		Log(sc.logContext(trace, ApplyFlow), InfoLevel, "before try notifying waiter")
		unlocker.unlock(ApplyCommandFlow)

		if ok {
			result := QueryWaiterResult{
				Config: cfg,
				Err:    OkErr,
			}
			select {
			case asyncWaiterWithTrace.Value <- result:
				Log(sc.logContext(trace, ApplyCommandFlow), InfoLevel, "notified async waiter")
			case <-sc.serverCancelContext.OnCancel(logContext):
				Log(sc.logContext(trace, ApplyCommandFlow), InfoLevel, "canceled, exit tryApplyCommand")
			}

			close(asyncWaiterWithTrace.Value)
		}

		unlocker = sc.lock(ApplyCommandFlow)
		defer unlocker.unlock(ApplyCommandFlow)
		Log(sc.logContext(trace, ApplyCommandFlow), InfoLevel, "tryApplyCommand exit: duration=%+v", time.Since(startAt))
	default:
		Log(sc.logContext(trace, ApplyCommandFlow), ErrorLevel, "invalid command.OpType: %+v", command.OpType)
		unlocker.unlock(ApplyCommandFlow)
	}
}

func (sc *ShardCtrler) applyJoin(trace telemetry.Trace, joinOp JoinOp) {
	logContext := sc.logContext(trace, ApplyCommandFlow)
	Log(logContext, InfoLevel, "applyJoin: joinOp=%+v, configs=%+v", joinOp, sc.configs)
	oldCfg := sc.getConfig(len(sc.configs))
	Log(logContext, InfoLevel, "before applying Join: %+v", oldCfg)

	newCfg := Config{
		Num:    oldCfg.Num + 1,
		Shards: [10]int{},
		Groups: map[int][]string{},
	}

	for groupID, oldServers := range oldCfg.Groups {
		newServers := make([]string, len(oldServers))
		copy(newServers, oldServers)
		newCfg.Groups[groupID] = newServers
	}

	newGroupIDs := make([]int, 0)
	for groupID, servers := range joinOp.Servers {
		newServers := make([]string, len(servers))
		copy(newServers, servers)
		newCfg.Groups[groupID] = newServers
		newGroupIDs = append(newGroupIDs, groupID)
	}

	sort.Slice(newGroupIDs, func(firstIndex, secondIndex int) bool {
		return newGroupIDs[firstIndex] < newGroupIDs[secondIndex]
	})

	newNumOfGroups := len(oldCfg.Groups) + len(joinOp.Servers)
	numOfShards := len(oldCfg.Shards)
	shardsPerGroup := max(numOfShards/newNumOfGroups, 1)
	Log(logContext, InfoLevel, "applying Join: newNumOfGroups=%v, numOfShards=%v, shardsPerGroup=%v, newGroupIDs=%v",
		newNumOfGroups,
		numOfShards,
		shardsPerGroup,
		newGroupIDs)

	newGroupIDIndex := 0
	groupToShardCount := make(map[int]int)
	for shardID, groupID := range oldCfg.Shards {
		if groupID == 0 || groupToShardCount[groupID] >= shardsPerGroup {
			newGroupID := newGroupIDs[newGroupIDIndex]
			newCfg.Shards[shardID] = newGroupID
			groupToShardCount[newGroupID]++
			newGroupIDIndex = (newGroupIDIndex + 1) % len(newGroupIDs)
			continue
		}

		newCfg.Shards[shardID] = groupID
		groupToShardCount[groupID]++
	}

	Log(logContext, InfoLevel, "after applying Join: %+v", newCfg)
	sc.configs = append(sc.configs, newCfg)
}

func (sc *ShardCtrler) applyLeave(trace telemetry.Trace, leaveOp LeaveOp) {
	logContext := sc.logContext(trace, ApplyCommandFlow)
	Log(logContext, InfoLevel, "applyLeave: leaveOp=%+v, configs=%+v", leaveOp, sc.configs)
	oldCfg := sc.getConfig(len(sc.configs))
	Log(logContext, InfoLevel, "before applying Leave: %+v", oldCfg)

	newCfg := Config{
		Num:    oldCfg.Num + 1,
		Shards: [10]int{},
		Groups: map[int][]string{},
	}

	leaveGroupIDs := make(map[int]struct{})
	for _, groupID := range leaveOp.GIDs {
		leaveGroupIDs[groupID] = struct{}{}
	}

	newGroupIDs := make([]int, 0)
	for groupID, oldServers := range oldCfg.Groups {
		_, ok := leaveGroupIDs[groupID]
		if ok {
			continue
		}

		newServers := make([]string, len(oldServers))
		copy(newServers, oldServers)
		newCfg.Groups[groupID] = newServers
		newGroupIDs = append(newGroupIDs, groupID)
	}

	groupToShardCount := make(map[int]int)
	for _, groupID := range oldCfg.Shards {
		groupToShardCount[groupID]++
	}

	sort.SliceStable(newGroupIDs, func(firstIndex, secondIndex int) bool {
		firstGroupID := newGroupIDs[firstIndex]
		secondGroupID := newGroupIDs[secondIndex]
		if groupToShardCount[firstGroupID] == groupToShardCount[secondGroupID] {
			return firstGroupID < secondGroupID
		}

		return groupToShardCount[firstGroupID] < groupToShardCount[secondGroupID]
	})
	Log(logContext, InfoLevel, "applying Leave: newGroupIDs=%+v, groupToShardCount=%+v", newGroupIDs, groupToShardCount)

	if len(newGroupIDs) > 0 {
		newGroupIDIndex := 0
		for shardID, groupID := range oldCfg.Shards {
			if groupID == 0 {
				continue
			}

			_, ok := leaveGroupIDs[groupID]
			if ok {
				oldGroupID := newGroupIDs[newGroupIDIndex]
				newCfg.Shards[shardID] = oldGroupID
				newGroupIDIndex = (newGroupIDIndex + 1) % len(newGroupIDs)
				continue
			}

			newCfg.Shards[shardID] = groupID
		}
	}

	Log(logContext, InfoLevel, "after applying Leave: %+v", newCfg)
	sc.configs = append(sc.configs, newCfg)
}

func (sc *ShardCtrler) applyMove(trace telemetry.Trace, moveOp MoveOp) {
	logContext := sc.logContext(trace, ApplyCommandFlow)
	Log(logContext, InfoLevel, "applyMove: moveOp=%+v, configs=%+v", moveOp, sc.configs)
	oldCfg := sc.getConfig(len(sc.configs))
	Log(logContext, InfoLevel, "before applying Move: %+v", oldCfg)

	newCfg := Config{
		Num:    oldCfg.Num + 1,
		Shards: [10]int{},
		Groups: map[int][]string{},
	}

	for groupID, oldServers := range oldCfg.Groups {
		newServers := make([]string, len(oldServers))
		copy(newServers, oldServers)
		newCfg.Groups[groupID] = newServers
	}

	for shardID, groupID := range oldCfg.Shards {
		if shardID == moveOp.Shard {
			newCfg.Shards[shardID] = moveOp.GID
			continue
		}

		newCfg.Shards[shardID] = groupID
	}

	Log(logContext, InfoLevel, "after applying Move: %+v", newCfg)
	sc.configs = append(sc.configs, newCfg)
}

func (sc *ShardCtrler) applyQuery(trace telemetry.Trace, queryOp QueryOp) Config {
	logContext := sc.logContext(trace, ApplyCommandFlow)
	Log(logContext, InfoLevel, "applyQuery: queryOp=%+v, configs=%+v", queryOp, sc.configs)
	return sc.getConfig(queryOp.Num)
}

func (sc *ShardCtrler) getConfig(num int) Config {
	if num == 0 || len(sc.configs) == 0 {
		return Config{
			Num:    0,
			Shards: [NShards]int{},
			Groups: map[int][]string{},
		}
	}

	if num < 0 || num >= len(sc.configs) {
		return sc.configs[len(sc.configs)-1]
	}

	return sc.configs[num-1]
}

func (sc *ShardCtrler) monitorRaftState(trace telemetry.Trace) {
	for {
		unlocker := sc.lock(RaftStateFlow)
		loopTrace := sc.newTrace()
		Log(sc.logContext(trace, RaftStateFlow), DebugLevel, "[loop-trace:(%v)] enter new monitorRaftState loop", loopTrace)

		if sc.serverCancelContext.IsCanceled(sc.logContext(loopTrace, RaftStateFlow)) {
			Log(sc.logContext(loopTrace, RaftStateFlow), DebugLevel, "canceled, exit monitorRaftState")
			unlocker.unlock(RaftStateFlow)
			return
		}

		newTerm, _ := sc.rf.GetState()
		if newTerm > sc.lastKnownLeaderTerm {
			Log(sc.logContext(loopTrace, RaftStateFlow), DebugLevel, "lost leadership: lastKnownTerm=%v, newTerm=%v",
				sc.lastKnownLeaderTerm,
				newTerm)
			sc.lastKnownLeaderTerm = newTerm
			unlocker.unlock(RaftStateFlow)
			sc.notifyLostLeadership()

			unlocker = sc.lock(RaftStateFlow)
		}

		logContext := sc.logContext(loopTrace, RaftStateFlow)
		unlocker.unlock(RaftStateFlow)

		Log(logContext, DebugLevel, "check raft state again after %v", monitorRaftStateInterval)

		select {
		case <-time.After(monitorRaftStateInterval):
		case <-sc.serverCancelContext.OnCancel(logContext):
			unlocker = sc.lock(RaftStateFlow)
			Log(sc.logContext(loopTrace, RaftStateFlow), DebugLevel, "canceled, exit monitorRaftState")
			unlocker.unlock(RaftStateFlow)
			return
		}
	}
}

func (sc *ShardCtrler) notifyLostLeadership() {
	unlocker := sc.lock(RaftStateFlow)
	joinWaiters := sc.joinWaiters
	leaveWaiters := sc.leaveWaiters
	moveWaiters := sc.moveWaiters
	queryWaiters := sc.queryWaiters
	sc.joinWaiters = make(map[Operation]telemetry.WithTrace[chan Err])
	sc.leaveWaiters = make(map[Operation]telemetry.WithTrace[chan Err])
	sc.moveWaiters = make(map[Operation]telemetry.WithTrace[chan Err])
	sc.queryWaiters = make(map[Operation]telemetry.WithTrace[chan QueryWaiterResult])
	unlocker.unlock(RaftStateFlow)

	for _, waiterWithTrace := range joinWaiters {
		Log(sc.logContext(waiterWithTrace.Trace, RaftStateFlow), InfoLevel, "not leader anymore, notify waiter")
		waiterWithTrace.Value <- ErrWrongLeaderErr
		close(waiterWithTrace.Value)
	}

	for _, waiterWithTrace := range leaveWaiters {
		Log(sc.logContext(waiterWithTrace.Trace, RaftStateFlow), InfoLevel, "not leader anymore, notify waiter")
		waiterWithTrace.Value <- ErrWrongLeaderErr
		close(waiterWithTrace.Value)
	}

	for _, waiterWithTrace := range moveWaiters {
		Log(sc.logContext(waiterWithTrace.Trace, RaftStateFlow), InfoLevel, "not leader anymore, notify waiter")
		waiterWithTrace.Value <- ErrWrongLeaderErr
		close(waiterWithTrace.Value)
	}

	for _, waiterWithTrace := range queryWaiters {
		Log(sc.logContext(waiterWithTrace.Trace, RaftStateFlow), InfoLevel, "not leader anymore, notify waiter")
		waiterWithTrace.Value <- QueryWaiterResult{
			Err: ErrWrongLeaderErr,
		}
		close(waiterWithTrace.Value)
	}
}

func (sc *ShardCtrler) logContext(trace telemetry.Trace, flow Flow) LogContext {
	return LogContext{
		EndpointNamespace: serverNamespace,
		EndpointID:        sc.serverID,
		Flow:              flow,
		Trace:             &trace,
	}
}

func (sc *ShardCtrler) newTrace() telemetry.Trace {
	nextTraceID := sc.nextTraceID
	sc.nextTraceID++
	trace := telemetry.Trace{
		Namespace:  serverNamespace,
		EndpointID: sc.serverID,
		TraceID:    nextTraceID,
	}
	return trace
}

func (sc *ShardCtrler) lock(flow Flow) *Unlocker {
	sc.mu.Lock()
	nextLockID := sc.nextLockID
	sc.nextLockID++
	LogAndSkipCallers(
		LogContext{
			EndpointNamespace: serverNamespace,
			EndpointID:        sc.serverID,
			Flow:              flow,
		}, DebugLevel, 1, "lock(%v)", nextLockID)
	return &Unlocker{
		lockID:     nextLockID,
		unlockFunc: sc.unlock,
	}
}

func (sc *ShardCtrler) unlock(lockID uint64, flow Flow, skipCallers int) {
	LogAndSkipCallers(LogContext{
		EndpointNamespace: serverNamespace,
		EndpointID:        sc.serverID,
		Flow:              flow,
	}, DebugLevel, skipCallers+1, "unlock(%v)", lockID)
	sc.mu.Unlock()
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, serverID int, persister *raft.Persister) *ShardCtrler {
	labgob.Register(telemetry.WithTrace[Command]{})

	applyCh := make(chan raft.ApplyMsg)
	rf := raft.Make(servers, serverID, persister, applyCh)
	defaultTrace := telemetry.Trace{
		Namespace:  serverNamespace,
		EndpointID: serverID,
		TraceID:    0,
	}
	sc := &ShardCtrler{
		serverID:            serverID,
		rf:                  rf,
		applyCh:             applyCh,
		configs:             make([]Config, 0),
		serverCancelContext: newCancelContext(defaultTrace),
		serverWaitGroup:     new(sync.WaitGroup),
		taskTracker:         newTaskTracker(),
		joinWaiters:         make(map[Operation]telemetry.WithTrace[chan Err]),
		leaveWaiters:        make(map[Operation]telemetry.WithTrace[chan Err]),
		moveWaiters:         make(map[Operation]telemetry.WithTrace[chan Err]),
		queryWaiters:        make(map[Operation]telemetry.WithTrace[chan QueryWaiterResult]),
		joinOperations:      make(map[int]Operations[any]),
		leaveOperations:     make(map[int]Operations[any]),
		moveOperations:      make(map[int]Operations[any]),
		queryOperations:     make(map[int]Operations[Config]),
	}

	applyFinisher := sc.taskTracker.Add(sc.logContext(defaultTrace, InitFlow), 1)
	sc.serverWaitGroup.Add(1)
	go func() {
		defer sc.serverWaitGroup.Done()
		unlocker := sc.lock(InitFlow)
		logContext := sc.logContext(defaultTrace, InitFlow)
		defer applyFinisher.Done(logContext)
		unlocker.unlock(InitFlow)

		sc.applyCommand(defaultTrace)
	}()

	monitorFinisher := sc.taskTracker.Add(sc.logContext(defaultTrace, InitFlow), 1)
	sc.serverWaitGroup.Add(1)
	go func() {
		defer sc.serverWaitGroup.Done()
		unlocker := sc.lock(InitFlow)
		logContext := sc.logContext(defaultTrace, InitFlow)
		defer monitorFinisher.Done(logContext)
		unlocker.unlock(InitFlow)

		sc.monitorRaftState(defaultTrace)
	}()
	return sc
}
