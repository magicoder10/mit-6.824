package raft

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/rpc"
	"6.5840/signal"
	"6.5840/telemetry"
)

const heartBeatInterval = 120 * time.Millisecond
const electionBaseTimeOut = 400 * time.Millisecond
const electionRandomTimeOutFactor = 7
const electionRandomTimeOutMultiplier = 20 * time.Millisecond

const requestTimeOut = 120 * time.Millisecond

const traceNamespace = "raft"

type Role string

const (
	LeaderRole    Role = "Leader"
	CandidateRole Role = "Candidate"
	FollowerRole  Role = "Follower"
)

type LogEntry struct {
	Command interface{}
	Index   int
	Term    int
}

var _ fmt.Stringer = (*LogEntry)(nil)

func (l LogEntry) String() string {
	return fmt.Sprintf("[LogEntry Command:%v, Index:%v, Term:%v]", l.Command, l.Index, l.Term)
}

type Snapshot struct {
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

var _ fmt.Stringer = (*Snapshot)(nil)

func (s Snapshot) String() string {
	return fmt.Sprintf("[Snapshot LastIncludedIndex:%v, LastIncludedTerm:%v]", s.LastIncludedIndex, s.LastIncludedTerm)
}

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister

	// persistent state on all servers
	currentTerm telemetry.WithTrace[int]
	votedFor    telemetry.WithTrace[*int]
	logEntries  telemetry.WithTrace[[]LogEntry]
	snapshot    telemetry.WithTrace[Snapshot]

	// volatile state on all servers
	serverID             int // this peer's index into peers[]
	currentRole          telemetry.WithTrace[Role]
	receivedValidMessage telemetry.WithTrace[bool]
	commitIndex          telemetry.WithTrace[int]
	lastAppliedIndex     telemetry.WithTrace[int]

	// volatile state on leaders
	nextIndices  []telemetry.WithTrace[int]
	matchIndices []telemetry.WithTrace[int]

	applyingSnapshots telemetry.WithTrace[int]

	applyCh                   chan ApplyMsg
	onNewCommandIndexSignals  []*signal.Signal[telemetry.WithTrace[int]]
	onNewSnapshotSignal       *signal.Signal[telemetry.WithTrace[WithCancel[Snapshot]]]
	onMatchIndexChangeSignal  *signal.Signal[telemetry.WithTrace[int]]
	onCommitIndexChangeSignal *signal.Signal[telemetry.WithTrace[int]]

	applyEntriesCancelContext  *CancelContext
	applySnapshotCancelContext *CancelContext
	roleCancelContext          *CancelContext
	serverCancelContext        *CancelContext
	taskTracker                *TaskTracker
	serverWaitGroup            *sync.WaitGroup

	nextLockID  uint64
	nextTraceID uint64
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	unlocker := rf.lock(StateFlow)
	defer unlocker.unlock(StateFlow)
	return rf.currentTerm.Value, rf.currentRole.Value == LeaderRole
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(snapshotLastIncludedIndex int, snapshotData []byte) {
	unlocker := rf.lock(SnapshotFlow)
	defer unlocker.unlock(SnapshotFlow)
	trace := rf.newTrace()
	Log(rf.logContext(trace, SnapshotFlow), InfoLevel, "enter Snapshot: snapshotLastIncludedIndex=%v", snapshotLastIncludedIndex)
	defer Log(rf.logContext(trace, SnapshotFlow), InfoLevel, "exit Snapshot")

	if rf.serverCancelContext.IsCanceled(rf.logContext(trace, SnapshotFlow)) {
		Log(rf.logContext(trace, SnapshotFlow), InfoLevel, "server canceled, exit Snapshot")
		return
	}

	if snapshotLastIncludedIndex <= rf.snapshot.Value.LastIncludedIndex {
		Log(rf.logContext(trace, SnapshotFlow), InfoLevel, "snapshot already up to date, exit Snapshot")
		return
	}

	snapshot := Snapshot{
		LastIncludedIndex: snapshotLastIncludedIndex,
		LastIncludedTerm:  rf.logTerm(snapshotLastIncludedIndex),
		Data:              snapshotData,
	}

	rf.useSnapshot(trace, snapshot)
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	unlocker := rf.lock(ElectionFlow)
	defer unlocker.unlock(ElectionFlow)

	Log(
		rf.logContext(args.Trace, ElectionFlow),
		InfoLevel,
		"enter RequestVote: %+v",
		args)
	defer Log(
		rf.logContext(args.Trace, ElectionFlow),
		InfoLevel,
		"exit RequestVote: %+v", reply)
	if rf.serverCancelContext.IsCanceled(rf.logContext(args.Trace, ElectionFlow)) {
		reply.IsCanceled = true
		return
	}

	reply.Term = rf.currentTerm.Value
	if args.Term < rf.currentTerm.Value {
		Log(
			rf.logContext(args.Trace, ElectionFlow),
			InfoLevel,
			"reject stale vote request")
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm.Value {
		Log(
			rf.logContext(args.Trace, ElectionFlow),
			InfoLevel,
			"update current term to %v",
			args.Term)
		rf.currentTerm = telemetry.WithTrace[int]{
			Trace: args.Trace,
			Value: args.Term,
		}
		reply.Term = rf.currentTerm.Value
		rf.votedFor = telemetry.WithTrace[*int]{
			Trace: args.Trace,
			Value: nil,
		}
		rf.persist(args.Trace, ElectionFlow)

		if rf.currentRole.Value != FollowerRole {
			Log(
				rf.logContext(args.Trace, ElectionFlow),
				InfoLevel,
				"role changed to follower")
			rf.currentRole = telemetry.WithTrace[Role]{
				Trace: args.Trace,
				Value: FollowerRole,
			}
			rf.runAsFollower(args.Trace)
		}
	}

	lastLogTerm := rf.lastLogTerm()
	if args.LastLogTerm < lastLogTerm {
		Log(
			rf.logContext(args.Trace, ElectionFlow),
			InfoLevel,
			"reject stale vote request from %v",
			args.CandidateID)
		reply.VoteGranted = false
		return
	}

	if args.LastLogTerm == lastLogTerm {
		lastLogIndex := rf.lastLogIndex()
		if args.LastLogIndex < lastLogIndex {
			Log(
				rf.logContext(args.Trace, ElectionFlow),
				InfoLevel,
				"reject vote request from %v because of lower lastLogIndex: candidateLastLogIndex=%v, serverLastLogIndex=%v",
				args.CandidateID,
				args.LastLogIndex,
				lastLogIndex,
			)
			reply.VoteGranted = false
			return
		}
	}

	rf.receivedValidMessage = telemetry.WithTrace[bool]{
		Trace: args.Trace,
		Value: true,
	}
	Log(
		rf.logContext(args.Trace, ElectionFlow),
		InfoLevel,
		"received valid message from %v",
		args.CandidateID)
	if rf.votedFor.Value != nil && *rf.votedFor.Value != args.CandidateID {
		reply.VoteGranted = false
		return
	}

	reply.VoteGranted = true
	rf.votedFor = telemetry.WithTrace[*int]{
		Trace: args.Trace,
		Value: &args.CandidateID,
	}
	rf.persist(args.Trace, ElectionFlow)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	unlocker := rf.lock(LogReplicationFlow)
	defer unlocker.unlock(LogReplicationFlow)

	Log(
		rf.logContext(args.Trace, LogReplicationFlow),
		InfoLevel,
		"enter AppendEntries: args=%+v",
		args)
	defer Log(
		rf.logContext(args.Trace, LogReplicationFlow),
		InfoLevel,
		"exit AppendEntries: reply=%+v",
		reply)

	if rf.serverCancelContext.IsCanceled(rf.logContext(args.Trace, ElectionFlow)) {
		reply.IsCanceled = true
		return
	}

	reply.Term = rf.currentTerm.Value
	if args.Term < rf.currentTerm.Value {
		Log(
			rf.logContext(args.Trace, LogReplicationFlow),
			InfoLevel,
			"reject stale AppendEntries")
		reply.Success = false
		return
	}

	rf.receivedValidMessage = telemetry.WithTrace[bool]{
		Trace: args.Trace,
		Value: true,
	}
	Log(
		rf.logContext(args.Trace, LogReplicationFlow),
		InfoLevel,
		"received valid message")

	if args.Term > rf.currentTerm.Value {
		Log(
			rf.logContext(args.Trace, LogReplicationFlow),
			InfoLevel,
			"update current term to %v",
			args.Term)
		rf.currentTerm = telemetry.WithTrace[int]{
			Trace: args.Trace,
			Value: args.Term,
		}
		reply.Term = rf.currentTerm.Value
		rf.votedFor = telemetry.WithTrace[*int]{
			Trace: args.Trace,
			Value: nil,
		}
		rf.persist(args.Trace, LogReplicationFlow)
	}

	if rf.currentRole.Value != FollowerRole {
		Log(
			rf.logContext(args.Trace, LogReplicationFlow),
			InfoLevel,
			"role changed to follower")
		rf.currentRole = telemetry.WithTrace[Role]{
			Trace: args.Trace,
			Value: FollowerRole,
		}
		rf.runAsFollower(args.Trace)
	}

	Log(
		rf.logContext(args.Trace, LogReplicationFlow),
		InfoLevel,
		"before appendEntries from %v: commitIndex=%v, snapshot=%v, log=%v",
		args.LeaderID,
		rf.commitIndex,
		rf.snapshot,
		rf.logEntries)

	lastLogIndex := rf.lastLogIndex()
	if args.PrevLogIndex > lastLogIndex {
		reply.Success = false
		reply.ConflictLogLastIndex = lastLogIndex
		Log(
			rf.logContext(args.Trace, LogReplicationFlow),
			InfoLevel,
			"prevLogIndex not found, reject AppendEntries: %+v",
			reply)
		return
	}

	if rf.toRelativeIndex(args.PrevLogIndex) >= 0 {
		// prevLogIndex is in logEntries
		prevLogTerm := rf.logTerm(args.PrevLogIndex)
		if args.PrevLogTerm != prevLogTerm {
			reply.Success = false
			conflictTerm := prevLogTerm
			reply.ConflictTerm = &conflictTerm
			for index := 0; index < len(rf.logEntries.Value); index++ {
				if rf.logEntries.Value[index].Term == conflictTerm {
					reply.ConflictIndex = rf.toAbsoluteLogIndex(index)
					break
				}
			}

			Log(
				rf.logContext(args.Trace, LogReplicationFlow),
				InfoLevel,
				"prevLogTerm not match, reject AppendEntries: %+v", reply)
			return
		}
	}

	Log(rf.logContext(args.Trace, LogReplicationFlow), InfoLevel, "before check log entry: snapshot=%v, logEntries=%v",
		rf.snapshot,
		rf.logEntries)
	reply.Success = true
	remainNewEntries := make([]LogEntry, 0)
	for index, logEntry := range args.LogEntries {
		newEntryIndex := args.PrevLogIndex + index + 1
		newEntryRelativeIndex := rf.toRelativeIndex(newEntryIndex)
		if newEntryRelativeIndex < 0 {
			Log(rf.logContext(args.Trace, LogReplicationFlow), DebugLevel, "log entry already in snapshot, skipping: logEntry=%v, snapshot=%v",
				newEntryIndex, rf.snapshot)
			continue
		}

		if newEntryIndex > lastLogIndex {
			Log(
				rf.logContext(args.Trace, LogReplicationFlow),
				InfoLevel,
				"prepare to append new entries: %v",
				args.LogEntries[index:])
			remainNewEntries = args.LogEntries[index:]
			break
		}

		Log(rf.logContext(args.Trace, LogReplicationFlow), DebugLevel, "check log entry: logEntry=%v, newEntryRelativeIndex=%v, snapshot=%v",
			logEntry,
			newEntryRelativeIndex,
			rf.snapshot)
		if rf.logEntries.Value[newEntryRelativeIndex].Term != logEntry.Term {
			Log(
				rf.logContext(args.Trace, LogReplicationFlow),
				InfoLevel,
				"truncate log entries: %v",
				rf.logEntries.Value[newEntryRelativeIndex:])
			rf.logEntries = telemetry.WithTrace[[]LogEntry]{
				Trace: args.Trace,
				Value: rf.logEntries.Value[:newEntryRelativeIndex],
			}
			rf.persist(args.Trace, LogReplicationFlow)

			Log(
				rf.logContext(args.Trace, LogReplicationFlow),
				InfoLevel,
				"prepare to append new entries: %v",
				remainNewEntries)
			remainNewEntries = args.LogEntries[index:]
			break
		}
	}

	if len(remainNewEntries) > 0 {
		Log(
			rf.logContext(args.Trace, LogReplicationFlow),
			InfoLevel,
			"append new entries: %v",
			remainNewEntries)
		rf.logEntries = telemetry.WithTrace[[]LogEntry]{
			Trace: args.Trace,
			Value: append(rf.logEntries.Value, remainNewEntries...),
		}
		rf.persist(args.Trace, LogReplicationFlow)
	}

	Log(
		rf.logContext(args.Trace, LogReplicationFlow),
		InfoLevel,
		"after appendEntries from %v: commitIndex=%v, log=%v",
		args.LeaderID,
		rf.commitIndex,
		rf.logEntries)

	if args.LeaderCommitIndex > rf.commitIndex.Value {
		// entries after the last entry of appendEntries may not be committed on the leader
		// we need to take the minimum between leaderCommitIndex and last log index of appendEntries

		newEntriesLastIndex := args.PrevLogIndex + len(args.LogEntries)
		newCommitIndex := min(args.LeaderCommitIndex, newEntriesLastIndex)
		if newCommitIndex > rf.commitIndex.Value {
			rf.commitIndex = telemetry.WithTrace[int]{
				Trace: args.Trace,
				Value: newCommitIndex,
			}
			Log(
				rf.logContext(args.Trace, LogReplicationFlow),
				InfoLevel,
				"update commitIndex to %v",
				rf.commitIndex)
			rf.notifyCommitIndexChange(args.Trace, newCommitIndex)
		}
	} else {
		Log(
			rf.logContext(args.Trace, LogReplicationFlow),
			InfoLevel,
			"no change to commitIndex")
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	unlocker := rf.lock(SnapshotFlow)
	defer unlocker.unlock(SnapshotFlow)

	Log(
		rf.logContext(args.Trace, SnapshotFlow),
		InfoLevel,
		"enter InstallSnapshot: args=%+v",
		args)
	defer Log(
		rf.logContext(args.Trace, SnapshotFlow),
		InfoLevel,
		"exit InstallSnapshot: reply=%+v",
		reply)

	if rf.serverCancelContext.IsCanceled(rf.logContext(args.Trace, SnapshotFlow)) {
		reply.IsCanceled = true
		return
	}

	reply.Term = rf.currentTerm.Value
	if args.Term < rf.currentTerm.Value {
		Log(
			rf.logContext(args.Trace, SnapshotFlow),
			InfoLevel,
			"reject stale InstallSnapshot")
		return
	}

	rf.receivedValidMessage = telemetry.WithTrace[bool]{
		Trace: args.Trace,
		Value: true,
	}
	if args.Term > rf.currentTerm.Value {
		Log(
			rf.logContext(args.Trace, SnapshotFlow),
			InfoLevel,
			"update current term to %v",
			args.Term)
		rf.currentTerm = telemetry.WithTrace[int]{
			Trace: args.Trace,
			Value: args.Term,
		}
		reply.Term = rf.currentTerm.Value
		rf.votedFor = telemetry.WithTrace[*int]{
			Trace: args.Trace,
			Value: nil,
		}
		rf.persist(args.Trace, SnapshotFlow)
	}

	if rf.currentRole.Value != FollowerRole {
		Log(
			rf.logContext(args.Trace, SnapshotFlow),
			InfoLevel,
			"role changed to follower")
		rf.currentRole = telemetry.WithTrace[Role]{
			Trace: args.Trace,
			Value: FollowerRole,
		}
		rf.runAsFollower(args.Trace)
	}

	if args.LastIncludedIndex <= rf.snapshot.Value.LastIncludedIndex {
		Log(rf.logContext(args.Trace, SnapshotFlow), InfoLevel, "snapshot already up to date, exit Snapshot")
		return
	}

	snapshot := Snapshot{
		LastIncludedIndex: args.LastIncludedIndex,
		LastIncludedTerm:  args.LastIncludedTerm,
		Data:              args.Data,
	}
	rf.commitIndex = telemetry.WithTrace[int]{
		Trace: args.Trace,
		Value: snapshot.LastIncludedIndex,
	}
	rf.useSnapshot(args.Trace, snapshot)
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	unlocker := rf.lock(LogReplicationFlow)
	defer unlocker.unlock(LogReplicationFlow)

	trace := rf.newTrace()
	Log(rf.logContext(trace, LogReplicationFlow), InfoLevel, "enter Start")
	defer Log(rf.logContext(trace, LogReplicationFlow), InfoLevel, "exit Start")

	nextIndex := rf.toAbsoluteLogIndex(len(rf.logEntries.Value))
	if !rf.serverCancelContext.IsCanceled(rf.logContext(trace, LogReplicationFlow)) &&
		rf.currentRole.Value == LeaderRole {
		roleCancelContext := rf.roleCancelContext
		logEntry := LogEntry{
			Command: command,
			Index:   nextIndex,
			Term:    rf.currentTerm.Value,
		}
		rf.logEntries = telemetry.WithTrace[[]LogEntry]{
			Trace: trace,
			Value: append(rf.logEntries.Value, logEntry),
		}
		rf.persist(trace, LogReplicationFlow)
		Log(
			rf.logContext(trace, LogReplicationFlow),
			InfoLevel,
			"append new log entry: newEntry=%v logEntries=%v",
			logEntry,
			rf.logEntries)

		// start replicating log entries
		for peerServerID, sig := range rf.onNewCommandIndexSignals {
			if peerServerID == rf.serverID {
				continue
			}

			if roleCancelContext.IsCanceled(rf.logContext(trace, LogReplicationFlow)) {
				Log(rf.logContext(trace, LogReplicationFlow), InfoLevel, "role canceled, exit Start")
				break
			}

			sig.Send(telemetry.WithTrace[int]{
				Trace: trace,
				Value: nextIndex,
			})
		}
	}

	return nextIndex, rf.currentTerm.Value, rf.currentRole.Value == LeaderRole
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	unlocker := rf.lock(TerminationFlow)
	trace := rf.newTrace()
	Log(rf.logContext(trace, TerminationFlow), InfoLevel, "Kill enter")

	if rf.roleCancelContext != nil {
		rf.roleCancelContext.TryCancel(rf.logContext(trace, TerminationFlow))
	}

	rf.serverCancelContext.TryCancel(rf.logContext(trace, TerminationFlow))
	unlocker.unlock(TerminationFlow)

	rf.serverWaitGroup.Wait()

	unlocker = rf.lock(TerminationFlow)
	defer unlocker.unlock(TerminationFlow)

	Log(rf.logContext(trace, TerminationFlow), InfoLevel, "close onCommitIndexChangeCh")
	rf.onCommitIndexChangeSignal.Close()

	Log(rf.logContext(trace, TerminationFlow), InfoLevel, "close onNewSnapshotCh")
	rf.onNewSnapshotSignal.Close()

	Log(rf.logContext(trace, TerminationFlow), InfoLevel, "close onMatchIndexChangeCh")
	rf.onMatchIndexChangeSignal.Close()

	for peerServerID, sig := range rf.onNewCommandIndexSignals {
		if peerServerID == rf.serverID {
			continue
		}

		sig.Close()
	}

	Log(rf.logContext(trace, TerminationFlow), InfoLevel, "Kill exit")
}

func (rf *Raft) runAsFollower(trace telemetry.Trace) {
	Log(rf.logContext(trace, FollowerFlow), InfoLevel, "enter runAsFollower")
	defer Log(rf.logContext(trace, FollowerFlow), InfoLevel, "exit runAsFollower")

	prevRoleCancelContext := rf.roleCancelContext
	newRoleCancelContext := newCancelContext(trace)
	rf.roleCancelContext = newRoleCancelContext
	if prevRoleCancelContext != nil {
		prevRoleCancelContext.TryCancel(rf.logContext(trace, FollowerFlow))
	}

	finisher := rf.taskTracker.Add(rf.logContext(trace, FollowerFlow), 1)
	rf.serverWaitGroup.Add(1)
	go func() {
		defer rf.serverWaitGroup.Done()
		logUnlocker := rf.lock(FollowerFlow)
		defer finisher.Done(rf.logContext(trace, FollowerFlow))
		logUnlocker.unlock(FollowerFlow)

		for {
			electionTimerUnlocker := rf.lock(FollowerFlow)
			loopTrace := rf.newTrace()
			Log(rf.logContext(trace, FollowerFlow), InfoLevel, "[loop-trace:(%v)] start new election timer loop", loopTrace)
			if newRoleCancelContext.IsCanceled(rf.logContext(loopTrace, FollowerFlow)) {
				Log(rf.logContext(loopTrace, FollowerFlow), InfoLevel, "role canceled, exit runAsFollower")
				electionTimerUnlocker.unlock(FollowerFlow)
				return
			}

			rf.receivedValidMessage = telemetry.WithTrace[bool]{
				Trace: loopTrace,
				Value: false,
			}
			logContext := rf.logContext(loopTrace, FollowerFlow)
			Log(logContext, InfoLevel, "begin waiting for election timeout")
			electionTimerUnlocker.unlock(FollowerFlow)

			select {
			case <-time.After(newElectionTimeOut()):
			case <-newRoleCancelContext.OnCancel(logContext):
				logUnlocker = rf.lock(FollowerFlow)
				Log(rf.logContext(loopTrace, FollowerFlow), InfoLevel, "role canceled, exit runAsFollower")
				logUnlocker.unlock(FollowerFlow)
				return
			}

			electionTimerUnlocker = rf.lock(FollowerFlow)
			Log(rf.logContext(loopTrace, FollowerFlow), InfoLevel, "end waiting for election timeout")

			if newRoleCancelContext.IsCanceled(rf.logContext(loopTrace, FollowerFlow)) {
				Log(rf.logContext(loopTrace, FollowerFlow), InfoLevel, "role canceled, exit runAsFollower")
				electionTimerUnlocker.unlock(FollowerFlow)
				return
			}

			if !rf.receivedValidMessage.Value {
				Log(rf.logContext(loopTrace, FollowerFlow), InfoLevel, "no valid message received")
				rf.currentRole = telemetry.WithTrace[Role]{
					Trace: loopTrace,
					Value: CandidateRole,
				}
				rf.currentTerm = telemetry.WithTrace[int]{
					Trace: loopTrace,
					Value: rf.currentTerm.Value + 1,
				}
				rf.votedFor = telemetry.WithTrace[*int]{
					Trace: loopTrace,
					Value: nil,
				}
				rf.persist(loopTrace, FollowerFlow)

				rf.runAsCandidate(loopTrace)
				Log(rf.logContext(loopTrace, FollowerFlow), InfoLevel, "end election timer")
				electionTimerUnlocker.unlock(FollowerFlow)
				return
			}

			electionTimerUnlocker.unlock(FollowerFlow)
		}
	}()
}

func (rf *Raft) runAsCandidate(trace telemetry.Trace) {
	Log(rf.logContext(trace, CandidateFlow), InfoLevel, "enter runAsCandidate")
	defer Log(rf.logContext(trace, CandidateFlow), InfoLevel, "exit runAsCandidate")

	prevRoleCancelContext := rf.roleCancelContext
	newRoleCancelContext := newCancelContext(trace)
	rf.roleCancelContext = newRoleCancelContext

	prevRoleCancelContext.TryCancel(rf.logContext(trace, CandidateFlow))
	finisher := rf.taskTracker.Add(rf.logContext(trace, CandidateFlow), 1)
	rf.serverWaitGroup.Add(1)
	go func() {
		defer rf.serverWaitGroup.Done()
		unlocker := rf.lock(CandidateFlow)
		defer finisher.Done(rf.logContext(trace, CandidateFlow))

		Log(rf.logContext(trace, CandidateFlow), InfoLevel, "start election timer")
		unlocker.unlock(CandidateFlow)

		for {
			electionTimerUnlocker := rf.lock(CandidateFlow)
			loopTrace := rf.newTrace()
			Log(rf.logContext(loopTrace, CandidateFlow), InfoLevel, "[loop-trace:(%v)] start new election timer loop", loopTrace)

			if newRoleCancelContext.IsCanceled(rf.logContext(loopTrace, CandidateFlow)) {
				Log(rf.logContext(loopTrace, CandidateFlow), InfoLevel, "role canceled, exit runAsCandidate")
				electionTimerUnlocker.unlock(CandidateFlow)
				return
			}

			logContext := rf.logContext(loopTrace, CandidateFlow)
			electionCancelContext := newCancelContext(trace)
			electionTimerUnlocker.unlock(CandidateFlow)

			rf.beginElection(loopTrace, newRoleCancelContext, electionCancelContext)
			select {
			case <-time.After(newElectionTimeOut()):
			case <-newRoleCancelContext.OnCancel(logContext):
				electionCancelContext.TryCancel(logContext)
				logUnlocker := rf.lock(CandidateFlow)
				Log(rf.logContext(loopTrace, CandidateFlow), InfoLevel, "role canceled, exit runAsCandidate")
				logUnlocker.unlock(CandidateFlow)
				return
			}

			electionTimerUnlocker = rf.lock(CandidateFlow)
			if newRoleCancelContext.IsCanceled(rf.logContext(loopTrace, CandidateFlow)) {
				Log(rf.logContext(loopTrace, CandidateFlow), InfoLevel, "role canceled, exit runAsCandidate")
				electionTimerUnlocker.unlock(CandidateFlow)
				return
			}

			electionCancelContext.TryCancel(logContext)
			rf.currentTerm = telemetry.WithTrace[int]{
				Trace: loopTrace,
				Value: rf.currentTerm.Value + 1,
			}
			rf.votedFor = telemetry.WithTrace[*int]{
				Trace: loopTrace,
				Value: nil,
			}
			rf.persist(loopTrace, CandidateFlow)
			electionTimerUnlocker.unlock(CandidateFlow)
		}
	}()
}

func (rf *Raft) runAsLeader(trace telemetry.Trace) {
	Log(rf.logContext(trace, LeaderFlow), InfoLevel, "enter runAsLeader")
	defer Log(rf.logContext(trace, LeaderFlow), InfoLevel, "exit runAsLeader")

	prevRoleCancelContext := rf.roleCancelContext
	newRoleCancelContext := newCancelContext(trace)
	rf.roleCancelContext = newRoleCancelContext

	prevRoleCancelContext.TryCancel(rf.logContext(trace, LeaderFlow))
	finisher := rf.taskTracker.Add(rf.logContext(trace, LeaderFlow), 1)
	rf.serverWaitGroup.Add(1)
	go func() {
		defer rf.serverWaitGroup.Done()
		runAsLeaderUnlocker := rf.lock(LeaderFlow)
		defer finisher.Done(rf.logContext(trace, LeaderFlow))
		runAsLeaderUnlocker.unlock(LeaderFlow)

		runAsLeaderUnlocker = rf.lock(LeaderFlow)
		defer runAsLeaderUnlocker.unlock(LeaderFlow)
		if newRoleCancelContext.IsCanceled(rf.logContext(trace, LeaderFlow)) {
			Log(rf.logContext(trace, LeaderFlow), InfoLevel, "role canceled, exit runAsLeader")
			return
		}

		for peerServerID := 0; peerServerID < len(rf.peers); peerServerID++ {
			if peerServerID == rf.serverID {
				continue
			}

			rf.nextIndices[peerServerID] = telemetry.WithTrace[int]{
				Trace: trace,
				Value: rf.toAbsoluteLogIndex(len(rf.logEntries.Value)),
			}
			rf.matchIndices[peerServerID] = telemetry.WithTrace[int]{
				Trace: trace,
				Value: 0,
			}
			Log(rf.logContext(trace, LeaderFlow), InfoLevel, "initialize nextIndex and matchIndex for %v: nextIndex=%+v, matchIndex=%+v",
				peerServerID,
				rf.nextIndices[peerServerID],
				rf.matchIndices[peerServerID])
		}

		sendHeartbeatsFinisher := rf.taskTracker.Add(rf.logContext(trace, LeaderFlow), 1)
		rf.serverWaitGroup.Add(1)
		go func() {
			defer rf.serverWaitGroup.Done()
			logUnlocker := rf.lock(LeaderFlow)
			logContextForSendHeartbeats := rf.logContext(trace, LeaderFlow)
			defer sendHeartbeatsFinisher.Done(logContextForSendHeartbeats)
			logUnlocker.unlock(LeaderFlow)

			rf.sendHeartbeats(trace, newRoleCancelContext)
		}()

		replicateLogFinisher := rf.taskTracker.Add(rf.logContext(trace, LeaderFlow), 1)
		rf.serverWaitGroup.Add(1)
		go func() {
			defer rf.serverWaitGroup.Done()
			logUnlocker := rf.lock(LeaderFlow)
			defer replicateLogFinisher.Done(rf.logContext(trace, LeaderFlow))
			logUnlocker.unlock(LeaderFlow)

			rf.replicateLogToAllPeers(trace, newRoleCancelContext)
		}()

		updateCommitIndexFinisher := rf.taskTracker.Add(rf.logContext(trace, LeaderFlow), 1)
		rf.serverWaitGroup.Add(1)
		go func() {
			defer rf.serverWaitGroup.Done()
			logUnlocker := rf.lock(LeaderFlow)
			defer updateCommitIndexFinisher.Done(rf.logContext(trace, LeaderFlow))
			logUnlocker.unlock(LeaderFlow)

			rf.updateCommitIndex(trace, newRoleCancelContext)
		}()
	}()
}

func (rf *Raft) beginElection(trace telemetry.Trace, roleCancelContext *CancelContext, electionCancelContext *CancelContext) {
	unlocker := rf.lock(ElectionFlow)
	defer unlocker.unlock(ElectionFlow)
	Log(rf.logContext(trace, ElectionFlow), InfoLevel, "enter beginElection")
	defer Log(rf.logContext(trace, ElectionFlow), InfoLevel, "exit beginElection")

	totalServers := len(rf.peers)
	majorityServers := totalServers/2 + 1
	currentServerID := rf.serverID
	currentTerm := rf.currentTerm.Value
	lastLogIndex := rf.toAbsoluteLogIndex(len(rf.logEntries.Value) - 1)
	lastLogTerm := rf.lastLogTerm()
	rf.votedFor = telemetry.WithTrace[*int]{
		Trace: trace,
		Value: &currentServerID,
	}

	rf.persist(trace, ElectionFlow)

	totalResults := 1
	grantedVotes := 1
	voteMu := &sync.Mutex{}
	voteCond := sync.NewCond(voteMu)

	electionFinisher := rf.taskTracker.Add(rf.logContext(trace, ElectionFlow), 1)
	rf.serverWaitGroup.Add(1)
	go func() {
		defer rf.serverWaitGroup.Done()
		requestVotesUnlocker := rf.lock(ElectionFlow)
		defer electionFinisher.Done(rf.logContext(trace, ElectionFlow))
		Log(rf.logContext(trace, ElectionFlow), InfoLevel, "begin requesting votes")

		for peerServerID := 0; peerServerID < totalServers; peerServerID++ {
			if peerServerID == currentServerID {
				continue
			}

			peerTrace := rf.newTrace()
			Log(rf.logContext(trace, ElectionFlow), InfoLevel, "[peer-trace:(%v)] before request vote from %v", peerTrace, peerServerID)

			requestVoteFinisher := rf.taskTracker.Add(rf.logContext(peerTrace, ElectionFlow), 1)
			rf.serverWaitGroup.Add(1)
			go func(peerServerID int, peerTrace telemetry.Trace) {
				defer rf.serverWaitGroup.Done()
				requestUnlocker := rf.lock(ElectionFlow)
				defer requestVoteFinisher.Done(rf.logContext(peerTrace, ElectionFlow))
				defer voteCond.Signal()

				requestVoteArgs := &RequestVoteArgs{
					Trace:        peerTrace,
					Term:         currentTerm,
					CandidateID:  currentServerID,
					LastLogIndex: lastLogIndex,
					LastLogTerm:  lastLogTerm,
				}
				requestUnlocker.unlock(ElectionFlow)

				for {
					unlocker = rf.lock(ElectionFlow)
					if roleCancelContext.IsCanceled(rf.logContext(peerTrace, ElectionFlow)) {
						Log(rf.logContext(peerTrace, ElectionFlow), InfoLevel, "role canceled, exit requestVote from %v", peerServerID)
						unlocker.unlock(ElectionFlow)
						return
					}

					if electionCancelContext.IsCanceled(rf.logContext(peerTrace, ElectionFlow)) {
						Log(rf.logContext(peerTrace, ElectionFlow), InfoLevel, "election canceled, exit requestVote from %v", peerServerID)
						unlocker.unlock(ElectionFlow)
						return
					}

					logContext := rf.logContext(peerTrace, ElectionFlow)
					unlocker.unlock(ElectionFlow)

					rpcResultCh := make(chan rpc.Result[RequestVoteReply], 1)
					finisher := rf.taskTracker.Add(logContext, 1)
					go func() {
						defer finisher.Done(logContext)
						tmpReply := &RequestVoteReply{}
						succeed := rf.sendRequestVote(peerServerID, requestVoteArgs, tmpReply, ElectionFlow)
						rpcResultCh <- rpc.Result[RequestVoteReply]{
							Succeed: succeed,
							Reply:   tmpReply,
						}
					}()

					var rpcResult rpc.Result[RequestVoteReply]
					select {
					case rpcResult = <-rpcResultCh:
					case <-time.After(requestTimeOut):
						logUnlocker := rf.lock(ElectionFlow)
						Log(rf.logContext(peerTrace, ElectionFlow),
							InfoLevel,
							"request timeout, retry requestVote")
						logUnlocker.unlock(HeartbeatFlow)
						continue
					case <-electionCancelContext.OnCancel(logContext):
						logUnlocker := rf.lock(ElectionFlow)
						Log(rf.logContext(peerTrace, ElectionFlow),
							InfoLevel,
							"election canceled, exit requestVote")
						logUnlocker.unlock(ElectionFlow)
						return
					case <-roleCancelContext.OnCancel(logContext):
						logUnlocker := rf.lock(ElectionFlow)
						Log(rf.logContext(peerTrace, ElectionFlow),
							InfoLevel,
							"canceled, exit requestVote")
						logUnlocker.unlock(ElectionFlow)
						return
					}

					succeed := rpcResult.Succeed
					reply := rpcResult.Reply
					voteMu.Lock()
					peerUnlocker := rf.lock(ElectionFlow)
					if roleCancelContext.IsCanceled(rf.logContext(peerTrace, ElectionFlow)) {
						Log(
							rf.logContext(peerTrace, ElectionFlow),
							InfoLevel,
							"role canceled, exit requestVote")
						peerUnlocker.unlock(ElectionFlow)
						voteMu.Unlock()
						return
					}

					if electionCancelContext.IsCanceled(rf.logContext(peerTrace, ElectionFlow)) {
						Log(
							rf.logContext(peerTrace, ElectionFlow),
							InfoLevel,
							"election canceled, exit requestVote")
						peerUnlocker.unlock(ElectionFlow)
						voteMu.Unlock()
						return
					}

					if !succeed {
						Log(
							rf.logContext(peerTrace, ElectionFlow),
							InfoLevel,
							"failed to send vote request")
						peerUnlocker.unlock(ElectionFlow)
						voteMu.Unlock()
						continue
					}

					totalResults++
					if reply.IsCanceled {
						Log(
							rf.logContext(peerTrace, ElectionFlow),
							InfoLevel,
							"peer canceled, retry request vote from %v", peerServerID)
						peerUnlocker.unlock(ElectionFlow)
						voteMu.Unlock()
						continue
					}

					if reply.Term > rf.currentTerm.Value {
						Log(
							rf.logContext(peerTrace, ElectionFlow),
							InfoLevel,
							"role changed to follower, exit requestVote")
						rf.currentRole = telemetry.WithTrace[Role]{
							Trace: peerTrace,
							Value: FollowerRole,
						}
						rf.currentTerm = telemetry.WithTrace[int]{
							Trace: peerTrace,
							Value: reply.Term,
						}
						rf.votedFor = telemetry.WithTrace[*int]{
							Trace: peerTrace,
							Value: nil,
						}
						rf.persist(peerTrace, ElectionFlow)
						rf.runAsFollower(peerTrace)
						peerUnlocker.unlock(ElectionFlow)
						voteMu.Unlock()
						return
					}

					Log(
						rf.logContext(peerTrace, ElectionFlow),
						InfoLevel,
						"received vote, granted:%v",
						reply.VoteGranted)
					if reply.VoteGranted {
						grantedVotes++
					}

					peerUnlocker.unlock(ElectionFlow)
					voteMu.Unlock()
					return
				}
			}(peerServerID, peerTrace)
		}

		requestVotesUnlocker.unlock(ElectionFlow)

		voteMu.Lock()
		for totalResults < totalServers &&
			grantedVotes < majorityServers &&
			totalResults-grantedVotes < majorityServers {
			waitVotesUnlocker := rf.lock(ElectionFlow)
			if roleCancelContext.IsCanceled(rf.logContext(trace, ElectionFlow)) {
				Log(rf.logContext(trace, ElectionFlow), InfoLevel, "role canceled, exit beginElection")
				waitVotesUnlocker.unlock(ElectionFlow)
				voteMu.Unlock()
				return
			}

			if electionCancelContext.IsCanceled(rf.logContext(trace, ElectionFlow)) {
				Log(rf.logContext(trace, ElectionFlow), InfoLevel, "election canceled, exit beginElection")
				waitVotesUnlocker.unlock(ElectionFlow)
				voteMu.Unlock()
				return
			}

			waitVotesUnlocker.unlock(ElectionFlow)
			voteCond.Wait()
		}

		voteMu.Unlock()

		unlocker = rf.lock(ElectionFlow)
		defer unlocker.unlock(ElectionFlow)
		defer Log(rf.logContext(trace, ElectionFlow), InfoLevel, "finish requesting votes")

		if roleCancelContext.IsCanceled(rf.logContext(trace, ElectionFlow)) {
			Log(rf.logContext(trace, ElectionFlow), InfoLevel, "role canceled, exit beginElection")
			return
		}

		if electionCancelContext.IsCanceled(rf.logContext(trace, ElectionFlow)) {
			Log(rf.logContext(trace, ElectionFlow), InfoLevel, "election canceled, exit beginElection")
			return
		}

		if grantedVotes >= majorityServers {
			rf.currentRole = telemetry.WithTrace[Role]{
				Trace: trace,
				Value: LeaderRole,
			}
			rf.runAsLeader(trace)
			return
		} else {
			Log(rf.logContext(trace, ElectionFlow), InfoLevel, "not enough votes, wait for new election")
		}
	}()
}

func (rf *Raft) sendHeartbeats(trace telemetry.Trace, cancelContext *CancelContext) {
	unlocker := rf.lock(HeartbeatFlow)
	Log(rf.logContext(trace, HeartbeatFlow), InfoLevel, "enter sendHeartbeats")
	unlocker.unlock(HeartbeatFlow)

	for {
		sendHeartbeatsUnlocker := rf.lock(HeartbeatFlow)
		loopTrace := rf.newTrace()
		if cancelContext.IsCanceled(rf.logContext(loopTrace, HeartbeatFlow)) {
			Log(rf.logContext(loopTrace, HeartbeatFlow), InfoLevel, "canceled, exit sendHeartbeats")
			sendHeartbeatsUnlocker.unlock(HeartbeatFlow)
			return
		}

		currentTerm := rf.currentTerm.Value
		leaderID := rf.serverID
		leaderCommitIndex := rf.commitIndex.Value
		for peerServerID := 0; peerServerID < len(rf.peers); peerServerID++ {
			if peerServerID == rf.serverID {
				continue
			}

			peerTrace := rf.newTrace()
			Log(rf.logContext(loopTrace, HeartbeatFlow), InfoLevel, "[peer-trace:(%v)] before send heartbeat to %v",
				peerTrace,
				peerServerID)
			if cancelContext.IsCanceled(rf.logContext(peerTrace, HeartbeatFlow)) {
				Log(rf.logContext(peerTrace, HeartbeatFlow), InfoLevel, "canceled, exit sendHeartbeats")
				sendHeartbeatsUnlocker.unlock(HeartbeatFlow)
				return
			}

			Log(rf.logContext(loopTrace, HeartbeatFlow), InfoLevel, "send heartbeat to %v", peerServerID)

			sendHeartbeatFinisher := rf.taskTracker.Add(rf.logContext(peerTrace, HeartbeatFlow), 1)
			go func(peerTrace telemetry.Trace, peerServerID int) {
				peerUnlocker := rf.lock(HeartbeatFlow)
				defer sendHeartbeatFinisher.Done(rf.logContext(peerTrace, HeartbeatFlow))

				if cancelContext.IsCanceled(rf.logContext(peerTrace, HeartbeatFlow)) {
					Log(rf.logContext(peerTrace, HeartbeatFlow), InfoLevel, "canceled, exit sendHeartbeat")
					peerUnlocker.unlock(HeartbeatFlow)
					return
				}

				prevLogIndex := rf.matchIndices[peerServerID].Value
				prevLogTerm := rf.logTerm(prevLogIndex)
				args := &AppendEntriesArgs{
					Trace:             peerTrace,
					Term:              currentTerm,
					LeaderID:          leaderID,
					PrevLogIndex:      prevLogIndex,
					PrevLogTerm:       prevLogTerm,
					LogEntries:        nil,
					LeaderCommitIndex: leaderCommitIndex,
				}

				peerUnlocker.unlock(HeartbeatFlow)

				reply := &AppendEntriesReply{}
				succeed := rf.sendAppendEntries(peerServerID, args, reply, HeartbeatFlow)

				peerUnlocker = rf.lock(HeartbeatFlow)
				defer peerUnlocker.unlock(HeartbeatFlow)
				if cancelContext.IsCanceled(rf.logContext(peerTrace, HeartbeatFlow)) {
					Log(
						rf.logContext(peerTrace, HeartbeatFlow),
						InfoLevel,
						"canceled, exit sendHeartbeat")
					return
				}

				if !succeed {
					Log(
						rf.logContext(peerTrace, HeartbeatFlow),
						InfoLevel,
						"failed to send heartbeat")
					return
				}

				if reply.Term > rf.currentTerm.Value {
					Log(
						rf.logContext(peerTrace, HeartbeatFlow),
						InfoLevel, "role changed to follower, exit sendHeartbeat")
					rf.currentRole = telemetry.WithTrace[Role]{
						Trace: peerTrace,
						Value: FollowerRole,
					}
					rf.currentTerm = telemetry.WithTrace[int]{
						Trace: peerTrace,
						Value: reply.Term,
					}
					rf.votedFor = telemetry.WithTrace[*int]{
						Trace: peerTrace,
						Value: nil,
					}
					rf.persist(peerTrace, HeartbeatFlow)
					rf.runAsFollower(peerTrace)
					return
				}
			}(peerTrace, peerServerID)
		}

		logContext := rf.logContext(loopTrace, HeartbeatFlow)
		sendHeartbeatsUnlocker.unlock(HeartbeatFlow)

		select {
		case <-time.After(heartBeatInterval):
		case <-cancelContext.OnCancel(logContext):
			logUnlocker := rf.lock(HeartbeatFlow)
			Log(rf.logContext(loopTrace, HeartbeatFlow), InfoLevel, "canceled, exit sendHeartbeats")
			logUnlocker.unlock(HeartbeatFlow)
		}
	}
}

func (rf *Raft) replicateLogToAllPeers(trace telemetry.Trace, cancelContext *CancelContext) {
	replicateLogToPeersUnlocker := rf.lock(LogReplicationFlow)
	Log(rf.logContext(trace, LogReplicationFlow), InfoLevel, "enter replicateLogToAllPeers")
	replicateLogToPeersUnlocker.unlock(LogReplicationFlow)

	wg := sync.WaitGroup{}
	for peerServerID := 0; peerServerID < len(rf.peers); peerServerID++ {
		if peerServerID == rf.serverID {
			continue
		}

		replicateLogToPeerUnlocker := rf.lock(LogReplicationFlow)
		peerTrace := rf.newTrace()
		Log(rf.logContext(trace, LogReplicationFlow), InfoLevel, "[peer-trace:(%v)] before replicateLogToOnePeer for %v", peerTrace, peerServerID)
		if cancelContext.IsCanceled(rf.logContext(peerTrace, LogReplicationFlow)) {
			Log(rf.logContext(peerTrace, LogReplicationFlow), InfoLevel, "canceled, exit replicateLogToAllPeers")
			replicateLogToPeerUnlocker.unlock(LogReplicationFlow)
			return
		}

		replicateLogToPeerFinisher := rf.taskTracker.Add(rf.logContext(peerTrace, LogReplicationFlow), 1)
		replicateLogToPeerUnlocker.unlock(LogReplicationFlow)
		wg.Add(1)
		go func(peerServerID int) {
			defer wg.Done()
			logUnlocker := rf.lock(LogReplicationFlow)
			defer replicateLogToPeerFinisher.Done(rf.logContext(peerTrace, LogReplicationFlow))
			logUnlocker.unlock(LogReplicationFlow)

			rf.replicateLogToOnePeer(peerTrace, cancelContext, peerServerID)
		}(peerServerID)
	}

	wg.Wait()

	replicateLogToPeersUnlocker = rf.lock(LogReplicationFlow)
	Log(rf.logContext(trace, LogReplicationFlow), InfoLevel, "exit replicateLogToPeers")
	replicateLogToPeersUnlocker.unlock(LogReplicationFlow)
}

func (rf *Raft) replicateLogToOnePeer(trace telemetry.Trace, cancelContext *CancelContext, peerServerID int) {
	replicateLogToOnePeerUnlocker := rf.lock(LogReplicationFlow)
	Log(rf.logContext(trace, LogReplicationFlow), InfoLevel, "enter replicateLogToOnePeer %v", peerServerID)
	onNewCommandIndexCh := rf.onNewCommandIndexSignals[peerServerID].ReceiveChan()
	replicateLogToOnePeerUnlocker.unlock(LogReplicationFlow)

	for {
		replicateLogUnlocker := rf.lock(LogReplicationFlow)
		loopTrace := rf.newTrace()
		Log(rf.logContext(trace, LogReplicationFlow), InfoLevel, "[loop-trace:(%v)] start new replicateLogToOnePeer loop for %v", loopTrace, peerServerID)
		if cancelContext.IsCanceled(rf.logContext(loopTrace, LogReplicationFlow)) {
			Log(rf.logContext(loopTrace, LogReplicationFlow), InfoLevel, "canceled, exit replicateLogToOnePeer for %v", peerServerID)
			replicateLogUnlocker.unlock(LogReplicationFlow)
			return
		}

		lastEntryIndex := rf.lastLogIndex()
		Log(rf.logContext(loopTrace, LogReplicationFlow), InfoLevel, "lastEntryIndex=%v, matchIndex=%v, nextIndex=%v",
			lastEntryIndex,
			rf.matchIndices[peerServerID],
			rf.nextIndices[peerServerID])
		if rf.matchIndices[peerServerID].Value == lastEntryIndex {
			logContext := rf.logContext(loopTrace, LogReplicationFlow)
			replicateLogUnlocker.unlock(LogReplicationFlow)

			var commandIndexWithTrace telemetry.WithTrace[int]
			var ok bool
			select {
			case commandIndexWithTrace, ok = <-onNewCommandIndexCh:
				replicateLogUnlocker = rf.lock(LogReplicationFlow)
				logContext = rf.logContext(loopTrace, LogReplicationFlow)
				replicateLogUnlocker.unlock(LogReplicationFlow)
				if !ok {
					Log(logContext, InfoLevel, "onNewCommandIndexCh closed, exit replicateLogToOnePeer for %v", peerServerID)
					return
				}
			case <-cancelContext.OnCancel(logContext):
				replicateLogUnlocker = rf.lock(LogReplicationFlow)
				Log(rf.logContext(loopTrace, LogReplicationFlow), InfoLevel, "canceled, exit replicateLogToOnePeer for %v", peerServerID)
				replicateLogUnlocker.unlock(LogReplicationFlow)
				return
			}

			commandIndex := commandIndexWithTrace.Value
			replicateLogUnlocker = rf.lock(LogReplicationFlow)
			if cancelContext.IsCanceled(rf.logContext(commandIndexWithTrace.Trace, LogReplicationFlow)) {
				Log(rf.logContext(commandIndexWithTrace.Trace, LogReplicationFlow), InfoLevel, "canceled, exit replicateLogToOnePeer for %v", peerServerID)
				replicateLogUnlocker.unlock(LogReplicationFlow)
				return
			}

			Log(rf.logContext(loopTrace, LogReplicationFlow), InfoLevel, "[commandIndex-trace:(%v)] received new command index: commandIndex=%v",
				commandIndexWithTrace.Trace,
				commandIndex)
			lastEntryIndex = rf.lastLogIndex()
			if commandIndex < lastEntryIndex {
				Log(rf.logContext(loopTrace, LogReplicationFlow), InfoLevel, "command index outdated, skipping: peer=%v, commandIndex=%v, endOfLog=%v",
					peerServerID,
					commandIndex,
					lastEntryIndex)
				replicateLogUnlocker.unlock(LogReplicationFlow)
				continue
			}

			if commandIndex <= rf.matchIndices[peerServerID].Value {
				Log(rf.logContext(loopTrace, LogReplicationFlow), InfoLevel, "command index outdated, skipping: peer=%v, commandIndex=%v, matchIndex=%v",
					peerServerID,
					commandIndex,
					rf.matchIndices[peerServerID])
				replicateLogUnlocker.unlock(LogReplicationFlow)
				continue
			}
		}

		nextIndex := rf.nextIndices[peerServerID].Value
		Log(rf.logContext(loopTrace, LogReplicationFlow), InfoLevel, "replicate log to %v: nextIndex=%v, matchIndex=%v, lastEntryIndex=%v, snapshot=%v",
			peerServerID,
			nextIndex,
			rf.matchIndices[peerServerID],
			lastEntryIndex,
			rf.snapshot)
		if nextIndex <= rf.snapshot.Value.LastIncludedIndex {
			installSnapshotArgs := InstallSnapshotArgs{
				Trace:             loopTrace,
				Term:              rf.currentTerm.Value,
				LeaderID:          rf.serverID,
				LastIncludedIndex: rf.snapshot.Value.LastIncludedIndex,
				LastIncludedTerm:  rf.snapshot.Value.LastIncludedTerm,
				Data:              rf.snapshot.Value.Data,
			}
			logContext := rf.logContext(loopTrace, SnapshotFlow)
			replicateLogUnlocker.unlock(SnapshotFlow)

			rpcResultCh := make(chan rpc.Result[InstallSnapshotReply], 1)
			finisher := rf.taskTracker.Add(logContext, 1)
			go func() {
				defer finisher.Done(logContext)
				tmpReply := &InstallSnapshotReply{}
				succeed := rf.sendInstallSnapshot(peerServerID, &installSnapshotArgs, tmpReply, SnapshotFlow)
				rpcResultCh <- rpc.Result[InstallSnapshotReply]{
					Succeed: succeed,
					Reply:   tmpReply,
				}
			}()

			var rpcResult rpc.Result[InstallSnapshotReply]
			select {
			case rpcResult = <-rpcResultCh:
			case <-time.After(requestTimeOut):
				logUnlocker := rf.lock(SnapshotFlow)
				Log(
					rf.logContext(loopTrace, SnapshotFlow),
					InfoLevel,
					"request timeout, retrying")
				logUnlocker.unlock(SnapshotFlow)
				continue
			case <-cancelContext.OnCancel(logContext):
				logUnlocker := rf.lock(SnapshotFlow)
				Log(
					rf.logContext(loopTrace, SnapshotFlow),
					InfoLevel,
					"canceled, exit replicateLogToOnePeer")
				logUnlocker.unlock(SnapshotFlow)
				return
			}

			succeed := rpcResult.Succeed
			reply := rpcResult.Reply
			replicateLogUnlocker = rf.lock(SnapshotFlow)
			if cancelContext.IsCanceled(rf.logContext(loopTrace, SnapshotFlow)) {
				Log(rf.logContext(loopTrace, SnapshotFlow),
					InfoLevel,
					"canceled, exit replicateLogToOnePeer for %v",
					peerServerID)
				replicateLogUnlocker.unlock(SnapshotFlow)
				return
			}

			if !succeed {
				Log(rf.logContext(loopTrace, SnapshotFlow),
					InfoLevel,
					"failed to install snapshot to %v, retrying", peerServerID)
				replicateLogUnlocker.unlock(SnapshotFlow)
				continue
			}

			if reply.IsCanceled {
				Log(rf.logContext(loopTrace, SnapshotFlow),
					InfoLevel,
					"peer canceled, retry install snapshot to %v", peerServerID)
				replicateLogUnlocker.unlock(SnapshotFlow)
				continue
			}

			if reply.Term > rf.currentTerm.Value {
				Log(rf.logContext(loopTrace, SnapshotFlow),
					InfoLevel,
					"role changed to follower, exit replicateLogToOnePeer for %v",
					peerServerID)
				rf.currentRole = telemetry.WithTrace[Role]{
					Trace: loopTrace,
					Value: FollowerRole,
				}
				rf.currentTerm = telemetry.WithTrace[int]{
					Trace: loopTrace,
					Value: reply.Term,
				}
				rf.votedFor = telemetry.WithTrace[*int]{
					Trace: loopTrace,
					Value: nil,
				}
				rf.persist(loopTrace, SnapshotFlow)
				rf.runAsFollower(loopTrace)
				replicateLogUnlocker.unlock(SnapshotFlow)
				return
			}

			rf.nextIndices[peerServerID] = telemetry.WithTrace[int]{
				Trace: loopTrace,
				Value: rf.snapshot.Value.LastIncludedIndex + 1,
			}
			rf.matchIndices[peerServerID] = telemetry.WithTrace[int]{
				Trace: loopTrace,
				Value: rf.snapshot.Value.LastIncludedIndex,
			}
			Log(rf.logContext(loopTrace, SnapshotFlow),
				InfoLevel,
				"installed snapshot to %v: nextIndex=%v, matchIndex=%v",
				peerServerID,
				rf.nextIndices[peerServerID],
				rf.matchIndices[peerServerID])
			replicateLogUnlocker.unlock(SnapshotFlow)
			continue
		}

		prevLogIndex := nextIndex - 1
		prevLogTerm := rf.snapshot.Value.LastIncludedTerm
		if prevLogIndex > rf.snapshot.Value.LastIncludedIndex {
			prevLogTerm = rf.logEntries.Value[rf.toRelativeIndex(prevLogIndex)].Term
		}

		logEntries := rf.logEntries.Value[rf.toRelativeIndex(nextIndex):]
		logEntries = append([]LogEntry{}, logEntries...)
		args := &AppendEntriesArgs{
			Trace:             loopTrace,
			Term:              rf.currentTerm.Value,
			LeaderID:          rf.serverID,
			PrevLogIndex:      prevLogIndex,
			PrevLogTerm:       prevLogTerm,
			LogEntries:        logEntries,
			LeaderCommitIndex: rf.commitIndex.Value,
		}
		logContext := rf.logContext(loopTrace, LogReplicationFlow)
		replicateLogUnlocker.unlock(LogReplicationFlow)

		rpcResultCh := make(chan rpc.Result[AppendEntriesReply], 1)
		finisher := rf.taskTracker.Add(logContext, 1)
		go func() {
			defer finisher.Done(logContext)
			tmpReply := &AppendEntriesReply{}
			succeed := rf.sendAppendEntries(peerServerID, args, tmpReply, LogReplicationFlow)
			rpcResultCh <- rpc.Result[AppendEntriesReply]{
				Succeed: succeed,
				Reply:   tmpReply,
			}
		}()

		var rpcResult rpc.Result[AppendEntriesReply]
		select {
		case rpcResult = <-rpcResultCh:
		case <-time.After(requestTimeOut):
			logUnlocker := rf.lock(LogReplicationFlow)
			Log(rf.logContext(loopTrace, LogReplicationFlow),
				InfoLevel,
				"request timeout, retrying")
			logUnlocker.unlock(LogReplicationFlow)
			continue
		case <-cancelContext.OnCancel(logContext):
			logUnlocker := rf.lock(LogReplicationFlow)
			Log(rf.logContext(loopTrace, LogReplicationFlow),
				InfoLevel,
				"canceled, exit replicateLogToOnePeer")
			logUnlocker.unlock(LogReplicationFlow)
			return
		}

		succeed := rpcResult.Succeed
		reply := rpcResult.Reply
		replicateLogUnlocker = rf.lock(LogReplicationFlow)
		if cancelContext.IsCanceled(rf.logContext(loopTrace, LogReplicationFlow)) {
			Log(rf.logContext(loopTrace, LogReplicationFlow),
				InfoLevel,
				"canceled, exit replicateLogToOnePeer")
			replicateLogUnlocker.unlock(LogReplicationFlow)
			return
		}

		Log(rf.logContext(loopTrace, LogReplicationFlow),
			InfoLevel,
			"received RPC result: succeed=%v, reply=%+v",
			succeed,
			reply)

		if !succeed {
			Log(rf.logContext(loopTrace, LogReplicationFlow),
				InfoLevel,
				"failed to replicate log")
			replicateLogUnlocker.unlock(LogReplicationFlow)
			continue
		}

		if reply.IsCanceled {
			Log(rf.logContext(loopTrace, LogReplicationFlow),
				InfoLevel,
				"peer canceled, retry replicating log")
			replicateLogUnlocker.unlock(LogReplicationFlow)
			continue
		}

		if reply.Term > rf.currentTerm.Value {
			Log(rf.logContext(loopTrace, LogReplicationFlow),
				InfoLevel,
				"role changed, exit replicateLogToOnePeer")
			rf.currentRole = telemetry.WithTrace[Role]{
				Trace: loopTrace,
				Value: FollowerRole,
			}
			rf.currentTerm = telemetry.WithTrace[int]{
				Trace: loopTrace,
				Value: reply.Term,
			}
			rf.votedFor = telemetry.WithTrace[*int]{
				Trace: loopTrace,
				Value: nil,
			}
			rf.persist(loopTrace, LogReplicationFlow)
			rf.runAsFollower(loopTrace)
			replicateLogUnlocker.unlock(LogReplicationFlow)
			return
		}

		if reply.Success {
			rf.nextIndices[peerServerID] = telemetry.WithTrace[int]{
				Trace: loopTrace,
				Value: lastEntryIndex + 1,
			}
			rf.matchIndices[peerServerID] = telemetry.WithTrace[int]{
				Trace: loopTrace,
				Value: lastEntryIndex,
			}
			rf.onMatchIndexChangeSignal.Send(telemetry.WithTrace[int]{
				Trace: loopTrace,
				Value: lastEntryIndex,
			})
			Log(rf.logContext(loopTrace, LogReplicationFlow),
				InfoLevel,
				"replicated log to %v, nextIndex=%v, matchIndex=%v",
				peerServerID,
				rf.nextIndices[peerServerID],
				rf.matchIndices[peerServerID])
			replicateLogUnlocker.unlock(LogReplicationFlow)
			continue
		}

		Log(rf.logContext(loopTrace, LogReplicationFlow),
			InfoLevel,
			"backing off: %+v",
			reply)
		newNextIndex := rf.nextIndices[peerServerID].Value
		if reply.ConflictTerm == nil {
			newNextIndex = reply.ConflictLogLastIndex + 1
		} else {
			newNextIndex = reply.ConflictIndex
			for relativeIndex := rf.toRelativeIndex(len(rf.logEntries.Value)); relativeIndex >= 0; relativeIndex-- {
				if rf.logEntries.Value[relativeIndex].Term == *reply.ConflictTerm {
					newNextIndex = rf.toAbsoluteLogIndex(relativeIndex)
					break
				}
			}
		}

		rf.nextIndices[peerServerID] = telemetry.WithTrace[int]{
			Trace: loopTrace,
			Value: newNextIndex,
		}
		replicateLogUnlocker.unlock(LogReplicationFlow)
	}
}

func (rf *Raft) updateCommitIndex(trace telemetry.Trace, cancelContext *CancelContext) {
	updateCommitIndexUnlocker := rf.lock(CommitFlow)
	logContext := rf.logContext(trace, CommitFlow)
	Log(logContext, InfoLevel, "enter updateCommitIndex")
	onMatchIndexChangeCh := rf.onMatchIndexChangeSignal.ReceiveChan()
	updateCommitIndexUnlocker.unlock(CommitFlow)

	for {
		logUnlocker := rf.lock(CommitFlow)
		loopTrace := rf.newTrace()
		Log(rf.logContext(trace, CommitFlow), InfoLevel, "[loop-trace:(%v)] start new updateCommitIndex loop", loopTrace)
		if cancelContext.IsCanceled(rf.logContext(loopTrace, CommitFlow)) {
			Log(rf.logContext(loopTrace, CommitFlow), InfoLevel, "canceled, exit updateCommitIndex")
			logUnlocker.unlock(CommitFlow)
			return
		}

		logUnlocker.unlock(CommitFlow)

		var matchIndexWithTrace telemetry.WithTrace[int]
		var ok bool
		select {
		case matchIndexWithTrace, ok = <-onMatchIndexChangeCh:
			if !ok {
				logUnlocker = rf.lock(CommitFlow)
				Log(rf.logContext(loopTrace, CommitFlow), InfoLevel, "onMatchIndexChangeCh closed, exit updateCommitIndex")
				logUnlocker.unlock(CommitFlow)
				return
			}
		case <-cancelContext.OnCancel(logContext):
			logUnlocker = rf.lock(CommitFlow)
			Log(rf.logContext(loopTrace, CommitFlow), InfoLevel, "canceled, exit updateCommitIndex")
			logUnlocker.unlock(CommitFlow)
			return
		}

		matchIndex := matchIndexWithTrace.Value
		updateIndexUnlocker := rf.lock(CommitFlow)
		Log(rf.logContext(loopTrace, CommitFlow), InfoLevel, "[matchIndex-trace:%v] received matchIndex: matchIndex=%v, commitIndex=%v",
			matchIndexWithTrace.Trace,
			matchIndex,
			rf.commitIndex)
		if cancelContext.IsCanceled(rf.logContext(loopTrace, CommitFlow)) {
			Log(rf.logContext(loopTrace, CommitFlow), InfoLevel, "canceled, exit updateCommitIndex")
			updateIndexUnlocker.unlock(CommitFlow)
			return
		}

		if rf.applyingSnapshots.Value > 0 {
			Log(rf.logContext(loopTrace, CommitFlow), InfoLevel, "applying snapshots, skip checking matchIndex: matchIndex=%v, commitIndex=%v",
				matchIndex,
				rf.commitIndex)
			updateIndexUnlocker.unlock(CommitFlow)
			continue
		}

		Log(rf.logContext(loopTrace, CommitFlow), InfoLevel, "received matchIndex: matchIndex=%v, commitIndex=%v, snapshot=%v",
			matchIndex,
			rf.commitIndex,
			rf.snapshot)
		if matchIndex <= rf.commitIndex.Value {
			Log(rf.logContext(loopTrace, CommitFlow), InfoLevel, "matchIndex:%v <= commitIndex:%v, skip",
				matchIndex,
				rf.commitIndex)
			updateIndexUnlocker.unlock(CommitFlow)
			continue
		}

		rf.tryUpdateCommitIndex(loopTrace, matchIndex)
		updateIndexUnlocker.unlock(CommitFlow)
	}
}

func (rf *Raft) tryUpdateCommitIndex(trace telemetry.Trace, matchIndex int) {
	for index := matchIndex; index > rf.commitIndex.Value; index-- {
		Log(rf.logContext(trace, CommitFlow), InfoLevel, "try update commitIndex: index=%v, commitIndex=%v, relativeIndex=%v, snapshot=%v",
			index,
			rf.commitIndex,
			rf.toRelativeIndex(index),
			rf.snapshot)
		if rf.logEntries.Value[rf.toRelativeIndex(index)].Term != rf.currentTerm.Value {
			// only commit entries from current term
			continue
		}

		if rf.isReplicatedToMajority(index) {
			rf.commitIndex = telemetry.WithTrace[int]{
				Trace: trace,
				Value: index,
			}
			Log(rf.logContext(trace, CommitFlow), InfoLevel, "commitIndex updated to %v", rf.commitIndex)
			rf.notifyCommitIndexChange(trace, index)
			return
		}
	}

	return
}

func (rf *Raft) applyCommittedEntriesAndSnapshot(trace telemetry.Trace) {
	applyCommittedEntriesUnlocker := rf.lock(ApplyFlow)
	Log(rf.logContext(trace, ApplyFlow), InfoLevel, "enter applyCommittedEntriesAndSnapshot")
	onCommitIndexChangeSignalCh := rf.onCommitIndexChangeSignal.ReceiveChan()
	onNewSnapshotSignalCh := rf.onNewSnapshotSignal.ReceiveChan()
	applyCommittedEntriesUnlocker.unlock(ApplyFlow)

	for {
		applyUnlocker := rf.lock(ApplyFlow)

		loopTrace := rf.newTrace()
		Log(rf.logContext(trace, ApplyFlow), InfoLevel, "[loop-trace:(%v)] start new applyCommittedEntriesAndSnapshot loop", loopTrace)
		logContext := rf.logContext(loopTrace, ApplyFlow)
		applyUnlocker.unlock(ApplyFlow)

		var commitIndexWithTrace telemetry.WithTrace[int]
		var ok bool
		select {
		case commitIndexWithTrace, ok = <-onCommitIndexChangeSignalCh:
			logUnlocker := rf.lock(ApplyEntryFlow)
			if !ok {
				Log(rf.logContext(loopTrace, ApplyEntryFlow), InfoLevel, "onCommitIndexChangeCh closed, exit applyCommittedEntriesAndSnapshot")
				logUnlocker.unlock(ApplyEntryFlow)
				return
			}

			commitIndex := commitIndexWithTrace.Value
			Log(rf.logContext(loopTrace, ApplyEntryFlow), InfoLevel, "[commitIndex-trace:(%v)] received commitIndex: commitIndex=%v",
				commitIndexWithTrace.Trace,
				commitIndex)
			if rf.applyingSnapshots.Value > 0 {
				Log(rf.logContext(loopTrace, ApplyEntryFlow), InfoLevel, "applying snapshots, skip applying log entries: commitIndex=%v",
					commitIndex)
				logUnlocker.unlock(ApplyEntryFlow)
				continue
			}

			logUnlocker.unlock(ApplyEntryFlow)
			if !rf.applyCommittedLogEntries(loopTrace, commitIndex) {
				return
			}
		case snapshotWithTraceAndCancel, ok := <-onNewSnapshotSignalCh:
			if !ok {
				logUnlocker := rf.lock(SnapshotFlow)
				Log(rf.logContext(loopTrace, SnapshotFlow), InfoLevel, "onNewSnapshotSignalCh closed, exit applyCommittedEntriesAndSnapshot")
				logUnlocker.unlock(SnapshotFlow)
				return
			}

			snapshotWithCancel := snapshotWithTraceAndCancel.Value
			Log(rf.logContext(loopTrace, SnapshotFlow), InfoLevel, "[snapshot-trace:(%v)] received new snapshot: snapshot=%v",
				snapshotWithTraceAndCancel.Trace,
				snapshotWithCancel.Value)
			if !rf.applySnapshot(loopTrace, snapshotWithCancel.CancelContext, snapshotWithCancel.Value) {
				continue
			}

		case <-rf.serverCancelContext.OnCancel(logContext):
			logUnlocker := rf.lock(ApplyFlow)
			Log(rf.logContext(loopTrace, ApplyFlow), InfoLevel, "canceled, exit applyCommittedEntriesAndSnapshot")
			logUnlocker.unlock(ApplyFlow)
			return
		}
	}
}

func (rf *Raft) applyCommittedLogEntries(trace telemetry.Trace, commitIndex int) bool {
	applyUnlocker := rf.lock(ApplyEntryFlow)
	Log(rf.logContext(trace, ApplyEntryFlow), InfoLevel, "enter applyCommittedLogEntries: commitIndex=%v, snapshot=%v",
		commitIndex,
		rf.snapshot)
	defer Log(rf.logContext(trace, ApplyEntryFlow), InfoLevel, "exit applyCommittedLogEntries: commitIndex=%v",
		commitIndex)
	if rf.serverCancelContext.IsCanceled(rf.logContext(trace, ApplyEntryFlow)) {
		Log(rf.logContext(trace, ApplyEntryFlow), InfoLevel, "canceled, exit applyCommittedLogEntries")
		applyUnlocker.unlock(ApplyEntryFlow)
		return false
	}

	Log(rf.logContext(trace, ApplyEntryFlow), InfoLevel, "received commitIndex: commitIndex=%v, lastAppliedIndex=%v",
		commitIndex,
		rf.lastAppliedIndex)
	if commitIndex <= rf.lastAppliedIndex.Value {
		Log(rf.logContext(trace, ApplyEntryFlow), InfoLevel, "commitIndex <= lastAppliedIndex, skip")
		applyUnlocker.unlock(ApplyEntryFlow)
		return true
	}

	cancelContext := newCancelContext(trace)
	rf.applyEntriesCancelContext = cancelContext
	applyUnlocker.unlock(ApplyEntryFlow)
	defer func() {
		unlocker := rf.lock(ApplyEntryFlow)
		defer unlocker.unlock(ApplyEntryFlow)
		rf.applyEntriesCancelContext = nil
	}()

	for index := rf.lastAppliedIndex.Value + 1; index <= commitIndex; index++ {
		applyUnlocker = rf.lock(ApplyEntryFlow)
		if cancelContext.IsCanceled(rf.logContext(trace, ApplyEntryFlow)) {
			Log(rf.logContext(trace, ApplyEntryFlow), InfoLevel, "canceled, exit applyCommittedLogEntries: commitIndex=%v",
				commitIndex)
			applyUnlocker.unlock(ApplyEntryFlow)
			return true
		}

		if rf.serverCancelContext.IsCanceled(rf.logContext(trace, ApplyEntryFlow)) {
			Log(rf.logContext(trace, ApplyEntryFlow), InfoLevel, "canceled, exit applyCommittedLogEntries: commitIndex=%v",
				commitIndex)
			applyUnlocker.unlock(ApplyEntryFlow)
			return false
		}

		relativeIndex := rf.toRelativeIndex(index)
		Log(rf.logContext(trace, ApplyEntryFlow), InfoLevel, "before applying log entry: commitIndex=%v, lastAppliedIndex=%v, index=%v, relativeIndex=%v, snapshot=%v",
			commitIndex,
			rf.lastAppliedIndex,
			index,
			relativeIndex,
			rf.snapshot,
		)
		logEntry := rf.logEntries.Value[relativeIndex]
		logContext := rf.logContext(trace, ApplyEntryFlow)
		Log(logContext, InfoLevel, "applying log entry: commitIndex=%v, index=%v, logEntry=%v, logEntries=%v",
			commitIndex,
			index,
			logEntry,
			rf.logEntries)
		applyUnlocker.unlock(ApplyEntryFlow)

		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      logEntry.Command,
			CommandIndex: index,
		}

		select {
		case rf.applyCh <- applyMsg:
		case <-cancelContext.OnCancel(logContext):
			logUnlocker := rf.lock(ApplyEntryFlow)
			Log(rf.logContext(trace, ApplyEntryFlow), InfoLevel, "canceled, exit applyCommittedLogEntries: commitIndex=%v", commitIndex)
			logUnlocker.unlock(ApplyEntryFlow)
			return true
		case <-rf.serverCancelContext.OnCancel(logContext):
			logUnlocker := rf.lock(ApplyEntryFlow)
			Log(rf.logContext(trace, ApplyEntryFlow), InfoLevel, "role canceled, exit applyCommittedLogEntries: commitIndex=%v", commitIndex)
			logUnlocker.unlock(ApplyEntryFlow)
			return false
		}

		applyUnlocker = rf.lock(ApplyEntryFlow)
		rf.lastAppliedIndex = telemetry.WithTrace[int]{
			Trace: trace,
			Value: index,
		}

		if cancelContext.IsCanceled(rf.logContext(trace, ApplyEntryFlow)) {
			Log(rf.logContext(trace, ApplyEntryFlow), InfoLevel, "canceled, exit applyCommittedLogEntries: commitIndex=%v", commitIndex)
			applyUnlocker.unlock(ApplyEntryFlow)
			return true
		}

		if rf.serverCancelContext.IsCanceled(rf.logContext(trace, ApplyEntryFlow)) {
			Log(rf.logContext(trace, ApplyEntryFlow), InfoLevel, "canceled, exit applyCommittedLogEntries: commitIndex=%v", commitIndex)
			applyUnlocker.unlock(ApplyEntryFlow)
			return false
		}

		applyUnlocker.unlock(ApplyEntryFlow)
	}

	applyUnlocker = rf.lock(ApplyEntryFlow)
	Log(rf.logContext(trace, ApplyEntryFlow), InfoLevel, "finish applying log entries: commitIndex=%v, lastAppliedIndex=%v", rf.commitIndex, rf.lastAppliedIndex)
	applyUnlocker.unlock(ApplyEntryFlow)
	return true
}

func (rf *Raft) applySnapshot(trace telemetry.Trace, cancelContext *CancelContext, snapshot Snapshot) bool {
	applyUnlocker := rf.lock(SnapshotFlow)
	if cancelContext.IsCanceled(rf.logContext(trace, SnapshotFlow)) {
		rf.applyingSnapshots = telemetry.WithTrace[int]{
			Trace: trace,
			Value: rf.applyingSnapshots.Value - 1,
		}
		Log(rf.logContext(trace, SnapshotFlow), InfoLevel, "canceled, exit applySnapshot")
		applyUnlocker.unlock(SnapshotFlow)
		return true
	}

	if rf.serverCancelContext.IsCanceled(rf.logContext(trace, SnapshotFlow)) {
		rf.applyingSnapshots = telemetry.WithTrace[int]{
			Trace: trace,
			Value: rf.applyingSnapshots.Value - 1,
		}
		Log(rf.logContext(trace, SnapshotFlow), InfoLevel, "canceled, exit applySnapshot")
		applyUnlocker.unlock(SnapshotFlow)
		return false
	}

	Log(rf.logContext(trace, SnapshotFlow), InfoLevel, "received snapshot: %+v", snapshot)
	applyUnlocker.unlock(SnapshotFlow)
	applyMsg := ApplyMsg{
		SnapshotValid: true,
		SnapshotIndex: snapshot.LastIncludedIndex,
		Snapshot:      snapshot.Data,
	}

	select {
	case rf.applyCh <- applyMsg:
	case <-cancelContext.OnCancel(rf.logContext(trace, SnapshotFlow)):
		logUnlocker := rf.lock(SnapshotFlow)
		rf.applyingSnapshots = telemetry.WithTrace[int]{
			Trace: trace,
			Value: rf.applyingSnapshots.Value - 1,
		}
		Log(rf.logContext(trace, SnapshotFlow), InfoLevel, "canceled, exit applySnapshot")
		logUnlocker.unlock(SnapshotFlow)
		return true
	case <-rf.serverCancelContext.OnCancel(rf.logContext(trace, SnapshotFlow)):
		logUnlocker := rf.lock(SnapshotFlow)
		rf.applyingSnapshots = telemetry.WithTrace[int]{
			Trace: trace,
			Value: rf.applyingSnapshots.Value - 1,
		}
		Log(rf.logContext(trace, SnapshotFlow), InfoLevel, "canceled, exit applySnapshot")
		logUnlocker.unlock(SnapshotFlow)
		return false
	}

	applyUnlocker = rf.lock(SnapshotFlow)
	defer applyUnlocker.unlock(SnapshotFlow)
	rf.lastAppliedIndex = telemetry.WithTrace[int]{
		Trace: trace,
		Value: snapshot.LastIncludedIndex,
	}
	Log(rf.logContext(trace, SnapshotFlow), InfoLevel, "finish applying snapshot: lastAppliedIndex=%v",
		rf.lastAppliedIndex)

	defer func() {
		rf.applyingSnapshots = telemetry.WithTrace[int]{
			Trace: trace,
			Value: rf.applyingSnapshots.Value - 1,
		}
	}()

	if cancelContext.IsCanceled(rf.logContext(trace, SnapshotFlow)) {
		Log(rf.logContext(trace, SnapshotFlow), InfoLevel, "canceled, exit applySnapshot")
		return true
	}

	if rf.serverCancelContext.IsCanceled(rf.logContext(trace, SnapshotFlow)) {
		Log(rf.logContext(trace, SnapshotFlow), InfoLevel, "canceled, exit applySnapshot")
		return false
	}

	if rf.commitIndex.Value > rf.lastAppliedIndex.Value {
		Log(rf.logContext(trace, SnapshotFlow), InfoLevel, "apply remaining log entries after snapshot: lastAppliedIndex=%v, commitIndex=%v",
			rf.lastAppliedIndex,
			rf.commitIndex)
		rf.notifyCommitIndexChange(trace, rf.commitIndex.Value)
	}

	return true
}

func (rf *Raft) isReplicatedToMajority(index int) bool {
	majorityPeerCount := len(rf.peers)/2 + 1
	matchPeerCount := 1
	for peerServerID := 0; peerServerID < len(rf.peers); peerServerID++ {
		if peerServerID == rf.serverID {
			continue
		}

		if rf.matchIndices[peerServerID].Value >= index {
			matchPeerCount++
		}

		if matchPeerCount >= majorityPeerCount {
			return true
		}
	}

	return false
}

func (rf *Raft) useSnapshot(trace telemetry.Trace, snapshot Snapshot) {
	Log(rf.logContext(trace, SnapshotFlow), InfoLevel, "enter useSnapshot: snapshot=%v, applyEntriesCancelContext=%+v",
		snapshot,
		rf.applyEntriesCancelContext)

	if rf.applyEntriesCancelContext != nil {
		rf.applyEntriesCancelContext.TryCancel(rf.logContext(trace, SnapshotFlow))
		rf.applyEntriesCancelContext = nil
	}

	if rf.applySnapshotCancelContext != nil {
		rf.applySnapshotCancelContext.TryCancel(rf.logContext(trace, SnapshotFlow))
	}

	cancelContext := newCancelContext(trace)
	rf.applySnapshotCancelContext = cancelContext

	relativeIndex := rf.toRelativeIndex(snapshot.LastIncludedIndex)
	Log(rf.logContext(trace, SnapshotFlow), InfoLevel, "before discarding log entries: snapshotLastIncludedIndex=%v, relativeSnapshotLastIncludedIndex=%v, logEntries=%v",
		snapshot.LastIncludedIndex,
		relativeIndex,
		rf.logEntries)
	if relativeIndex < len(rf.logEntries.Value) {
		rf.logEntries = telemetry.WithTrace[[]LogEntry]{
			Trace: trace,
			Value: rf.logEntries.Value[relativeIndex+1:],
		}
	} else {
		rf.logEntries = telemetry.WithTrace[[]LogEntry]{
			Trace: trace,
			Value: nil,
		}
	}

	rf.snapshot = telemetry.WithTrace[Snapshot]{
		Trace: trace,
		Value: snapshot,
	}
	Log(rf.logContext(trace, SnapshotFlow), InfoLevel, "after discarding log entries: logEntries=%v", rf.logEntries)
	rf.persist(trace, SnapshotFlow)

	rf.applyingSnapshots = telemetry.WithTrace[int]{
		Trace: trace,
		Value: rf.applyingSnapshots.Value + 1,
	}
	rf.onNewSnapshotSignal.Send(telemetry.WithTrace[WithCancel[Snapshot]]{
		Trace: trace,
		Value: WithCancel[Snapshot]{
			CancelContext: cancelContext,
			Value:         snapshot,
		},
	})
	Log(rf.logContext(trace, SnapshotFlow), InfoLevel, "notified new snapshot to apply: snapshot=%v", rf.snapshot)
}

func (rf *Raft) logTerm(logIndex int) int {
	relativeIndex := rf.toRelativeIndex(logIndex)
	if relativeIndex < 0 {
		return rf.snapshot.Value.LastIncludedTerm
	}

	return rf.logEntries.Value[relativeIndex].Term
}

func (rf *Raft) lastLogIndex() int {
	if len(rf.logEntries.Value) == 0 {
		return rf.snapshot.Value.LastIncludedIndex
	}

	return rf.toAbsoluteLogIndex(len(rf.logEntries.Value) - 1)
}

func (rf *Raft) lastLogTerm() int {
	if len(rf.logEntries.Value) == 0 {
		return rf.snapshot.Value.LastIncludedTerm
	}

	return rf.logEntries.Value[len(rf.logEntries.Value)-1].Term
}

func (rf *Raft) toRelativeIndex(absoluteIndex int) int {
	return absoluteIndex - rf.snapshot.Value.LastIncludedIndex - 1
}

func (rf *Raft) toAbsoluteLogIndex(relativeIndex int) int {
	return rf.snapshot.Value.LastIncludedIndex + relativeIndex + 1
}

func (rf *Raft) notifyCommitIndexChange(trace telemetry.Trace, newCommitIndex int) {
	rf.onCommitIndexChangeSignal.Send(telemetry.WithTrace[int]{
		Trace: trace,
		Value: newCommitIndex,
	})
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist(trace telemetry.Trace, flow Flow) {
	Log(rf.logContext(trace, flow), InfoLevel, "persist enter")
	currentTerm := rf.currentTerm.Value
	voteFor := rf.votedFor.Value
	log := rf.logEntries.Value

	buf := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buf)
	err := encoder.Encode(rf.nextLockID)
	if err != nil {
		Log(rf.logContext(trace, flow), ErrorLevel, "failed to encode nextLockID: %v", err)
	}

	err = encoder.Encode(rf.nextTraceID)
	if err != nil {
		Log(rf.logContext(trace, flow), ErrorLevel, "failed to encode nextTraceID: %v", err)
	}

	err = encoder.Encode(currentTerm)
	if err != nil {
		Log(rf.logContext(trace, flow), ErrorLevel, "failed to encode currentTerm: %v", err)
	}

	var votedForPersist int
	if voteFor == nil {
		votedForPersist = -1
	} else {
		votedForPersist = *voteFor
	}

	err = encoder.Encode(votedForPersist)
	if err != nil {
		Log(rf.logContext(trace, flow), ErrorLevel, "failed to encode votedFor: %v", err)
	}

	err = encoder.Encode(log)
	if err != nil {
		Log(rf.logContext(trace, flow), ErrorLevel, "failed to encode log: %v", err)
	}

	err = encoder.Encode(rf.snapshot.Value.LastIncludedIndex)
	if err != nil {
		Log(rf.logContext(trace, flow), ErrorLevel, "failed to encode snapshotLastIncludedIndex: %v", err)
	}

	err = encoder.Encode(rf.snapshot.Value.LastIncludedTerm)
	if err != nil {
		Log(rf.logContext(trace, flow), ErrorLevel, "failed to encode snapshotLastIncludedTerm: %v", err)
	}

	data := buf.Bytes()
	rf.persister.Save(data, rf.snapshot.Value.Data)
	Log(rf.logContext(trace, flow), InfoLevel, "persist exit")
}

// restore previously persisted state.
func (rf *Raft) readPersist(trace telemetry.Trace, state []byte, snapshotData []byte, flow Flow) {
	Log(rf.logContext(trace, flow), InfoLevel, "readPersist enter")

	if state == nil || len(state) < 1 { // bootstrap without any state?
		Log(rf.logContext(trace, flow), InfoLevel, "readPersist exit")
		return
	}

	var nexLockID uint64
	var nextTraceID uint64
	var currentTerm int
	var votedFor *int
	var log []LogEntry

	buf := bytes.NewBuffer(state)
	decoder := labgob.NewDecoder(buf)
	err := decoder.Decode(&nexLockID)
	if err != nil {
		Log(rf.logContext(trace, flow), ErrorLevel, "failed to decode nextLockID: %v", err)
	}

	err = decoder.Decode(&nextTraceID)
	if err != nil {
		Log(rf.logContext(trace, flow), ErrorLevel, "failed to decode nextTraceID: %v", err)
	}

	err = decoder.Decode(&currentTerm)
	if err != nil {
		Log(rf.logContext(trace, flow), ErrorLevel, "failed to decode currentTerm: %v", err)
	}

	var votedForPersist int
	err = decoder.Decode(&votedForPersist)
	if err != nil {
		Log(rf.logContext(trace, flow), ErrorLevel, "failed to decode votedFor: %v", err)
	}

	if votedForPersist == -1 {
		votedFor = nil
	} else {
		votedFor = &votedForPersist
	}

	err = decoder.Decode(&log)
	if err != nil {
		Log(rf.logContext(trace, flow), ErrorLevel, "failed to decode logs: %v", err)
	}

	snapshot := Snapshot{
		Data: snapshotData,
	}
	err = decoder.Decode(&snapshot.LastIncludedIndex)
	if err != nil {
		Log(rf.logContext(trace, flow), ErrorLevel, "failed to decode snapshotLastIncludedIndex: %v", err)
	}

	err = decoder.Decode(&snapshot.LastIncludedTerm)
	if err != nil {
		Log(rf.logContext(trace, flow), ErrorLevel, "failed to decode snapshotLastIncludedTerm: %v", err)
	}

	rf.nextLockID = nexLockID
	rf.nextTraceID = nextTraceID
	rf.currentTerm = telemetry.WithTrace[int]{
		Trace: trace,
		Value: currentTerm,
	}
	rf.votedFor = telemetry.WithTrace[*int]{
		Trace: trace,
		Value: votedFor,
	}
	rf.logEntries = telemetry.WithTrace[[]LogEntry]{
		Trace: trace,
		Value: log,
	}
	rf.snapshot = telemetry.WithTrace[Snapshot]{
		Trace: trace,
		Value: snapshot,
	}
}

// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, flow Flow) bool {
	unlocker := rf.lock(flow)

	LogMessage(rf.messageContext(args.Trace, flow, rf.serverID, server), InfoLevel, "send RequestVote to %v: %+v", server, args)
	unlocker.unlock(flow)

	ok := rf.peers[server].Call("Raft.RequestVote", labrpc.MessageContext{
		Trace:      args.Trace,
		SenderID:   rf.serverID,
		ReceiverID: server,
	}, args, reply)

	unlocker = rf.lock(flow)
	defer unlocker.unlock(flow)
	if ok {
		LogMessage(rf.messageContext(args.Trace, flow, server, rf.serverID), InfoLevel, "receive RequestVote reply from %v: args=%+v", server, reply)
	} else {
		LogMessage(rf.messageContext(args.Trace, flow, server, rf.serverID), InfoLevel, "fail to receive RequestVote reply from %v: args=%+v", server, reply)
	}

	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, flow Flow) bool {
	unlocker := rf.lock(flow)
	LogMessage(rf.messageContext(args.Trace, flow, rf.serverID, server), InfoLevel, "send AppendEntries to %v: args=%+v", server, args)
	unlocker.unlock(flow)
	ok := rf.peers[server].Call("Raft.AppendEntries", labrpc.MessageContext{
		Trace:      args.Trace,
		SenderID:   rf.serverID,
		ReceiverID: server,
	}, args, reply)

	unlocker = rf.lock(flow)
	defer unlocker.unlock(flow)
	if ok {
		LogMessage(rf.messageContext(args.Trace, flow, server, rf.serverID), InfoLevel, "receive AppendEntries reply from %v: reply=%+v", server, reply)
	} else {
		LogMessage(rf.messageContext(args.Trace, flow, server, rf.serverID), InfoLevel, "fail to receive AppendEntries reply from %v: reply=%+v", server, reply)
	}

	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply, flow Flow) bool {
	unlocker := rf.lock(flow)
	LogMessage(rf.messageContext(args.Trace, flow, rf.serverID, server), InfoLevel, "send InstallSnapshot to %v: args=%+v", server, args)
	unlocker.unlock(flow)
	ok := rf.peers[server].Call("Raft.InstallSnapshot", labrpc.MessageContext{
		Trace:      args.Trace,
		SenderID:   rf.serverID,
		ReceiverID: server,
	}, args, reply)

	unlocker = rf.lock(flow)
	defer unlocker.unlock(flow)
	if ok {
		LogMessage(rf.messageContext(args.Trace, flow, server, rf.serverID), InfoLevel, "receive InstallSnapshot reply from %v: reply=%+v", server, reply)
	} else {
		LogMessage(rf.messageContext(args.Trace, flow, server, rf.serverID), InfoLevel, "fail to receive InstallSnapshot reply from %v: reply=%+v", server, reply)
	}

	return ok
}

func (rf *Raft) newTrace() telemetry.Trace {
	nextTraceID := rf.nextTraceID
	rf.nextTraceID++
	return telemetry.Trace{
		Namespace:  traceNamespace,
		EndpointID: rf.serverID,
		TraceID:    nextTraceID,
	}
}

func (rf *Raft) logContext(trace telemetry.Trace, flow Flow) LogContext {
	return LogContext{
		ServerID: rf.serverID,
		Role:     rf.currentRole.Value,
		Term:     rf.currentTerm.Value,
		Flow:     flow,
		Trace:    &trace,
	}
}

func (rf *Raft) messageContext(trace telemetry.Trace, flow Flow, senderID int, receiverID int) MessageContext {
	return MessageContext{
		LogContext: LogContext{
			ServerID: rf.serverID,
			Role:     rf.currentRole.Value,
			Term:     rf.currentTerm.Value,
			Flow:     flow,
			Trace:    &trace,
		},
		SenderID:   senderID,
		ReceiverID: receiverID,
	}
}

func (rf *Raft) lock(flow Flow) *Unlocker {
	rf.mu.Lock()
	nextLockID := rf.nextLockID
	rf.nextLockID++
	LogAndSkipCallers(
		LogContext{
			ServerID: rf.serverID,
			Role:     rf.currentRole.Value,
			Term:     rf.currentTerm.Value,
			Flow:     flow,
		}, DebugLevel, 1, "lock(%v)", nextLockID)
	return &Unlocker{
		lockID:     nextLockID,
		lockedAt:   time.Now(),
		unlockFunc: rf.unlock,
	}
}

func (rf *Raft) unlock(lockID uint64, flow Flow, lockDuration time.Duration, skipCallers int) {
	LogAndSkipCallers(LogContext{
		ServerID: rf.serverID,
		Role:     rf.currentRole.Value,
		Term:     rf.currentTerm.Value,
		Flow:     flow,
	}, DebugLevel, skipCallers+1, "unlock(%v): lockDuration=%v", lockID, lockDuration)
	rf.mu.Unlock()
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(
	peers []*labrpc.ClientEnd,
	me int,
	persister *Persister,
	applyCh chan ApplyMsg,
) *Raft {
	onNewCommandIndexSignals := make([]*signal.Signal[telemetry.WithTrace[int]], len(peers))
	for peerServerIndex := 0; peerServerIndex < len(peers); peerServerIndex++ {
		if peerServerIndex == me {
			continue
		}

		onNewCommandIndexSignals[peerServerIndex] = signal.NewSignal[telemetry.WithTrace[int]](1)
	}

	var nextCancelContextId uint64 = 0
	defaultTrace := telemetry.Trace{
		Namespace:  traceNamespace,
		EndpointID: me,
		TraceID:    0,
	}
	rf := &Raft{
		peers:     peers,
		persister: persister,
		serverID:  me,
		currentTerm: telemetry.WithTrace[int]{
			Trace: defaultTrace,
			Value: 0,
		},
		commitIndex: telemetry.WithTrace[int]{
			Trace: defaultTrace,
			Value: 0,
		},
		lastAppliedIndex: telemetry.WithTrace[int]{
			Trace: defaultTrace,
			Value: 0,
		},
		nextIndices:  make([]telemetry.WithTrace[int], len(peers)),
		matchIndices: make([]telemetry.WithTrace[int], len(peers)),
		applyingSnapshots: telemetry.WithTrace[int]{
			Trace: defaultTrace,
			Value: 0,
		},
		currentRole: telemetry.WithTrace[Role]{
			Trace: defaultTrace,
			Value: FollowerRole,
		},
		applyCh:                   applyCh,
		onNewCommandIndexSignals:  onNewCommandIndexSignals,
		onNewSnapshotSignal:       signal.NewSignal[telemetry.WithTrace[WithCancel[Snapshot]]](1),
		onMatchIndexChangeSignal:  signal.NewSignal[telemetry.WithTrace[int]](1),
		onCommitIndexChangeSignal: signal.NewSignal[telemetry.WithTrace[int]](1),
		taskTracker:               newTaskTracker(),
		serverWaitGroup:           &sync.WaitGroup{},
	}

	rf.readPersist(defaultTrace, persister.ReadRaftState(), persister.ReadSnapshot(), SharedFlow)

	trace := rf.newTrace()
	Log(rf.logContext(trace, SharedFlow), InfoLevel, "Make(%v) begin", rf.serverID)
	cancelContext := newCancelContext(trace)
	rf.serverCancelContext = cancelContext
	nextCancelContextId++

	// initialize from state persisted before a crash
	rf.commitIndex = telemetry.WithTrace[int]{
		Trace: trace,
		Value: rf.snapshot.Value.LastIncludedIndex,
	}

	finisher := rf.taskTracker.Add(rf.logContext(trace, SharedFlow), 1)
	rf.serverWaitGroup.Add(1)
	go func() {
		defer rf.serverWaitGroup.Done()
		unlocker := rf.lock(SharedFlow)
		finisher.Done(rf.logContext(trace, SharedFlow))
		unlocker.unlock(SharedFlow)

		rf.applyCommittedEntriesAndSnapshot(trace)
	}()

	if rf.snapshot.Value.LastIncludedIndex > 0 {
		unlocker := rf.lock(SharedFlow)
		rf.useSnapshot(trace, rf.snapshot.Value)
		unlocker.unlock(SharedFlow)
	}

	rf.runAsFollower(trace)
	unlocker := rf.lock(SharedFlow)
	defer unlocker.unlock(SharedFlow)
	Log(rf.logContext(trace, SharedFlow), InfoLevel, "Make(%v) end", rf.serverID)
	return rf
}

func newElectionTimeOut() time.Duration {
	return electionBaseTimeOut + time.Duration(rand.Intn(electionRandomTimeOutFactor))*electionRandomTimeOutMultiplier
}
