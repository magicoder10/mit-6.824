package raft

import (
	"bytes"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"time"

	"6.5840/labgob"
	//	"6.5840/labgob"
	"6.5840/labrpc"
)

const heartBeatInterval = 150 * time.Millisecond
const electionBaseTimeOut = 400 * time.Millisecond
const electionRandomTimeOutFactor = 7
const electionRandomTimeOutMultiplier = 20 * time.Millisecond

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

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Role string

const (
	LeaderRole    Role = "Leader"
	CandidateRole Role = "Candidate"
	FollowerRole  Role = "Follower"

	ServerRole Role = "Server"
)

type LogEntry struct {
	Command interface{}
	Index   int
	Term    int
}

func (l LogEntry) String() string {
	return fmt.Sprintf("[LogEntry Command:%v, Index:%v, Term:%v]", l.Command, l.Index, l.Term)
}

type RequestVoteArgs struct {
	MessageID    uint64
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	MessageID         uint64
	Term              int
	LeaderID          int
	PrevLogIndex      int
	PreLogTerm        int
	LogEntries        []LogEntry
	LeaderCommitIndex int
}

type AppendEntriesReply struct {
	Term                 int
	Success              bool
	HasPrevLogConflict   bool
	ConflictTerm         *int
	ConflictIndex        int
	ConflictLogLastIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister

	// persistent state on all servers
	currentTerm int
	votedFor    *int
	logEntries  []LogEntry

	// volatile state on all servers
	serverID             int // this peer's index into peers[]
	currentRole          Role
	receivedValidMessage bool
	commitIndex          int
	lastAppliedIndex     int

	// volatile state on leaders
	nextIndices  []int
	matchIndices []int

	applyCh               chan ApplyMsg
	onNewCommandIndexChs  []chan int
	onMatchIndexChangeCh  chan int
	onCommitIndexChangeCh chan int

	locks map[uint64]struct{}

	roleCancelContext   *CancelContext
	serverCancelContext *CancelContext

	nextLockID          uint64
	nextMessageID       uint64
	nextCancelContextID uint64
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	unlocker := rf.lock(StateFlow)
	defer unlocker.unlock(StateFlow)
	return rf.currentTerm, rf.currentRole == LeaderRole
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	unlocker := rf.lock(ElectionFlow)
	defer unlocker.unlock(ElectionFlow)

	LogMessage(
		rf.messageContext(ElectionFlow, args.CandidateID, rf.serverID, args.MessageID),
		InfoLevel,
		"enter RequestVote: %+v",
		args)
	defer LogMessage(
		rf.messageContext(ElectionFlow, args.CandidateID, rf.serverID, args.MessageID),
		InfoLevel,
		"exit RequestVote: %+v", reply)
	reply.Term = rf.currentTerm
	if rf.serverCancelContext.IsCanceled(rf.logContext(ElectionFlow)) {
		reply.VoteGranted = false
		return
	}

	if args.Term < rf.currentTerm {
		LogMessage(
			rf.messageContext(ElectionFlow, args.CandidateID, rf.serverID, args.MessageID),
			InfoLevel,
			"reject stale vote request")
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		LogMessage(
			rf.messageContext(ElectionFlow, args.CandidateID, rf.serverID, args.MessageID),
			InfoLevel,
			"update current term to %v",
			args.Term)
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		rf.votedFor = nil
		rf.persist(ElectionFlow)

		if rf.currentRole != FollowerRole {
			LogMessage(
				rf.messageContext(ElectionFlow, args.CandidateID, rf.serverID, args.MessageID),
				InfoLevel,
				"role changed to follower")
			rf.currentRole = FollowerRole
			rf.runAsFollower()
		}
	}

	lastLogTerm := 0
	if len(rf.logEntries) > 0 {
		lastLogTerm = rf.logEntries[len(rf.logEntries)-1].Term
	}

	if args.LastLogTerm < lastLogTerm {
		LogMessage(
			rf.messageContext(ElectionFlow, args.CandidateID, rf.serverID, args.MessageID),
			InfoLevel,
			"reject stale vote request from %v",
			args.CandidateID)
		reply.VoteGranted = false
		return
	}

	if args.LastLogTerm == lastLogTerm {
		if args.LastLogIndex < rf.toAbsoluteLogIndex(len(rf.logEntries)-1) {
			LogMessage(
				rf.messageContext(ElectionFlow, args.CandidateID, rf.serverID, args.MessageID),
				InfoLevel,
				"reject vote request from %v because of lower lastLogIndex",
				args.CandidateID)
			reply.VoteGranted = false
			return
		}
	}

	rf.receivedValidMessage = true
	LogMessage(
		rf.messageContext(ElectionFlow, args.CandidateID, rf.serverID, args.MessageID),
		InfoLevel,
		"received valid message from %v",
		args.CandidateID)
	if rf.votedFor != nil && *rf.votedFor != args.CandidateID {
		reply.VoteGranted = false
		return
	}

	reply.VoteGranted = true
	rf.votedFor = &args.CandidateID
	rf.persist(ElectionFlow)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	unlocker := rf.lock(LogReplicationFlow)
	defer unlocker.unlock(LogReplicationFlow)

	LogMessage(
		rf.messageContext(LogReplicationFlow, args.LeaderID, rf.serverID, args.MessageID),
		InfoLevel,
		"enter AppendEntries: args=%+v",
		args)
	defer LogMessage(
		rf.messageContext(LogReplicationFlow, args.LeaderID, rf.serverID, args.MessageID),
		InfoLevel,
		"exit AppendEntries: reply=%+v",
		reply)

	reply.Term = rf.currentTerm
	reply.HasPrevLogConflict = false
	if rf.serverCancelContext.IsCanceled(rf.logContext(ElectionFlow)) {
		reply.Success = false
		return
	}

	if args.Term < rf.currentTerm {
		LogMessage(
			rf.messageContext(LogReplicationFlow, args.LeaderID, rf.serverID, args.MessageID),
			InfoLevel,
			"reject stale AppendEntries")
		reply.Success = false
		return
	}

	rf.receivedValidMessage = true
	LogMessage(
		rf.messageContext(LogReplicationFlow, args.LeaderID, rf.serverID, args.MessageID),
		InfoLevel,
		"received valid message")

	if args.Term > rf.currentTerm {
		LogMessage(
			rf.messageContext(LogReplicationFlow, args.LeaderID, rf.serverID, args.MessageID),
			InfoLevel,
			"update current term to %v",
			args.Term)
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		rf.votedFor = nil
		rf.persist(LogReplicationFlow)
	}

	if rf.currentRole != FollowerRole {
		LogMessage(
			rf.messageContext(LogReplicationFlow, args.LeaderID, rf.serverID, args.MessageID),
			InfoLevel,
			"role changed to follower")
		rf.currentRole = FollowerRole
		rf.runAsFollower()
	}

	LogMessage(
		rf.messageContext(LogReplicationFlow, args.LeaderID, rf.serverID, args.MessageID),
		InfoLevel,
		"before appendEntries from %v: commitIndex=%v, log=%v",
		args.LeaderID,
		rf.commitIndex,
		rf.logEntries)
	defer LogMessage(
		rf.messageContext(LogReplicationFlow, args.LeaderID, rf.serverID, args.MessageID),
		InfoLevel,
		"after appendEntries from %v: commitIndex=%v, log=%v",
		args.LeaderID,
		rf.commitIndex,
		rf.logEntries)

	relativePrevLogIndex := rf.toRelativeLogIndex(args.PrevLogIndex)
	if relativePrevLogIndex > rf.toRelativeLogIndex(len(rf.logEntries)) {
		reply.Success = false
		reply.HasPrevLogConflict = true
		reply.ConflictLogLastIndex = rf.toAbsoluteLogIndex(len(rf.logEntries))
		LogMessage(
			rf.messageContext(LogReplicationFlow, args.LeaderID, rf.serverID, args.MessageID),
			InfoLevel,
			"prevLogIndex not found, reject AppendEntries: %+v",
			reply)
		return
	}

	if relativePrevLogIndex >= 0 {
		if args.PreLogTerm != rf.logEntries[relativePrevLogIndex].Term {
			reply.Success = false
			reply.HasPrevLogConflict = true
			conflictTerm := rf.logEntries[relativePrevLogIndex].Term
			reply.ConflictTerm = &conflictTerm
			for index := 0; index < len(rf.logEntries); index++ {
				if rf.logEntries[index].Term == conflictTerm {
					reply.ConflictIndex = rf.toAbsoluteLogIndex(index)
					break
				}
			}

			LogMessage(
				rf.messageContext(LogReplicationFlow, args.LeaderID, rf.serverID, args.MessageID),
				InfoLevel,
				"prevLogTerm not match, reject AppendEntries: %+v", reply)
			return
		}
	}

	reply.Success = true
	remainNewEntries := make([]LogEntry, 0)
	for index, logEntry := range args.LogEntries {
		originalIndex := relativePrevLogIndex + index + 1
		if originalIndex >= len(rf.logEntries) {
			LogMessage(
				rf.messageContext(LogReplicationFlow, args.LeaderID, rf.serverID, args.MessageID),
				InfoLevel,
				"prepare to append new entries: %v",
				args.LogEntries[index:])
			remainNewEntries = args.LogEntries[index:]
			break
		}

		if rf.logEntries[originalIndex].Term != logEntry.Term {
			LogMessage(
				rf.messageContext(LogReplicationFlow, args.LeaderID, rf.serverID, args.MessageID),
				InfoLevel,
				"truncate log entries: %v",
				rf.logEntries[originalIndex:])
			rf.logEntries = rf.logEntries[:originalIndex]
			rf.persist(LogReplicationFlow)

			LogMessage(
				rf.messageContext(LogReplicationFlow, args.LeaderID, rf.serverID, args.MessageID),
				InfoLevel,
				"prepare to append new entries: %v",
				remainNewEntries)
			remainNewEntries = args.LogEntries[index:]
			break
		}
	}

	if len(remainNewEntries) > 0 {
		LogMessage(
			rf.messageContext(LogReplicationFlow, args.LeaderID, rf.serverID, args.MessageID),
			InfoLevel,
			"append new entries: %v",
			remainNewEntries)
		rf.logEntries = append(rf.logEntries, remainNewEntries...)
		rf.persist(LogReplicationFlow)
	}

	if args.LeaderCommitIndex > rf.commitIndex {
		// entries after the last entry of appendEntries may not be committed on the leader
		// we need to take the minimum between leaderCommitIndex and last log index of appendEntries

		newCommitIndex := min(args.LeaderCommitIndex, args.PrevLogIndex+len(args.LogEntries))
		if newCommitIndex > rf.commitIndex {
			rf.commitIndex = newCommitIndex
			LogMessage(
				rf.messageContext(LogReplicationFlow, args.LeaderID, rf.serverID, args.MessageID),
				InfoLevel,
				"update commitIndex to %v",
				rf.commitIndex)

			finisher := rf.serverCancelContext.Add(rf.logContext(LogReplicationFlow), 1)
			go func() {
				logUnlocker := rf.lock(LogReplicationFlow)
				logContext := rf.logContext(LogReplicationFlow)
				logUnlocker.unlock(LogReplicationFlow)

				defer finisher.Done(logContext)
				select {
				case rf.onCommitIndexChangeCh <- newCommitIndex:
				case <-rf.serverCancelContext.OnCancel(logContext):
					logUnlocker = rf.lock(LogReplicationFlow)
					LogMessage(
						rf.messageContext(LogReplicationFlow, args.LeaderID, rf.serverID, args.MessageID),
						InfoLevel,
						"server canceled, exit AppendEntries")
					logUnlocker.unlock(LogReplicationFlow)
				}
			}()
		}
	} else {
		LogMessage(
			rf.messageContext(LogReplicationFlow, args.LeaderID, rf.serverID, args.MessageID),
			InfoLevel,
			"no change to commitIndex")
	}
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
	Log(rf.logContext(LogReplicationFlow), InfoLevel, "Start(%v)", command)

	Log(rf.logContext(LogReplicationFlow), InfoLevel, "enter Start")
	defer Log(rf.logContext(LogReplicationFlow), InfoLevel, "exit Start")

	if !rf.serverCancelContext.IsCanceled(rf.logContext(LogReplicationFlow)) && rf.currentRole == LeaderRole {
		roleCancelContext := rf.roleCancelContext
		nextIndex := len(rf.logEntries)
		logEntry := append(rf.logEntries, LogEntry{
			Command: command,
			Index:   nextIndex + 1,
			Term:    rf.currentTerm,
		})
		rf.logEntries = logEntry
		rf.persist(LogReplicationFlow)
		Log(rf.logContext(LogReplicationFlow), InfoLevel, "append new log entry: %v", logEntry)

		// start replicating log entries
		for _, ch := range rf.onNewCommandIndexChs {
			finisher := roleCancelContext.Add(rf.logContext(LogReplicationFlow), 1)
			serverFinisher := rf.serverCancelContext.Add(rf.logContext(LogReplicationFlow), 1)
			go func(ch chan int) {
				logUnlocker := rf.lock(LogReplicationFlow)
				logContext := rf.logContext(LogReplicationFlow)
				logUnlocker.unlock(LogReplicationFlow)

				defer finisher.Done(logContext)
				defer serverFinisher.Done(logContext)

				select {
				case ch <- nextIndex + 1:
				case <-roleCancelContext.OnCancel(logContext):
					logUnlocker = rf.lock(LogReplicationFlow)
					Log(rf.logContext(LogReplicationFlow), InfoLevel, "role canceled, exit Start")
					logUnlocker.unlock(LogReplicationFlow)
				}
			}(ch)
		}
	}

	return len(rf.logEntries), rf.currentTerm, rf.currentRole == LeaderRole
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
	Log(rf.logContext(TerminationFlow), InfoLevel, "Kill enter")

	if rf.roleCancelContext != nil {
		rf.roleCancelContext.TryCancel(rf.logContext(TerminationFlow))
	}

	rf.serverCancelContext.TryCancel(rf.logContext(TerminationFlow))
	unlocker.unlock(TerminationFlow)

	//rf.roleCancelContext.Wait(rf.logContext(TerminationFlow))
	rf.serverCancelContext.Wait(rf.logContext(TerminationFlow))

	unlocker = rf.lock(TerminationFlow)
	defer unlocker.unlock(TerminationFlow)

	Log(rf.logContext(TerminationFlow), InfoLevel, "close onCommitIndexChangeCh")
	close(rf.onCommitIndexChangeCh)

	Log(rf.logContext(TerminationFlow), InfoLevel, "close onMatchIndexChangeCh")
	close(rf.onMatchIndexChangeCh)

	for peerServerID, ch := range rf.onNewCommandIndexChs {
		if peerServerID == rf.serverID {
			continue
		}

		Log(rf.logContext(TerminationFlow), InfoLevel, "close onNewCommandIndexCh %v", peerServerID)
		close(ch)
	}

	Log(rf.logContext(TerminationFlow), InfoLevel, "Kill exit")
}

func (rf *Raft) runAsFollower() {
	Log(rf.logContext(FollowerFlow), InfoLevel, "enter runAsFollower")
	defer Log(rf.logContext(FollowerFlow), InfoLevel, "exit runAsFollower")

	prevRoleCancelContext := rf.roleCancelContext
	newRoleCancelContext := newCancelContext(rf.nextCancelContextID, string(FollowerRole))
	rf.nextCancelContextID++
	rf.roleCancelContext = newRoleCancelContext
	if prevRoleCancelContext != nil {
		prevRoleCancelContext.TryCancel(rf.logContext(FollowerFlow))
	}

	finisher := newRoleCancelContext.Add(rf.logContext(FollowerFlow), 1)
	go func() {
		logUnlocker := rf.lock(FollowerFlow)
		defer finisher.Done(rf.logContext(FollowerFlow))
		logUnlocker.unlock(FollowerFlow)

		for {
			electionTimerUnlocker := rf.lock(FollowerFlow)
			if newRoleCancelContext.IsCanceled(rf.logContext(FollowerFlow)) {
				Log(rf.logContext(FollowerFlow), InfoLevel, "role canceled, exit runAsFollower")
				electionTimerUnlocker.unlock(FollowerFlow)
				return
			}

			rf.receivedValidMessage = false
			logContext := rf.logContext(FollowerFlow)
			Log(logContext, InfoLevel, "begin waiting for election timeout")
			electionTimerUnlocker.unlock(FollowerFlow)

			select {
			case <-time.After(newElectionTimeOut()):
			case <-newRoleCancelContext.OnCancel(logContext):
				logUnlocker = rf.lock(FollowerFlow)
				Log(rf.logContext(FollowerFlow), InfoLevel, "role canceled, exit runAsFollower")
				logUnlocker.unlock(FollowerFlow)
				return
			}

			electionTimerUnlocker = rf.lock(FollowerFlow)
			Log(rf.logContext(FollowerFlow), InfoLevel, "end waiting for election timeout")

			if newRoleCancelContext.IsCanceled(rf.logContext(FollowerFlow)) {
				Log(rf.logContext(FollowerFlow), InfoLevel, "role canceled, exit runAsFollower")
				electionTimerUnlocker.unlock(FollowerFlow)
				return
			}

			if !rf.receivedValidMessage {
				Log(rf.logContext(FollowerFlow), InfoLevel, "no valid message received")
				rf.currentRole = CandidateRole

				rf.currentTerm++
				rf.votedFor = nil
				rf.persist(FollowerFlow)

				rf.runAsCandidate()
				Log(rf.logContext(FollowerFlow), InfoLevel, "end election timer")
				electionTimerUnlocker.unlock(FollowerFlow)
				return
			}

			electionTimerUnlocker.unlock(FollowerFlow)
		}
	}()
}

func (rf *Raft) runAsCandidate() {
	Log(rf.logContext(CandidateFlow), InfoLevel, "enter runAsCandidate")
	defer Log(rf.logContext(CandidateFlow), InfoLevel, "exit runAsCandidate")

	prevRoleCancelContext := rf.roleCancelContext
	newRoleCancelContext := newCancelContext(rf.nextCancelContextID, string(CandidateRole))
	rf.nextCancelContextID++
	rf.roleCancelContext = newRoleCancelContext

	prevRoleCancelContext.TryCancel(rf.logContext(CandidateFlow))
	finisher := newRoleCancelContext.Add(rf.logContext(CandidateFlow), 1)
	go func() {
		unlocker := rf.lock(CandidateFlow)
		defer finisher.Done(rf.logContext(CandidateFlow))

		Log(rf.logContext(CandidateFlow), InfoLevel, "start election timer")
		unlocker.unlock(CandidateFlow)

		for {
			electionTimerUnlocker := rf.lock(CandidateFlow)
			Log(rf.logContext(CandidateFlow), InfoLevel, "start new election timer")

			if newRoleCancelContext.IsCanceled(rf.logContext(CandidateFlow)) {
				Log(rf.logContext(CandidateFlow), InfoLevel, "role canceled, exit runAsCandidate")
				electionTimerUnlocker.unlock(CandidateFlow)
				return
			}

			logContext := rf.logContext(CandidateFlow)
			electionCancelContext := newCancelContext(rf.nextCancelContextID, string(ElectionFlow))
			electionTimerUnlocker.unlock(CandidateFlow)

			finishElectionCh := rf.beginElection(newRoleCancelContext, electionCancelContext)
			select {
			case <-time.After(newElectionTimeOut()):
			case <-newRoleCancelContext.OnCancel(logContext):
				electionCancelContext.TryCancel(logContext)
				logUnlocker := rf.lock(CandidateFlow)
				Log(rf.logContext(CandidateFlow), InfoLevel, "role canceled, exit runAsCandidate")
				logUnlocker.unlock(CandidateFlow)
				return
			case isElectedAsLeader := <-finishElectionCh:
				if isElectedAsLeader {
					Log(rf.logContext(CandidateFlow), InfoLevel, "elected as leader, exit runAsCandidate")
					return
				}
			}

			electionTimerUnlocker = rf.lock(CandidateFlow)
			if newRoleCancelContext.IsCanceled(rf.logContext(CandidateFlow)) {
				Log(rf.logContext(CandidateFlow), InfoLevel, "role canceled, exit runAsCandidate")
				electionTimerUnlocker.unlock(CandidateFlow)
				return
			}

			electionCancelContext.TryCancel(logContext)
			rf.currentTerm++
			rf.votedFor = nil
			rf.persist(CandidateFlow)

			electionTimerUnlocker.unlock(CandidateFlow)
		}
	}()
}

func (rf *Raft) runAsLeader() {
	Log(rf.logContext(LeaderFlow), InfoLevel, "enter runAsLeader")
	defer Log(rf.logContext(LeaderFlow), InfoLevel, "exit runAsLeader")

	prevRoleCancelContext := rf.roleCancelContext
	newRoleCancelContext := newCancelContext(rf.nextCancelContextID, string(LeaderRole))
	rf.nextCancelContextID++
	rf.roleCancelContext = newRoleCancelContext

	prevRoleCancelContext.TryCancel(rf.logContext(LeaderFlow))
	finisher := newRoleCancelContext.Add(rf.logContext(LeaderFlow), 1)
	go func() {
		runAsLeaderUnlocker := rf.lock(LeaderFlow)
		logContext := rf.logContext(LeaderFlow)
		defer finisher.Done(logContext)
		runAsLeaderUnlocker.unlock(LeaderFlow)

		runAsLeaderUnlocker = rf.lock(LeaderFlow)
		defer runAsLeaderUnlocker.unlock(LeaderFlow)
		if newRoleCancelContext.IsCanceled(rf.logContext(LeaderFlow)) {
			Log(rf.logContext(LeaderFlow), InfoLevel, "role canceled, exit runAsLeader")
			return
		}

		for peerServerID := 0; peerServerID < len(rf.peers); peerServerID++ {
			if peerServerID == rf.serverID {
				continue
			}

			rf.nextIndices[peerServerID] = len(rf.logEntries) + 1
			rf.matchIndices[peerServerID] = 0
		}

		sendHeartbeatsFinisher := newRoleCancelContext.Add(rf.logContext(LeaderFlow), 1)
		go func() {
			logUnlocker := rf.lock(LeaderFlow)
			logContextForSendHeartbeats := rf.logContext(LeaderFlow)
			logUnlocker.unlock(LeaderFlow)
			defer sendHeartbeatsFinisher.Done(logContextForSendHeartbeats)

			rf.sendHeartbeats(newRoleCancelContext)
		}()

		replicateLogFinisher := newRoleCancelContext.Add(rf.logContext(LeaderFlow), 1)
		go func() {
			logUnlocker := rf.lock(LeaderFlow)
			logContextForReplicateLog := rf.logContext(LeaderFlow)
			logUnlocker.unlock(LeaderFlow)
			defer replicateLogFinisher.Done(logContextForReplicateLog)

			rf.replicateLogToAllPeers(newRoleCancelContext)
		}()

		updateCommitIndexFinisher := newRoleCancelContext.Add(rf.logContext(LeaderFlow), 1)
		go func() {
			logUnlocker := rf.lock(LeaderFlow)
			logContextForUpdateCommitIndex := rf.logContext(LeaderFlow)
			logUnlocker.unlock(LeaderFlow)
			defer updateCommitIndexFinisher.Done(logContextForUpdateCommitIndex)

			rf.updateCommitIndex(newRoleCancelContext)
		}()
	}()
}

func (rf *Raft) beginElection(roleCancelContext *CancelContext, electionCancelContext *CancelContext) chan bool {
	electionFinish := make(chan bool)
	unlocker := rf.lock(ElectionFlow)
	defer unlocker.unlock(ElectionFlow)
	Log(rf.logContext(ElectionFlow), InfoLevel, "enter beginElection")
	defer Log(rf.logContext(ElectionFlow), InfoLevel, "exit beginElection")

	totalServers := len(rf.peers)
	majorityServers := totalServers/2 + 1
	currentServerID := rf.serverID
	lastLogIndex := len(rf.logEntries)
	currentTerm := rf.currentTerm
	lastLogTerm := 0
	if lastLogIndex > 0 {
		lastLogTerm = rf.logEntries[lastLogIndex-1].Term
	}

	rf.votedFor = &currentServerID
	rf.persist(ElectionFlow)

	totalVotes := 1
	grantedVotes := 1
	voteMu := &sync.Mutex{}
	voteCond := sync.NewCond(voteMu)

	electionFinisher := roleCancelContext.Add(rf.logContext(ElectionFlow), 1)
	go func() {
		requestVotesUnlocker := rf.lock(ElectionFlow)
		defer electionFinisher.Done(rf.logContext(ElectionFlow))
		Log(rf.logContext(ElectionFlow), InfoLevel, "begin requesting votes")

		for peerServerID := 0; peerServerID < totalServers; peerServerID++ {
			if peerServerID == currentServerID {
				continue
			}

			requestVoteFinisher := roleCancelContext.Add(rf.logContext(ElectionFlow), 1)
			go func(peerServerID int) {
				requestUnlocker := rf.lock(ElectionFlow)
				defer voteCond.Signal()
				defer requestVoteFinisher.Done(rf.logContext(ElectionFlow))
				messageID := rf.nextMessageID
				requestVoteArgs := &RequestVoteArgs{
					MessageID:    messageID,
					Term:         currentTerm,
					CandidateID:  currentServerID,
					LastLogIndex: lastLogIndex,
					LastLogTerm:  lastLogTerm,
				}
				requestUnlocker.unlock(ElectionFlow)

				for {
					unlocker = rf.lock(ElectionFlow)
					rf.nextMessageID++
					if roleCancelContext.IsCanceled(rf.logContext(ElectionFlow)) {
						Log(rf.logContext(ElectionFlow), InfoLevel, "role canceled, exit requestVote from %v", peerServerID)
						unlocker.unlock(ElectionFlow)
						return
					}

					if electionCancelContext.IsCanceled(rf.logContext(ElectionFlow)) {
						Log(rf.logContext(ElectionFlow), InfoLevel, "election canceled, exit requestVote from %v", peerServerID)
						unlocker.unlock(ElectionFlow)
						return
					}

					unlocker.unlock(ElectionFlow)

					reply := &RequestVoteReply{}
					succeed := rf.sendRequestVote(peerServerID, requestVoteArgs, reply, ElectionFlow)
					voteMu.Lock()
					peerUnlocker := rf.lock(ElectionFlow)
					if roleCancelContext.IsCanceled(rf.logContext(ElectionFlow)) {
						LogMessage(
							rf.messageContext(ElectionFlow, peerServerID, rf.serverID, messageID),
							InfoLevel,
							"role canceled, exit requestVote")
						peerUnlocker.unlock(ElectionFlow)
						voteMu.Unlock()
						return
					}

					if electionCancelContext.IsCanceled(rf.logContext(ElectionFlow)) {
						LogMessage(
							rf.messageContext(ElectionFlow, peerServerID, rf.serverID, messageID),
							InfoLevel,
							"election canceled, exit requestVote")
						peerUnlocker.unlock(ElectionFlow)
						voteMu.Unlock()
						return
					}

					if !succeed {
						LogMessage(
							rf.messageContext(ElectionFlow, peerServerID, rf.serverID, messageID),
							InfoLevel,
							"failed to send vote request")
						peerUnlocker.unlock(ElectionFlow)
						voteMu.Unlock()
						continue
					}

					totalVotes++
					if reply.Term > rf.currentTerm {
						LogMessage(
							rf.messageContext(ElectionFlow, peerServerID, rf.serverID, messageID),
							InfoLevel,
							"role changed to follower, exit requestVote")
						rf.currentRole = FollowerRole
						rf.currentTerm = reply.Term
						rf.votedFor = nil
						rf.persist(ElectionFlow)
						rf.runAsFollower()
						peerUnlocker.unlock(ElectionFlow)
						voteMu.Unlock()
						return
					}

					LogMessage(
						rf.messageContext(ElectionFlow, peerServerID, rf.serverID, messageID),
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
			}(peerServerID)
		}

		requestVotesUnlocker.unlock(ElectionFlow)

		voteMu.Lock()
		for totalVotes < totalServers &&
			grantedVotes < majorityServers &&
			totalVotes-grantedVotes < majorityServers {
			waitVotesUnlocker := rf.lock(ElectionFlow)
			if roleCancelContext.IsCanceled(rf.logContext(ElectionFlow)) {
				Log(rf.logContext(ElectionFlow), InfoLevel, "role canceled, exit beginElection")
				waitVotesUnlocker.unlock(ElectionFlow)
				voteMu.Unlock()
				return
			}

			if electionCancelContext.IsCanceled(rf.logContext(ElectionFlow)) {
				Log(rf.logContext(ElectionFlow), InfoLevel, "election canceled, exit beginElection")
				waitVotesUnlocker.unlock(ElectionFlow)
				voteMu.Unlock()
				return
			}

			waitVotesUnlocker.unlock(ElectionFlow)
			voteCond.Wait()
		}

		voteMu.Unlock()

		unlocker = rf.lock(ElectionFlow)
		defer Log(rf.logContext(ElectionFlow), InfoLevel, "finish requesting votes")

		if roleCancelContext.IsCanceled(rf.logContext(ElectionFlow)) {
			Log(rf.logContext(ElectionFlow), InfoLevel, "role canceled, exit beginElection")
			unlocker.unlock(ElectionFlow)
			return
		}

		if electionCancelContext.IsCanceled(rf.logContext(ElectionFlow)) {
			Log(rf.logContext(ElectionFlow), InfoLevel, "election canceled, exit beginElection")
			unlocker.unlock(ElectionFlow)
			return
		}

		if grantedVotes >= majorityServers {
			rf.currentRole = LeaderRole
			rf.runAsLeader()
			unlocker.unlock(ElectionFlow)
			electionFinish <- true
			return
		}

		unlocker.unlock(ElectionFlow)
		electionFinish <- false
	}()
	return electionFinish
}

func (rf *Raft) sendHeartbeats(cancelContext *CancelContext) {
	unlocker := rf.lock(HeartbeatFlow)
	Log(rf.logContext(HeartbeatFlow), InfoLevel, "enter sendHeartbeats")
	unlocker.unlock(HeartbeatFlow)

	for {
		sendHeartbeatsUnlocker := rf.lock(HeartbeatFlow)
		if cancelContext.IsCanceled(rf.logContext(HeartbeatFlow)) {
			Log(rf.logContext(HeartbeatFlow), InfoLevel, "canceled, exit sendHeartbeats")
			sendHeartbeatsUnlocker.unlock(HeartbeatFlow)
			return
		}

		currentTerm := rf.currentTerm
		leaderID := rf.serverID
		prevLogIndex := len(rf.logEntries)
		prevLogTerm := 0
		if prevLogIndex > 0 {
			prevLogTerm = rf.logEntries[prevLogIndex-1].Term
		}

		leaderCommitIndex := rf.commitIndex
		for peerServerID := 0; peerServerID < len(rf.peers); peerServerID++ {
			if peerServerID == rf.serverID {
				continue
			}

			if cancelContext.IsCanceled(rf.logContext(HeartbeatFlow)) {
				Log(rf.logContext(HeartbeatFlow), InfoLevel, "canceled, exit sendHeartbeats")
				sendHeartbeatsUnlocker.unlock(HeartbeatFlow)
				return
			}

			sendHeartbeatFinisher := cancelContext.Add(rf.logContext(HeartbeatFlow), 1)
			go func(peerServerID int) {
				peerUnlocker := rf.lock(HeartbeatFlow)
				defer sendHeartbeatFinisher.Done(rf.logContext(HeartbeatFlow))

				if cancelContext.IsCanceled(rf.logContext(HeartbeatFlow)) {
					Log(rf.logContext(HeartbeatFlow), InfoLevel, "canceled, exit sendHeartbeat")
					peerUnlocker.unlock(HeartbeatFlow)
					return
				}

				args := &AppendEntriesArgs{
					MessageID:         rf.nextMessageID,
					Term:              currentTerm,
					LeaderID:          leaderID,
					PrevLogIndex:      prevLogIndex,
					PreLogTerm:        prevLogTerm,
					LogEntries:        nil,
					LeaderCommitIndex: leaderCommitIndex,
				}
				rf.nextMessageID++
				peerUnlocker.unlock(HeartbeatFlow)

				reply := &AppendEntriesReply{}
				succeed := rf.sendAppendEntries(peerServerID, args, reply, HeartbeatFlow)

				peerUnlocker = rf.lock(HeartbeatFlow)
				defer peerUnlocker.unlock(HeartbeatFlow)
				if cancelContext.IsCanceled(rf.logContext(HeartbeatFlow)) {
					LogMessage(
						rf.messageContext(HeartbeatFlow, peerServerID, rf.serverID, args.MessageID),
						InfoLevel,
						"canceled, exit sendHeartbeat")
					return
				}

				if !succeed {
					LogMessage(rf.messageContext(HeartbeatFlow, peerServerID, rf.serverID, args.MessageID),
						InfoLevel,
						"failed to send heartbeat")
					return
				}

				if reply.Term > rf.currentTerm {
					LogMessage(
						rf.messageContext(HeartbeatFlow, peerServerID, rf.serverID, args.MessageID),
						InfoLevel, "role changed to follower, exit sendHeartbeat")
					rf.currentRole = FollowerRole
					rf.currentTerm = reply.Term
					rf.votedFor = nil
					rf.persist(HeartbeatFlow)
					rf.runAsFollower()
					return
				}
			}(peerServerID)
		}

		logContext := rf.logContext(HeartbeatFlow)
		sendHeartbeatsUnlocker.unlock(HeartbeatFlow)

		select {
		case <-time.After(heartBeatInterval):
		case <-cancelContext.OnCancel(logContext):
			Log(rf.logContext(HeartbeatFlow), InfoLevel, "canceled, exit sendHeartbeats")
		}
	}
}

func (rf *Raft) replicateLogToAllPeers(cancelContext *CancelContext) {
	replicateLogToPeersUnlocker := rf.lock(LogReplicationFlow)
	Log(rf.logContext(LogReplicationFlow), InfoLevel, "enter replicateLogToAllPeers")
	replicateLogToPeersUnlocker.unlock(LogReplicationFlow)

	wg := sync.WaitGroup{}
	for peerServerID := 0; peerServerID < len(rf.peers); peerServerID++ {
		if peerServerID == rf.serverID {
			continue
		}

		replicateLogToPeerUnlocker := rf.lock(LogReplicationFlow)
		if cancelContext.IsCanceled(rf.logContext(LogReplicationFlow)) {
			Log(rf.logContext(LogReplicationFlow), InfoLevel, "canceled, exit replicateLogToAllPeers")
			replicateLogToPeerUnlocker.unlock(LogReplicationFlow)
			return
		}

		replicateLogToPeerUnlocker.unlock(LogReplicationFlow)

		replicateLogToPeerFinisher := cancelContext.Add(rf.logContext(LogReplicationFlow), 1)
		wg.Add(1)
		go func(peerServerID int) {
			defer wg.Done()
			logUnlocker := rf.lock(LogReplicationFlow)
			defer replicateLogToPeerFinisher.Done(rf.logContext(LogReplicationFlow))
			logUnlocker.unlock(LogReplicationFlow)

			rf.replicateLogToOnePeer(cancelContext, peerServerID)
		}(peerServerID)
	}

	wg.Wait()

	replicateLogToPeersUnlocker = rf.lock(LogReplicationFlow)
	Log(rf.logContext(LogReplicationFlow), InfoLevel, "exit replicateLogToPeers")
	replicateLogToPeersUnlocker.unlock(LogReplicationFlow)
}

func (rf *Raft) replicateLogToOnePeer(cancelContext *CancelContext, peerServerID int) {
	replicateLogToOnePeerUnlocker := rf.lock(LogReplicationFlow)
	Log(rf.logContext(LogReplicationFlow), InfoLevel, "enter replicateLogToOnePeer %v", peerServerID)
	onNewCommandIndexCh := rf.onNewCommandIndexChs[peerServerID]
	replicateLogToOnePeerUnlocker.unlock(LogReplicationFlow)

	for {
		replicateLogUnlocker := rf.lock(LogReplicationFlow)
		if cancelContext.IsCanceled(rf.logContext(LogReplicationFlow)) {
			Log(rf.logContext(LogReplicationFlow), InfoLevel, "canceled, exit replicateLogToOnePeer")
			replicateLogUnlocker.unlock(LogReplicationFlow)
			return
		}

		lastEntryIndex := len(rf.logEntries)
		if rf.matchIndices[peerServerID] == lastEntryIndex {
			logContext := rf.logContext(LogReplicationFlow)
			replicateLogUnlocker.unlock(LogReplicationFlow)

			var commandIndex int
			var ok bool
			select {
			case commandIndex, ok = <-onNewCommandIndexCh:
				replicateLogUnlocker = rf.lock(LogReplicationFlow)
				logContext = rf.logContext(LogReplicationFlow)
				replicateLogUnlocker.unlock(LogReplicationFlow)

				if !ok {
					Log(logContext, InfoLevel, "onNewCommandIndexCh closed, exit replicateLogToOnePeer")
					return
				}
			case <-cancelContext.OnCancel(logContext):
				replicateLogUnlocker = rf.lock(LogReplicationFlow)
				Log(rf.logContext(LogReplicationFlow), InfoLevel, "canceled, exit replicateLogToOnePeer")
				replicateLogUnlocker.unlock(LogReplicationFlow)
				return
			}

			replicateLogUnlocker = rf.lock(LogReplicationFlow)
			if cancelContext.IsCanceled(rf.logContext(LogReplicationFlow)) {
				Log(rf.logContext(LogReplicationFlow), InfoLevel, "canceled, exit replicateLogToOnePeer")
				replicateLogUnlocker.unlock(LogReplicationFlow)
				return
			}

			lastEntryIndex = len(rf.logEntries)
			if commandIndex < lastEntryIndex {
				Log(rf.logContext(LogReplicationFlow), InfoLevel, "command index outdated, skipping: peer:%v, commandIndex=%v, endOfLog=%v",
					peerServerID,
					commandIndex,
					lastEntryIndex)
				replicateLogUnlocker.unlock(LogReplicationFlow)
				continue
			}
		}

		nextIndex := rf.nextIndices[peerServerID]
		prevLogIndex := nextIndex - 1
		prevLogTerm := 0
		if prevLogIndex > 0 {
			prevLogTerm = rf.logEntries[rf.toRelativeLogIndex(prevLogIndex)].Term
		}

		logEntries := rf.logEntries[rf.toRelativeLogIndex(nextIndex):]
		messageID := rf.nextMessageID
		args := &AppendEntriesArgs{
			MessageID:         messageID,
			Term:              rf.currentTerm,
			LeaderID:          rf.serverID,
			PrevLogIndex:      prevLogIndex,
			PreLogTerm:        prevLogTerm,
			LogEntries:        logEntries,
			LeaderCommitIndex: rf.commitIndex,
		}
		rf.nextMessageID++
		replicateLogUnlocker.unlock(LogReplicationFlow)
		reply := &AppendEntriesReply{}
		succeed := rf.sendAppendEntries(peerServerID, args, reply, LogReplicationFlow)

		replicateLogUnlocker = rf.lock(LogReplicationFlow)
		if cancelContext.IsCanceled(rf.logContext(LogReplicationFlow)) {
			LogMessage(
				rf.messageContext(LogReplicationFlow, peerServerID, rf.serverID, messageID),
				InfoLevel,
				"canceled, exit replicateLogToOnePeer")
			replicateLogUnlocker.unlock(LogReplicationFlow)
			return
		}

		if !succeed {
			LogMessage(
				rf.messageContext(LogReplicationFlow, peerServerID, rf.serverID, messageID),
				InfoLevel,
				"failed to replicate log")
			replicateLogUnlocker.unlock(LogReplicationFlow)
			continue
		}

		if reply.Term > rf.currentTerm {
			LogMessage(
				rf.messageContext(LogReplicationFlow, peerServerID, rf.serverID, messageID),
				InfoLevel,
				"role changed, exit replicateLogToOnePeer")
			rf.currentRole = FollowerRole
			rf.currentTerm = reply.Term
			rf.votedFor = nil
			rf.persist(LogReplicationFlow)
			rf.runAsFollower()
			replicateLogUnlocker.unlock(LogReplicationFlow)
			return
		}

		if reply.Success {
			rf.nextIndices[peerServerID] = lastEntryIndex + 1
			rf.matchIndices[peerServerID] = lastEntryIndex

			finisher := cancelContext.Add(rf.logContext(LogReplicationFlow), 1)
			serverFinisher := rf.serverCancelContext.Add(rf.logContext(LogReplicationFlow), 1)
			go func(lastEntryIndex int) {
				logUnlocker := rf.lock(LogReplicationFlow)
				logContext := rf.logContext(LogReplicationFlow)
				defer serverFinisher.Done(logContext)
				defer finisher.Done(logContext)
				logUnlocker.unlock(LogReplicationFlow)

				select {
				case rf.onMatchIndexChangeCh <- lastEntryIndex:
				case <-cancelContext.OnCancel(logContext):
					logUnlocker = rf.lock(LogReplicationFlow)
					LogMessage(
						rf.messageContext(LogReplicationFlow, peerServerID, rf.serverID, messageID),
						InfoLevel,
						"role canceled, exit replicateLogToOnePeer")
					logUnlocker.unlock(LogReplicationFlow)
				}
			}(lastEntryIndex)
			replicateLogUnlocker.unlock(LogReplicationFlow)
			continue
		}

		if !reply.HasPrevLogConflict {
			LogMessage(
				rf.messageContext(LogReplicationFlow, peerServerID, rf.serverID, messageID),
				InfoLevel,
				"previous log entries matches, retry replicating log to %v",
				peerServerID)
			replicateLogUnlocker.unlock(LogReplicationFlow)
			continue
		}

		LogMessage(
			rf.messageContext(LogReplicationFlow, peerServerID, rf.serverID, messageID),
			InfoLevel,
			"backing off: %+v",
			reply)
		newNextIndex := rf.nextIndices[peerServerID]
		if reply.ConflictTerm == nil {
			newNextIndex = reply.ConflictLogLastIndex
		} else {
			newNextIndex = reply.ConflictIndex
			for relativeIndex := rf.toRelativeLogIndex(len(rf.logEntries)); relativeIndex >= 0; relativeIndex-- {
				if rf.logEntries[relativeIndex].Term == *reply.ConflictTerm {
					newNextIndex = rf.toAbsoluteLogIndex(relativeIndex)
					break
				}
			}
		}

		rf.nextIndices[peerServerID] = newNextIndex
		replicateLogUnlocker.unlock(LogReplicationFlow)
	}
}

func (rf *Raft) updateCommitIndex(cancelContext *CancelContext) {
	updateCommitIndexUnlocker := rf.lock(CommitFlow)
	logContext := rf.logContext(CommitFlow)
	Log(logContext, InfoLevel, "enter updateCommitIndex")
	updateCommitIndexUnlocker.unlock(CommitFlow)

	for {
		var matchIndex int
		var ok bool
		select {
		case matchIndex, ok = <-rf.onMatchIndexChangeCh:
			if !ok {
				logUnlocker := rf.lock(CommitFlow)
				Log(rf.logContext(CommitFlow), InfoLevel, "onMatchIndexChangeCh closed, exit updateCommitIndex")
				logUnlocker.unlock(CommitFlow)
				return
			}
		case <-cancelContext.OnCancel(logContext):
			logUnlocker := rf.lock(CommitFlow)
			Log(rf.logContext(CommitFlow), InfoLevel, "canceled, exit updateCommitIndex")
			logUnlocker.unlock(CommitFlow)
			return
		}

		updateIndexUnlocker := rf.lock(CommitFlow)
		if cancelContext.IsCanceled(rf.logContext(CommitFlow)) {
			Log(rf.logContext(CommitFlow), InfoLevel, "canceled, exit updateCommitIndex")
			updateIndexUnlocker.unlock(CommitFlow)
			break
		}

		if matchIndex <= rf.commitIndex {
			Log(rf.logContext(CommitFlow), InfoLevel, "matchIndex:%v <= commitIndex:%v, skip", matchIndex, rf.commitIndex)
			updateIndexUnlocker.unlock(CommitFlow)
			continue
		}

		rf.tryUpdateCommitIndex(cancelContext, matchIndex)
		updateIndexUnlocker.unlock(CommitFlow)
	}

	updateCommitIndexUnlocker = rf.lock(CommitFlow)
	Log(rf.logContext(CommitFlow), InfoLevel, "exit updateCommitIndex")
	updateCommitIndexUnlocker.unlock(CommitFlow)
}

func (rf *Raft) tryUpdateCommitIndex(cancelContext *CancelContext, matchIndex int) {
	for index := matchIndex; index > rf.commitIndex; index-- {
		if rf.logEntries[rf.toRelativeLogIndex(index)].Term != rf.currentTerm {
			// only commit entries from current term
			continue
		}

		if rf.isReplicatedToMajority(index) {
			rf.commitIndex = index
			Log(rf.logContext(CommitFlow), InfoLevel, "commitIndex updated to %v", rf.commitIndex)

			finisher := cancelContext.Add(rf.logContext(CommitFlow), 1)
			serverFinisher := rf.serverCancelContext.Add(rf.logContext(CommitFlow), 1)
			go func(index int) {
				logUnlocker := rf.lock(CommitFlow)
				logContext := rf.logContext(CommitFlow)
				defer finisher.Done(logContext)
				defer serverFinisher.Done(logContext)
				logUnlocker.unlock(CommitFlow)

				select {
				case rf.onCommitIndexChangeCh <- index:
				case <-cancelContext.OnCancel(logContext):
					logUnlocker = rf.lock(CommitFlow)
					Log(rf.logContext(CommitFlow), InfoLevel, "role canceled, exit tryUpdateCommitIndex")
					logUnlocker.unlock(CommitFlow)
				}
			}(index)
			return
		}
	}

	return
}

func (rf *Raft) applyCommittedEntries() {
	applyCommittedEntriesUnlocker := rf.lock(ApplyEntryFlow)
	Log(rf.logContext(ApplyEntryFlow), InfoLevel, "enter applyCommittedEntries")
	applyCommittedEntriesUnlocker.unlock(ApplyEntryFlow)

	for {
		applyUnlocker := rf.lock(ApplyEntryFlow)
		logContext := rf.logContext(ApplyEntryFlow)
		applyUnlocker.unlock(ApplyEntryFlow)

		commitIndex := 0
		var ok bool
		select {
		case commitIndex, ok = <-rf.onCommitIndexChangeCh:
			if !ok {
				logUnlocker := rf.lock(ApplyEntryFlow)
				Log(rf.logContext(ApplyEntryFlow), InfoLevel, "onCommitIndexChangeCh closed, exit applyCommittedEntries")
				logUnlocker.unlock(ApplyEntryFlow)
				return
			}
		case <-rf.serverCancelContext.OnCancel(logContext):
			logUnlocker := rf.lock(ApplyEntryFlow)
			Log(rf.logContext(ApplyEntryFlow), InfoLevel, "canceled, exit applyCommittedEntries")
			logUnlocker.unlock(ApplyEntryFlow)
			return
		}

		applyUnlocker = rf.lock(ApplyEntryFlow)
		if rf.serverCancelContext.IsCanceled(rf.logContext(ApplyEntryFlow)) {
			Log(rf.logContext(ApplyEntryFlow), InfoLevel, "canceled, exit applyCommittedEntries")
			applyUnlocker.unlock(ApplyEntryFlow)
			return
		}

		if commitIndex <= rf.lastAppliedIndex {
			Log(rf.logContext(ApplyEntryFlow), InfoLevel, "commitIndex:%v <= lastAppliedIndex:%v, skip", commitIndex, rf.lastAppliedIndex)
			applyUnlocker.unlock(ApplyEntryFlow)
			continue
		}

		applyUnlocker.unlock(ApplyEntryFlow)

		for index := rf.lastAppliedIndex + 1; index <= commitIndex; index++ {
			applyUnlocker = rf.lock(ApplyEntryFlow)

			if rf.serverCancelContext.IsCanceled(rf.logContext(ApplyEntryFlow)) {
				Log(rf.logContext(ApplyEntryFlow), InfoLevel, "canceled, exit applyCommittedEntries")
				applyUnlocker.unlock(ApplyEntryFlow)
				return
			}

			logEntry := rf.logEntries[rf.toRelativeLogIndex(index)]
			logContext = rf.logContext(ApplyEntryFlow)
			Log(logContext, InfoLevel, "applying log entry %v: %v", index, logEntry)
			applyUnlocker.unlock(ApplyEntryFlow)

			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      logEntry.Command,
				CommandIndex: index,
			}

			select {
			case rf.applyCh <- applyMsg:
			case <-rf.serverCancelContext.OnCancel(logContext):
				logUnlocker := rf.lock(ApplyEntryFlow)
				Log(rf.logContext(ApplyEntryFlow), InfoLevel, "role canceled, exit applyCommittedEntries")
				logUnlocker.unlock(ApplyEntryFlow)
				return
			}

			rf.lastAppliedIndex = index
		}

		applyUnlocker = rf.lock(ApplyEntryFlow)
		Log(rf.logContext(ApplyEntryFlow), InfoLevel, "finish applying log entries: lastAppliedIndex=%v", rf.lastAppliedIndex)
		applyUnlocker.unlock(ApplyEntryFlow)
	}
}

func (rf *Raft) isReplicatedToMajority(index int) bool {
	majorityPeerCount := len(rf.peers)/2 + 1
	matchPeerCount := 1
	for peerServerID := 0; peerServerID < len(rf.peers); peerServerID++ {
		if peerServerID == rf.serverID {
			continue
		}

		if rf.matchIndices[peerServerID] >= index {
			matchPeerCount++
		}

		if matchPeerCount >= majorityPeerCount {
			return true
		}
	}

	return false
}

func (rf *Raft) toRelativeLogIndex(absoluteIndex int) int {
	// TODO: convert index for snapshot
	return absoluteIndex - 1
}

func (rf *Raft) toAbsoluteLogIndex(relativeIndex int) int {
	// TODO: convert index for snapshot
	return relativeIndex + 1
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist(flow Flow) {
	Log(rf.logContext(flow), InfoLevel, "persist enter")
	currentTerm := rf.currentTerm
	voteFor := rf.votedFor
	log := rf.logEntries

	buf := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buf)
	err := encoder.Encode(currentTerm)
	if err != nil {
		Log(rf.logContext(flow), ErrorLevel, "failed to encode currentTerm: %v", err)
	}

	var votedForPersist int
	if voteFor == nil {
		votedForPersist = -1
	} else {
		votedForPersist = *voteFor
	}

	err = encoder.Encode(votedForPersist)
	if err != nil {
		Log(rf.logContext(flow), ErrorLevel, "failed to encode votedFor: %v", err)
	}

	err = encoder.Encode(log)
	if err != nil {
		Log(rf.logContext(flow), ErrorLevel, "failed to encode log: %v", err)
	}

	data := buf.Bytes()
	rf.persister.Save(data, nil)
	Log(rf.logContext(flow), InfoLevel, "persist exit")
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte, flow Flow) {
	Log(rf.logContext(flow), InfoLevel, "readPersist enter")

	if data == nil || len(data) < 1 { // bootstrap without any state?
		Log(rf.logContext(flow), InfoLevel, "readPersist exit")
		return
	}

	var currentTerm int
	var votedFor *int
	var log []LogEntry
	buf := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buf)
	err := decoder.Decode(&currentTerm)
	if err != nil {
		Log(rf.logContext(flow), ErrorLevel, "failed to decode currentTerm: %v", err)
	}

	var votedForPersist int
	err = decoder.Decode(&votedForPersist)
	if err != nil {
		Log(rf.logContext(flow), ErrorLevel, "failed to decode votedFor: %v", err)
	}

	if votedForPersist == -1 {
		votedFor = nil
	} else {
		votedFor = &votedForPersist
	}

	err = decoder.Decode(&log)
	if err != nil {
		Log(rf.logContext(flow), ErrorLevel, "failed to decode logs: %v", err)
	}

	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.logEntries = log
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
	LogMessage(rf.messageContext(flow, rf.serverID, server, args.MessageID), InfoLevel, "send RequestVote to %v: %+v", server, args)
	unlocker.unlock(flow)

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	unlocker = rf.lock(flow)
	LogMessage(rf.messageContext(flow, server, rf.serverID, args.MessageID), InfoLevel, "receive RequestVote reply from %v: args=%+v", server, reply)
	unlocker.unlock(flow)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, flow Flow) bool {
	unlocker := rf.lock(flow)
	LogMessage(rf.messageContext(flow, rf.serverID, server, args.MessageID), InfoLevel, "send AppendEntries to %v: args=%+v", server, args)
	unlocker.unlock(flow)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	unlocker = rf.lock(flow)
	LogMessage(rf.messageContext(flow, server, rf.serverID, args.MessageID), InfoLevel, "receive AppendEntries reply from %v: reply=%+v", server, reply)
	unlocker.unlock(flow)
	return ok
}

func (rf *Raft) logContext(flow Flow) LogContext {
	return LogContext{
		ServerID:      rf.serverID,
		Role:          rf.currentRole,
		Term:          rf.currentTerm,
		Flow:          flow,
		NumGoroutines: runtime.NumGoroutine(),
	}
}

func (rf *Raft) messageContext(flow Flow, senderID int, receiverID int, messageID uint64) MessageContext {
	return MessageContext{
		LogContext: LogContext{
			ServerID:      rf.serverID,
			Role:          rf.currentRole,
			Term:          rf.currentTerm,
			Flow:          flow,
			NumGoroutines: runtime.NumGoroutine(),
		},
		SenderID:   senderID,
		ReceiverID: receiverID,
		MessageID:  messageID,
	}
}

func (rf *Raft) lock(flow Flow) *Unlocker {
	rf.mu.Lock()
	nextLockID := rf.nextLockID
	rf.nextLockID++
	rf.locks[nextLockID] = struct{}{}
	LogAndSkipCallers(
		LogContext{
			ServerID: rf.serverID,
			Role:     rf.currentRole,
			Term:     rf.currentTerm,
			Flow:     flow,
		}, DebugLevel, 1, "lock(%v): locks=%v", nextLockID, rf.locks)
	return &Unlocker{
		lockID:     nextLockID,
		unlockFunc: rf.unlock,
	}
}

func (rf *Raft) unlock(lockID uint64, flow Flow, skipCallers int) {
	delete(rf.locks, lockID)
	LogAndSkipCallers(LogContext{
		ServerID: rf.serverID,
		Role:     rf.currentRole,
		Term:     rf.currentTerm,
		Flow:     flow,
	}, DebugLevel, skipCallers+1, "unlock(%v): locks=%v", lockID, rf.locks)
	rf.mu.Unlock()
}

func newElectionTimeOut() time.Duration {
	return electionBaseTimeOut + time.Duration(rand.Intn(electionRandomTimeOutFactor))*electionRandomTimeOutMultiplier
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
	onNewCommandIndexChs := make([]chan int, len(peers))
	for peerServerIndex := 0; peerServerIndex < len(peers); peerServerIndex++ {
		if peerServerIndex == me {
			continue
		}

		onNewCommandIndexChs[peerServerIndex] = make(chan int)
	}

	var nextCancelContextId uint64 = 0
	cancelContext := newCancelContext(nextCancelContextId, string(ServerRole))
	rf := &Raft{
		peers:                 peers,
		persister:             persister,
		serverID:              me,
		currentTerm:           0,
		commitIndex:           0,
		lastAppliedIndex:      0,
		nextIndices:           make([]int, len(peers)),
		matchIndices:          make([]int, len(peers)),
		currentRole:           FollowerRole,
		applyCh:               applyCh,
		onNewCommandIndexChs:  onNewCommandIndexChs,
		onMatchIndexChangeCh:  make(chan int),
		onCommitIndexChangeCh: make(chan int),
		serverCancelContext:   cancelContext,
		nextCancelContextID:   nextCancelContextId,
		locks:                 make(map[uint64]struct{}),
	}

	nextCancelContextId++

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState(), SharedFlow)
	finisher := cancelContext.Add(rf.logContext(SharedFlow), 1)
	go func() {
		unlocker := rf.lock(SharedFlow)
		logContext := rf.logContext(SharedFlow)
		defer finisher.Done(logContext)
		unlocker.unlock(SharedFlow)

		rf.applyCommittedEntries()
	}()
	rf.runAsFollower()
	// start ticker goroutine to start elections
	return rf
}
