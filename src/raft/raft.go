package raft

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"6.5840/labgob"
	//	"6.5840/labgob"
	"6.5840/labrpc"
)

const heartBeatInterval = 100 * time.Millisecond
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
)

type LogEntry struct {
	Command interface{}
	Index   int
	Term    int
}

func (l LogEntry) String() string {
	return fmt.Sprintf("[LogEntry Command:%v, Index:%v, Term:%v]", l.Command, l.Index, l.Term)
}

type Unlocker struct {
	lockID uint64
	rf     *Raft
}

func (rf *Raft) lock(flow Flow) *Unlocker {
	rf.mu.Lock()
	nextLockID := rf.nextLockID
	rf.nextLockID++
	LogAndSkipCallers(rf.serverID, rf.currentRole, rf.currentTerm, DebugLevel, flow, 1, "lock(%v)", nextLockID)
	return &Unlocker{
		lockID: nextLockID,
		rf:     rf,
	}
}

func (u *Unlocker) unlock(flow Flow) {
	LogAndSkipCallers(u.rf.serverID, u.rf.currentRole, u.rf.currentTerm, DebugLevel, flow, 1, "unlock(%v)", u.lockID)
	u.rf.mu.Unlock()
}

type RequestVoteArgs struct {
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
	Term              int
	LeaderID          int
	PrevLogIndex      int
	PreLogTerm        int
	LogEntries        []LogEntry
	LeaderCommitIndex int
}

func (a AppendEntriesArgs) String() string {
	return fmt.Sprintf("[AppendEntriesArgs Term:%v, LeaderID:%v, PrevLogIndex:%v, PreLogTerm:%v, LogEntries:%v, LeaderCommitIndex:%v]",
		a.Term,
		a.LeaderID,
		a.PrevLogIndex,
		a.PreLogTerm,
		a.LogEntries,
		a.LeaderCommitIndex)
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictTerm  int
	ConflictIndex int
}

func (a AppendEntriesReply) String() string {
	return fmt.Sprintf("[AppendEntriesReply Term:%v, Success:%v, ConflictTerm:%v, ConflictIndex:%v]",
		a.Term,
		a.Success,
		a.ConflictTerm,
		a.ConflictIndex)
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister

	// persistent state on all servers
	currentTerm int
	votedFor    *int
	log         []LogEntry

	// volatile state on all servers
	serverID             int // this peer's index into peers[]
	currentRole          Role
	receivedValidMessage bool
	commitIndex          int
	lastAppliedIndex     int

	// volatile state on leaders
	nextIndices  []int
	matchIndices []int

	dead                  bool
	applyCh               chan ApplyMsg
	cancelCh              chan struct{}
	onNewCommandIndexChs  []chan int
	onMatchIndexChangeCh  chan int
	onCommitIndexChangeCh chan int
	sendWaitGroup         *sync.WaitGroup

	nextLockID    uint64
	nextRequestID uint64
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

	Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, ElectionFlow, "enter RequestVote")
	defer Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, ElectionFlow, "exit RequestVote")

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, ElectionFlow, "reject stale vote request from %v", args.CandidateID)
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, ElectionFlow, "update current term to %v", args.Term)
		rf.currentTerm = args.Term
		rf.votedFor = nil
		rf.persist(ElectionFlow)

		if rf.currentRole != FollowerRole {
			Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, ElectionFlow, "role changed to follower")
			rf.currentRole = FollowerRole
			rf.runAsFollower()
		}
	}

	lastLogTerm := 0
	if len(rf.log) > 0 {
		lastLogTerm = rf.log[len(rf.log)-1].Term
	}

	if args.LastLogTerm < lastLogTerm {
		Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, ElectionFlow, "reject stale vote request from %v", args.CandidateID)
		reply.VoteGranted = false
		return
	}

	if args.LastLogTerm == lastLogTerm {
		if args.LastLogIndex < rf.toAbsoluteLogIndex(len(rf.log)-1) {
			Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, ElectionFlow, "reject vote request from %v because of lower lastLogIndex", args.CandidateID)
			reply.VoteGranted = false
			return
		}
	}

	rf.receivedValidMessage = true
	Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, ElectionFlow, "received valid message from %v", args.CandidateID)

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

	Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, LogReplicationFlow, "enter AppendEntries: args=%v", args)
	defer Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, LogReplicationFlow, "exit AppendEntries=reply=%v", reply)

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, LogReplicationFlow, "reject stale AppendEntries from %v", args.LeaderID)
		reply.Success = false
		return
	}

	rf.receivedValidMessage = true
	Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, LogReplicationFlow, "received valid message from %v", args.LeaderID)

	if args.Term > rf.currentTerm {
		Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, LogReplicationFlow, "update current term to %v", args.Term)
		rf.currentTerm = args.Term
		rf.votedFor = nil
		rf.persist(LogReplicationFlow)
	}

	if rf.currentRole != FollowerRole {
		Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, LogReplicationFlow, "role changed to follower")
		rf.currentRole = FollowerRole
		rf.runAsFollower()
		return
	}

	Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, LogReplicationFlow, "before appendEntries from %v: commitIndex=%v, log=%v", args.LeaderID, rf.commitIndex, rf.log)
	defer Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, LogReplicationFlow, "after appendEntries from %v: commitIndex=%v, log=%v", args.LeaderID, rf.commitIndex, rf.log)

	relativePrevLogIndex := rf.toRelativeLogIndex(args.PrevLogIndex)
	if relativePrevLogIndex > len(rf.log)-1 {
		Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, LogReplicationFlow, "prevLogIndex not found, reject AppendEntries from %v", args.LeaderID)
		reply.Success = false
		// TODO: implement nextIndex backoff optimization
		return
	}

	if relativePrevLogIndex >= 0 {
		if args.PreLogTerm != rf.log[relativePrevLogIndex].Term {
			Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, LogReplicationFlow, "prevLogTerm not match, reject AppendEntries from %v", args.LeaderID)
			reply.Success = false
			// TODO: implement nextIndex backoff optimization
			return
		}
	}

	reply.Success = true
	remainNewEntries := make([]LogEntry, 0)
	for index, logEntry := range args.LogEntries {
		originalIndex := relativePrevLogIndex + index + 1
		if originalIndex >= len(rf.log) {
			Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, LogReplicationFlow, "prepare to append new entries from %v: %v", args.LeaderID, args.LogEntries[index:])
			remainNewEntries = args.LogEntries[index:]
			break
		}

		if rf.log[originalIndex].Term != logEntry.Term {
			Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, LogReplicationFlow, "truncate log entries from %v: %v", args.LeaderID, rf.log[originalIndex:])
			rf.log = rf.log[:originalIndex]
			rf.persist(LogReplicationFlow)

			Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, LogReplicationFlow, "prepare to append new entries from %v: %v", args.LeaderID, remainNewEntries)
			remainNewEntries = args.LogEntries[index:]
			break
		}
	}

	if len(remainNewEntries) > 0 {
		Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, LogReplicationFlow, "append new entries from %v: %v", args.LeaderID, remainNewEntries)
		rf.log = append(rf.log, remainNewEntries...)
		rf.persist(LogReplicationFlow)
	}

	if args.LeaderCommitIndex > rf.commitIndex {
		// entries after the last entry of appendEntries may not be committed on the leader
		// we need to take the minimum between leaderCommitIndex and last log index of appendEntries

		newCommitIndex := min(args.LeaderCommitIndex, args.PrevLogIndex+len(args.LogEntries))
		if newCommitIndex > rf.commitIndex {
			rf.commitIndex = newCommitIndex
			Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, LogReplicationFlow, "update commitIndex to %v", rf.commitIndex)

			rf.sendWaitGroup.Add(1)
			go func() {
				defer rf.sendWaitGroup.Done()
				select {
				case rf.onCommitIndexChangeCh <- newCommitIndex:
				case <-rf.cancelCh:
				}
			}()
		}
	} else {
		Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, LogReplicationFlow, "no change to commitIndex")
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
	fmt.Printf("Start(%v)\n", command)
	unlocker := rf.lock(LogReplicationFlow)
	defer unlocker.unlock(LogReplicationFlow)
	Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, LogReplicationFlow, "enter Start")
	defer Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, LogReplicationFlow, "exit Start")

	if !rf.dead && rf.currentRole == LeaderRole {
		nextIndex := len(rf.log)
		logEntry := append(rf.log, LogEntry{
			Command: command,
			Index:   nextIndex + 1,
			Term:    rf.currentTerm,
		})
		rf.log = logEntry
		rf.persist(LogReplicationFlow)
		Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, LogReplicationFlow, "append new log entry: %v", logEntry)

		// start replicating log entries
		for _, ch := range rf.onNewCommandIndexChs {
			rf.sendWaitGroup.Add(1)
			go func(ch chan int) {
				defer rf.sendWaitGroup.Done()
				select {
				case ch <- nextIndex + 1:
				case <-rf.cancelCh:
				}
			}(ch)
		}
	}

	return len(rf.log), rf.currentTerm, rf.currentRole == LeaderRole
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
	Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, TerminationFlow, "Kill enter")
	rf.dead = true
	close(rf.cancelCh)
	unlocker.unlock(TerminationFlow)

	rf.sendWaitGroup.Wait()

	unlocker = rf.lock(TerminationFlow)
	defer unlocker.unlock(TerminationFlow)

	Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, TerminationFlow, "close onCommitIndexChangeCh")
	close(rf.onCommitIndexChangeCh)

	Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, TerminationFlow, "close onMatchIndexChangeCh")
	close(rf.onMatchIndexChangeCh)

	for peerServerID, ch := range rf.onNewCommandIndexChs {
		if peerServerID == rf.serverID {
			continue
		}

		Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, TerminationFlow, "close onNewCommandIndexCh %v", peerServerID)
		close(ch)
	}

	Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, TerminationFlow, "Kill exit")
}

func (rf *Raft) runAsFollower() {
	Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, FollowerFlow, "enter runAsFollower")
	defer Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, FollowerFlow, "exit runAsFollower")

	go func() {
		startElectionTimerUnlocker := rf.lock(FollowerFlow)
		Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, FollowerFlow, "start election timer")
		startElectionTimerUnlocker.unlock(FollowerFlow)

		for {
			electionTimerUnlocker := rf.lock(FollowerFlow)
			if rf.dead {
				Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, FollowerFlow, "killed, exit runAsFollower")
				electionTimerUnlocker.unlock(FollowerFlow)
				break
			}

			if rf.currentRole != FollowerRole {
				Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, FollowerFlow, "role changed, exit runAsFollower")
				electionTimerUnlocker.unlock(FollowerFlow)
				break
			}

			rf.receivedValidMessage = false
			Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, FollowerFlow, "begin waiting for election timeout")
			electionTimerUnlocker.unlock(FollowerFlow)

			time.Sleep(newElectionTimeOut())
			electionTimerUnlocker = rf.lock(FollowerFlow)
			Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, FollowerFlow, "end waiting for election timeout")

			if rf.dead {
				Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, FollowerFlow, "killed, exit runAsFollower")
				electionTimerUnlocker.unlock(FollowerFlow)
				break
			}

			if rf.currentRole != FollowerRole {
				Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, FollowerFlow, "role changed, exit runAsFollower")
				electionTimerUnlocker.unlock(FollowerFlow)
				break
			}

			if !rf.receivedValidMessage {
				Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, FollowerFlow, "no valid message received")
				rf.currentRole = CandidateRole

				rf.currentTerm++
				rf.votedFor = nil
				rf.persist(FollowerFlow)

				rf.runAsCandidate()
				electionTimerUnlocker.unlock(FollowerFlow)
				break
			}

			electionTimerUnlocker.unlock(FollowerFlow)
		}

		endElectionTimerUnlocker := rf.lock(FollowerFlow)
		Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, FollowerFlow, "end election timer")
		endElectionTimerUnlocker.unlock(FollowerFlow)
	}()
}

func (rf *Raft) runAsCandidate() {
	Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, CandidateFlow, "enter runAsCandidate")
	defer Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, CandidateFlow, "exit runAsCandidate")

	go func() {
		startElectionTimerUnlocker := rf.lock(CandidateFlow)
		Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, CandidateFlow, "start election timer")
		startElectionTimerUnlocker.unlock(CandidateFlow)

		for {
			electionTimerUnlocker := rf.lock(CandidateFlow)
			Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, CandidateFlow, "start new election timer")

			if rf.dead {
				Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, CandidateFlow, "killed, exit runAsCandidate")
				electionTimerUnlocker.unlock(CandidateFlow)
				break
			}

			if rf.currentRole != CandidateRole {
				Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, CandidateFlow, "role changed, exit runAsCandidate")
				electionTimerUnlocker.unlock(CandidateFlow)
				break
			}

			electionTerm := rf.currentTerm
			electionTimerUnlocker.unlock(CandidateFlow)

			rf.beginElection(electionTerm)
			time.Sleep(newElectionTimeOut())

			electionTimerUnlocker = rf.lock(CandidateFlow)
			if rf.dead {
				Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, CandidateFlow, "killed, exit runAsCandidate")
				electionTimerUnlocker.unlock(CandidateFlow)
				break
			}

			if rf.currentRole != CandidateRole {
				Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, CandidateFlow, "role changed, exit runAsCandidate")
				electionTimerUnlocker.unlock(CandidateFlow)
				break
			}

			rf.currentTerm++
			rf.votedFor = nil
			rf.persist(CandidateFlow)

			electionTimerUnlocker.unlock(CandidateFlow)
		}

		endElectionTimerUnlocker := rf.lock(CandidateFlow)
		Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, CandidateFlow, "end election timer")
		endElectionTimerUnlocker.unlock(CandidateFlow)
	}()
}

func (rf *Raft) runAsLeader() {
	Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, LeaderFlow, "enter runAsLeader")
	defer Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, LeaderFlow, "exit runAsLeader")
	for peerServerID := 0; peerServerID < len(rf.peers); peerServerID++ {
		if peerServerID == rf.serverID {
			continue
		}

		rf.nextIndices[peerServerID] = len(rf.log) + 1
		rf.matchIndices[peerServerID] = 0
	}

	go func() {
		rf.sendHeartbeats()
	}()
	go func() {
		rf.replicateLogToAllPeers()
	}()
	go func() {
		rf.updateCommitIndex()
	}()
}

func (rf *Raft) beginElection(electionTerm int) {
	unlocker := rf.lock(ElectionFlow)
	defer unlocker.unlock(ElectionFlow)
	Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, ElectionFlow, "enter beginElection: electionTerm=%v", electionTerm)
	defer Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, ElectionFlow, "exit beginElection, electionTerm=%v", electionTerm)

	totalServers := len(rf.peers)
	majorityServers := totalServers/2 + 1
	currentServerID := rf.serverID
	lastLogIndex := len(rf.log)
	currentTerm := rf.currentTerm
	lastLogTerm := 0
	if lastLogIndex > 0 {
		lastLogTerm = rf.log[lastLogIndex-1].Term
	}

	rf.votedFor = &currentServerID
	rf.persist(ElectionFlow)

	requestVoteArgs := &RequestVoteArgs{
		Term:         currentTerm,
		CandidateID:  currentServerID,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	totalVotes := 1
	grantedVotes := 1
	voteMu := &sync.Mutex{}
	voteCond := sync.NewCond(voteMu)

	go func() {
		requestVotesUnlocker := rf.lock(ElectionFlow)
		Log(currentServerID, rf.currentRole, currentTerm, InfoLevel, ElectionFlow, "begin requesting votes")
		requestVotesUnlocker.unlock(ElectionFlow)

		for peerServerID := 0; peerServerID < totalServers; peerServerID++ {
			if peerServerID == currentServerID {
				continue
			}

			go func(peerServerID int) {
				reply := &RequestVoteReply{}
				succeed := rf.sendRequestVote(peerServerID, requestVoteArgs, reply, ElectionFlow)
				voteMu.Lock()
				defer voteMu.Unlock()
				defer voteCond.Signal()
				totalVotes++

				peerUnlocker := rf.lock(ElectionFlow)
				defer peerUnlocker.unlock(ElectionFlow)
				if !succeed {
					Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, ElectionFlow, "failed to send vote request to %v", peerServerID)
					return
				}

				if rf.dead {
					Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, ElectionFlow, "killed, exit beginElection")
					return
				}

				if rf.currentRole != CandidateRole {
					Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, ElectionFlow, "role changed, exit beginElection")
					return
				}

				if rf.currentTerm != electionTerm {
					Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, ElectionFlow, "term changed, exit beginElection")
					return
				}

				if reply.Term > rf.currentTerm {
					rf.currentRole = FollowerRole
					rf.currentTerm = reply.Term
					rf.votedFor = nil
					rf.persist(ElectionFlow)
					rf.runAsFollower()
					return
				}

				Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, ElectionFlow, "received vote from %v, granted:%v", peerServerID, reply.VoteGranted)
				if reply.VoteGranted {
					grantedVotes++
				}
			}(peerServerID)
		}

		voteMu.Lock()
		for totalVotes < totalServers && grantedVotes < majorityServers {
			voteCond.Wait()
		}

		unlocker = rf.lock(ElectionFlow)
		defer unlocker.unlock(ElectionFlow)
		defer Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, ElectionFlow, "finish requesting votes")

		if rf.dead {
			Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, ElectionFlow, "killed, exit beginElection")
			return
		}

		if rf.currentRole != CandidateRole {
			Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, ElectionFlow, "role changed, exit beginElection")
			return
		}

		if rf.currentTerm != electionTerm {
			Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, ElectionFlow, "term changed, exit beginElection")
			return
		}

		if grantedVotes >= majorityServers {
			rf.currentRole = LeaderRole
			rf.runAsLeader()
			return
		}
	}()
}

func (rf *Raft) sendHeartbeats() {
	unlocker := rf.lock(HeartbeatFlow)
	Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, HeartbeatFlow, "enter sendHeartbeats")
	unlocker.unlock(HeartbeatFlow)

	for {
		sendHeartbeatsUnlocker := rf.lock(HeartbeatFlow)
		if rf.dead {
			Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, HeartbeatFlow, "killed, exit sendHeartbeats")
			sendHeartbeatsUnlocker.unlock(HeartbeatFlow)
			break
		}

		if rf.currentRole != LeaderRole {
			Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, HeartbeatFlow, "role changed, exit sendHeartbeats")
			sendHeartbeatsUnlocker.unlock(HeartbeatFlow)
			break
		}

		currentTerm := rf.currentTerm
		leaderID := rf.serverID
		prevLogIndex := len(rf.log)
		prevLogTerm := 0
		if prevLogIndex > 0 {
			prevLogTerm = rf.log[prevLogIndex-1].Term
		}

		leaderCommitIndex := rf.commitIndex
		args := &AppendEntriesArgs{
			Term:              currentTerm,
			LeaderID:          leaderID,
			PrevLogIndex:      prevLogIndex,
			PreLogTerm:        prevLogTerm,
			LogEntries:        nil,
			LeaderCommitIndex: leaderCommitIndex,
		}

		for peerServerID := 0; peerServerID < len(rf.peers); peerServerID++ {
			if peerServerID == rf.serverID {
				continue
			}

			go func(peerServerID int) {
				peerUnlocker := rf.lock(HeartbeatFlow)

				if rf.dead {
					Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, HeartbeatFlow, "killed, exit sendHeartbeat")
					peerUnlocker.unlock(HeartbeatFlow)
					return
				}

				if rf.currentRole != LeaderRole {
					Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, HeartbeatFlow, "role changed, exit sendHeartbeat")
					peerUnlocker.unlock(HeartbeatFlow)
					return
				}

				peerUnlocker.unlock(HeartbeatFlow)

				reply := &AppendEntriesReply{}
				succeed := rf.sendAppendEntries(peerServerID, args, reply, HeartbeatFlow)

				peerUnlocker = rf.lock(HeartbeatFlow)
				defer peerUnlocker.unlock(HeartbeatFlow)
				if rf.dead {
					Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, HeartbeatFlow, "killed, exit sendHeartbeat")
					return
				}

				if rf.currentRole != LeaderRole {
					Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, HeartbeatFlow, "role changed, exit sendHeartbeat")
					return
				}

				if !succeed {
					Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, HeartbeatFlow, "failed to send heartbeat to %v", peerServerID)
					return
				}

				if reply.Term > rf.currentTerm {
					rf.currentRole = FollowerRole
					rf.currentTerm = reply.Term
					rf.votedFor = nil
					rf.persist(HeartbeatFlow)
					rf.runAsFollower()
					return
				}
			}(peerServerID)
		}

		sendHeartbeatsUnlocker.unlock(HeartbeatFlow)
		time.Sleep(heartBeatInterval)
	}

	unlocker = rf.lock(HeartbeatFlow)
	Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, HeartbeatFlow, "exit sendHeartbeats")
	unlocker.unlock(HeartbeatFlow)
}

func (rf *Raft) replicateLogToAllPeers() {
	replicateLogToPeersUnlocker := rf.lock(LogReplicationFlow)
	Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, LogReplicationFlow, "enter replicateLogToAllPeers")
	replicateLogToPeersUnlocker.unlock(LogReplicationFlow)

	wg := sync.WaitGroup{}
	for peerServerID := 0; peerServerID < len(rf.peers); peerServerID++ {
		if peerServerID == rf.serverID {
			continue
		}

		replicateLogToPeerUnlocker := rf.lock(LogReplicationFlow)
		if rf.dead {
			Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, LogReplicationFlow, "killed, exit replicateLogToAllPeers")
			replicateLogToPeerUnlocker.unlock(LogReplicationFlow)
			break
		}

		if rf.currentRole != LeaderRole {
			Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, LogReplicationFlow, "role changed, exit replicateLogToAllPeers")
			replicateLogToPeerUnlocker.unlock(LogReplicationFlow)
			break
		}

		replicateLogToPeerUnlocker.unlock(LogReplicationFlow)

		wg.Add(1)
		go func(peerServerID int) {
			defer wg.Done()
			rf.replicateLogToOnePeer(peerServerID)
		}(peerServerID)
	}

	wg.Wait()

	replicateLogToPeersUnlocker = rf.lock(LogReplicationFlow)
	Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, LogReplicationFlow, "exit replicatelogToPeers")
	replicateLogToPeersUnlocker.unlock(LogReplicationFlow)
}

func (rf *Raft) replicateLogToOnePeer(peerServerID int) {
	replicateLogToOnePeerUnlocker := rf.lock(LogReplicationFlow)
	Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, LogReplicationFlow, "enter replicateLogToOnePeer %v", peerServerID)
	onNewCommandIndexCh := rf.onNewCommandIndexChs[peerServerID]
	replicateLogToOnePeerUnlocker.unlock(LogReplicationFlow)

	for {
		replicateLogUnlocker := rf.lock(LogReplicationFlow)
		if rf.dead {
			Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, LogReplicationFlow, "killed, exit replicateLogToOnePeer")
			replicateLogUnlocker.unlock(LogReplicationFlow)
			break
		}

		if rf.currentRole != LeaderRole {
			Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, LogReplicationFlow, "role changed, exit replicateLogToOnePeer")
			replicateLogUnlocker.unlock(LogReplicationFlow)
			break
		}

		lastEntryIndex := len(rf.log)
		if rf.matchIndices[peerServerID] == lastEntryIndex {
			replicateLogUnlocker.unlock(LogReplicationFlow)

			commandIndex, ok := <-onNewCommandIndexCh
			if !ok {
				Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, LogReplicationFlow, "onNewCommandIndexCh closed, exit replicateLogToOnePeer")
				break
			}

			replicateLogUnlocker = rf.lock(LogReplicationFlow)
			if rf.currentRole != LeaderRole {
				Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, LogReplicationFlow, "role changed, exit replicateLogToOnePeer")
				replicateLogUnlocker.unlock(LogReplicationFlow)
				break
			}

			lastEntryIndex = len(rf.log)
			if commandIndex < lastEntryIndex {
				Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, LogReplicationFlow, "command index outdated, skipping: peer:%v, commandIndex=%v, endOfLog=%v",
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
			prevLogTerm = rf.log[rf.toRelativeLogIndex(prevLogIndex)].Term
		}

		logEntries := rf.log[rf.toRelativeLogIndex(nextIndex):]
		args := &AppendEntriesArgs{
			Term:              rf.currentTerm,
			LeaderID:          rf.serverID,
			PrevLogIndex:      prevLogIndex,
			PreLogTerm:        prevLogTerm,
			LogEntries:        logEntries,
			LeaderCommitIndex: rf.commitIndex,
		}
		replicateLogUnlocker.unlock(LogReplicationFlow)
		reply := &AppendEntriesReply{}
		succeed := rf.sendAppendEntries(peerServerID, args, reply, LogReplicationFlow)

		replicateLogUnlocker = rf.lock(LogReplicationFlow)
		if rf.dead {
			Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, LogReplicationFlow, "killed, exit replicateLogToOnePeer")
			replicateLogUnlocker.unlock(LogReplicationFlow)
			break
		}

		if rf.currentRole != LeaderRole {
			Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, LogReplicationFlow, "role changed, exit replicateLogToOnePeer")
			replicateLogUnlocker.unlock(LogReplicationFlow)
			break
		}

		if !succeed {
			Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, LogReplicationFlow, "failed to replicate log to %v", peerServerID)
			replicateLogUnlocker.unlock(LogReplicationFlow)
			continue
		}

		if reply.Term > rf.currentTerm {
			Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, LogReplicationFlow, "role changed, exit replicateLogToOnePeer")
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

			rf.sendWaitGroup.Add(1)
			go func(lastEntryIndex int) {
				defer rf.sendWaitGroup.Done()
				lastEntryUnlocker := rf.lock(LogReplicationFlow)
				if rf.dead {
					lastEntryUnlocker.unlock(LogReplicationFlow)
					return
				}

				if rf.currentRole != LeaderRole {
					lastEntryUnlocker.unlock(LogReplicationFlow)
					return
				}

				lastEntryUnlocker.unlock(LogReplicationFlow)

				select {
				case rf.onMatchIndexChangeCh <- lastEntryIndex:
				case <-rf.cancelCh:
				}
			}(lastEntryIndex)
			replicateLogUnlocker.unlock(LogReplicationFlow)
			continue
		}

		// TODO: implement index backoff optimization
		rf.nextIndices[peerServerID] = max(rf.nextIndices[peerServerID]-1, 1)
		replicateLogUnlocker.unlock(LogReplicationFlow)
	}

	replicateLogToOnePeerUnlocker = rf.lock(LogReplicationFlow)
	Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, LogReplicationFlow, "exit replicateLogToOnePeer %v", peerServerID)
	replicateLogToOnePeerUnlocker.unlock(LogReplicationFlow)
}

func (rf *Raft) updateCommitIndex() {
	updateCommitIndexUnlocker := rf.lock(CommitFlow)
	Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, CommitFlow, "enter updateCommitIndex")
	updateCommitIndexUnlocker.unlock(CommitFlow)

	for matchIndex := range rf.onMatchIndexChangeCh {
		updateIndexUnlocker := rf.lock(CommitFlow)
		if rf.dead {
			Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, CommitFlow, "killed, exit updateCommitIndex")
			updateIndexUnlocker.unlock(CommitFlow)
			return
		}

		if rf.currentRole != LeaderRole {
			Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, CommitFlow, "role changed, exit updateCommitIndex")
			updateIndexUnlocker.unlock(CommitFlow)
			return
		}

		if matchIndex <= rf.commitIndex {
			Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, CommitFlow, "matchIndex:%v <= commitIndex:%v, skip", matchIndex, rf.commitIndex)
			updateIndexUnlocker.unlock(CommitFlow)
			continue
		}

		rf.tryUpdateCommitIndex(matchIndex)
		updateIndexUnlocker.unlock(CommitFlow)
	}

	updateCommitIndexUnlocker = rf.lock(CommitFlow)
	Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, CommitFlow, "exit applyCommittedEntries")
	updateCommitIndexUnlocker.unlock(CommitFlow)
}

func (rf *Raft) tryUpdateCommitIndex(matchIndex int) {
	for index := matchIndex; index > rf.commitIndex; index-- {
		if rf.log[rf.toRelativeLogIndex(index)].Term != rf.currentTerm {
			// only commit entries from current term
			continue
		}

		if rf.isReplicatedToMajority(index) {
			rf.commitIndex = index
			Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, CommitFlow, "commitIndex updated to %v", rf.commitIndex)
			rf.sendWaitGroup.Add(1)
			go func(index int) {
				defer rf.sendWaitGroup.Done()
				commitIndexUnlocker := rf.lock(CommitFlow)
				if rf.dead {
					commitIndexUnlocker.unlock(CommitFlow)
					return
				}

				commitIndexUnlocker.unlock(CommitFlow)

				select {
				case rf.onCommitIndexChangeCh <- index:
				case <-rf.cancelCh:
				}
			}(index)
			return
		}
	}

	return
}

func (rf *Raft) applyCommittedEntries() {
	defer rf.sendWaitGroup.Done()
	applyCommittedEntriesUnlocker := rf.lock(ApplyEntryFlow)
	Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, ApplyEntryFlow, "enter applyCommittedEntries")
	applyCommittedEntriesUnlocker.unlock(ApplyEntryFlow)

	for {
		commitIndex := 0
		var ok bool
		select {
		case commitIndex, ok = <-rf.onCommitIndexChangeCh:
			if !ok {
				break
			}
		case <-rf.cancelCh:
			break
		}

		applyUnlocker := rf.lock(ApplyEntryFlow)
		if rf.dead {
			Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, ApplyEntryFlow, "killed, exit applyCommittedEntries")
			applyUnlocker.unlock(ApplyEntryFlow)
			return
		}

		if commitIndex <= rf.lastAppliedIndex {
			Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, ApplyEntryFlow, "commitIndex:%v <= lastAppliedIndex:%v, skip", commitIndex, rf.lastAppliedIndex)
			applyUnlocker.unlock(ApplyEntryFlow)
			continue
		}

		applyUnlocker.unlock(ApplyEntryFlow)

		for index := rf.lastAppliedIndex + 1; index <= commitIndex; index++ {
			applyUnlocker = rf.lock(ApplyEntryFlow)
			if rf.dead {
				Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, ApplyEntryFlow, "killed, exit applyCommittedEntries")
				applyUnlocker.unlock(ApplyEntryFlow)
				return
			}

			logEntry := rf.log[rf.toRelativeLogIndex(index)]
			Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, ApplyEntryFlow, "applying log entry %v: %v", index, logEntry)
			applyUnlocker.unlock(ApplyEntryFlow)

			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      logEntry.Command,
				CommandIndex: index,
			}

			select {
			case rf.applyCh <- applyMsg:
			case <-rf.cancelCh:
				return
			}

			rf.lastAppliedIndex = index
		}

		applyUnlocker = rf.lock(ApplyEntryFlow)
		Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, ApplyEntryFlow, "finish applying log entries: lastAppliedIndex=%v", rf.lastAppliedIndex)
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
	Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, flow, "persist enter")
	serverID := rf.serverID
	currentRole := rf.currentRole

	currentTerm := rf.currentTerm
	voteFor := rf.votedFor
	log := rf.log

	buf := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buf)
	err := encoder.Encode(currentTerm)
	if err != nil {
		Log(serverID, currentRole, currentTerm, ErrorLevel, flow, "failed to encode currentTerm: %v", err)
	}

	var votedForPersist int
	if voteFor == nil {
		votedForPersist = -1
	} else {
		votedForPersist = *voteFor
	}

	err = encoder.Encode(votedForPersist)
	if err != nil {
		Log(serverID, currentRole, currentTerm, ErrorLevel, flow, "failed to encode votedFor: %v", err)
	}

	err = encoder.Encode(log)
	if err != nil {
		Log(serverID, currentRole, currentTerm, ErrorLevel, flow, "failed to encode log: %v", err)
	}

	data := buf.Bytes()
	rf.persister.Save(data, nil)
	Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, flow, "persist exit")
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte, flow Flow) {
	Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, flow, "readPersist enter")

	if data == nil || len(data) < 1 { // bootstrap without any state?
		Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, flow, "readPersist exit")
		return
	}

	serverID := rf.serverID
	currentRole := rf.currentRole

	var currentTerm int
	var votedFor *int
	var log []LogEntry
	buf := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buf)
	err := decoder.Decode(&currentTerm)
	if err != nil {
		Log(serverID, currentRole, currentTerm, ErrorLevel, flow, "failed to decode currentTerm: %v", err)
	}

	var votedForPersist int
	err = decoder.Decode(&votedForPersist)
	if err != nil {
		Log(serverID, currentRole, currentTerm, ErrorLevel, flow, "failed to decode votedFor: %v", err)
	}

	if votedForPersist == -1 {
		votedFor = nil
	} else {
		votedFor = &votedForPersist
	}

	err = decoder.Decode(&log)
	if err != nil {
		Log(serverID, currentRole, currentTerm, ErrorLevel, flow, "failed to decode logs: %v", err)
	}

	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = log
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
	Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, flow, "send RequestVote to %v", server)
	unlocker.unlock(flow)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, flow Flow) bool {
	unlocker := rf.lock(flow)
	Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, flow, "send AppendEntries to %v", server)
	unlocker.unlock(flow)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
		cancelCh:              make(chan struct{}),
		sendWaitGroup:         &sync.WaitGroup{},
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState(), FollowerFlow)
	go func() {
		rf.sendWaitGroup.Add(1)
		rf.applyCommittedEntries()
	}()
	rf.runAsFollower()
	// start ticker goroutine to start elections
	return rf
}
