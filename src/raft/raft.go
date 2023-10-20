package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

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
	Term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	serverID  int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentRole Role
	currentTerm int
	votedFor    *int
	logs        []LogEntry

	receivedValidMessage bool
	commitIndex          int
	lastApplied          int

	nextIndex          []int
	matchIndex         []int
	newCommandIndexChs []chan int
	nextLockID         uint64
	nextRequestID      uint64
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	unlocker := rf.lock(StateFlow)
	defer unlocker.unlock(StateFlow)
	return rf.currentTerm, rf.currentRole == LeaderRole
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	unlocker := rf.lock(ElectionFlow)
	defer unlocker.unlock(ElectionFlow)

	Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, ElectionFlow, "enter RequestVote")
	defer Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, ElectionFlow, "exit RequestVote")

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, ElectionFlow, "reject vote request from %v because of lower term", args.CandidateID)
		reply.VoteGranted = false
		return
	}

	// TODO: check if the candidate's log is at least as up-to-date as receiver's log
	rf.receivedValidMessage = true
	Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, ElectionFlow, "received valid message from %v", args.CandidateID)

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = nil

		if rf.currentRole != FollowerRole {
			rf.currentRole = FollowerRole
			rf.runAsFollower()
		}
	}

	if rf.votedFor != nil && *rf.votedFor != args.CandidateID {
		reply.VoteGranted = false
		return
	}

	reply.VoteGranted = true
	rf.votedFor = &args.CandidateID
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
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

type AppendEntriesArgs struct {
	Term              int
	LeaderID          int
	PrevLogIndex      int
	PreLogTerm        int
	LogEntries        []LogEntry
	LeaderCommitIndex int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	unlocker := rf.lock(LogReplicationFlow)
	defer unlocker.unlock(LogReplicationFlow)

	Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, LogReplicationFlow, "enter AppendEntries")
	defer Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, LogReplicationFlow, "exit AppendEntries")

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	rf.receivedValidMessage = true
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = nil
	}

	if rf.currentRole != FollowerRole {
		rf.currentRole = FollowerRole
		rf.runAsFollower()
		return
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, flow Flow) bool {
	unlocker := rf.lock(flow)
	Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, flow, "send AppendEntries to %v", server)
	unlocker.unlock(flow)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
	unlocker := rf.lock(LeaderFlow)
	defer unlocker.unlock(LeaderFlow)

	if !rf.killed() && rf.currentRole == LeaderRole {
		rf.logs = append(rf.logs, LogEntry{
			Command: command,
			Term:    rf.currentTerm,
		})
		index := len(rf.logs)
		// start replicating log entries
		for _, ch := range rf.newCommandIndexChs {
			go func(ch chan int) {
				ch <- index
			}(ch)
		}
	}

	return len(rf.logs), rf.currentTerm, rf.currentRole == LeaderRole
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
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) runAsFollower() {
	Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, FollowerFlow, "enter runAsFollower")
	defer Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, FollowerFlow, "exit runAsFollower")

	go func() {
		startElectionTimerUnlocker := rf.lock(FollowerFlow)
		Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, FollowerFlow, "start election timer")
		startElectionTimerUnlocker.unlock(FollowerFlow)

		for !rf.killed() {
			electionTimerUnlocker := rf.lock(FollowerFlow)
			if rf.currentRole != FollowerRole {
				Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, FollowerFlow, "role changed, exit runAsFollower")
				electionTimerUnlocker.unlock(FollowerFlow)
				break
			}

			electionTimerUnlocker.unlock(FollowerFlow)
			time.Sleep(newElectionTimeOut())

			electionTimerUnlocker = rf.lock(FollowerFlow)
			if rf.killed() {
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
				rf.currentTerm++
				rf.votedFor = nil
				rf.currentRole = CandidateRole
				rf.runAsCandidate()
				electionTimerUnlocker.unlock(FollowerFlow)
				break
			}

			rf.receivedValidMessage = false
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

		for !rf.killed() {
			electionTimerUnlocker := rf.lock(CandidateFlow)
			if rf.currentRole != CandidateRole {
				Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, CandidateFlow, "role changed, exit runAsCandidate")
				electionTimerUnlocker.unlock(CandidateFlow)
				break
			}

			electionTimerUnlocker.unlock(CandidateFlow)

			rf.beginElection()
			time.Sleep(newElectionTimeOut())

			electionTimerUnlocker = rf.lock(CandidateFlow)
			if rf.killed() {
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
			electionTimerUnlocker.unlock(CandidateFlow)
		}

		endElectionTimerUnlocker := rf.lock(CandidateFlow)
		Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, CandidateFlow, "end election timer")
		endElectionTimerUnlocker.unlock(CandidateFlow)
	}()
}

func (rf *Raft) beginElection() {
	unlocker := rf.lock(ElectionFlow)
	defer unlocker.unlock(ElectionFlow)
	Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, ElectionFlow, "enter beginElection")
	defer Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, ElectionFlow, "exit beginElection")

	totalServers := len(rf.peers)
	majorityServers := totalServers/2 + 1
	currentServerID := rf.serverID
	lastLogIndex := len(rf.logs)
	currentTerm := rf.currentTerm
	lastLogTerm := 0
	if lastLogIndex > 0 {
		lastLogTerm = rf.logs[lastLogIndex-1].Term
	}

	rf.votedFor = &currentServerID

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

				if rf.killed() {
					Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, ElectionFlow, "killed, exit beginElection")
					return
				}

				if rf.currentRole != CandidateRole {
					Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, ElectionFlow, "role changed, exit beginElection")
					return
				}

				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.currentRole = FollowerRole
					rf.votedFor = nil
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

		if rf.killed() {
			Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, ElectionFlow, "killed, exit beginElection")
			return
		}

		if rf.currentRole != CandidateRole {
			Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, ElectionFlow, "role changed, exit beginElection")
			return
		}

		if grantedVotes >= majorityServers {
			rf.currentRole = LeaderRole
			rf.runAsLeader()
			return
		}
	}()
}

func (rf *Raft) runAsLeader() {
	Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, LeaderFlow, "enter runAsLeader")
	defer Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, LeaderFlow, "exit runAsLeader")
	go func() {
		rf.sendHeartbeats()
	}()
}

func (rf *Raft) sendHeartbeats() {
	unlocker := rf.lock(HeartbeatFlow)
	Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, HeartbeatFlow, "enter sendHeartbeats")
	unlocker.unlock(HeartbeatFlow)

	for {
		sendHeartbeatsUnlocker := rf.lock(HeartbeatFlow)
		if rf.killed() {
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
		prevLogIndex := len(rf.logs)
		prevLogTerm := 0
		if prevLogIndex > 0 {
			prevLogTerm = rf.logs[prevLogIndex-1].Term
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

				if rf.killed() {
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
				if !succeed {
					Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, HeartbeatFlow, "failed to send heartbeat to %v", peerServerID)
					return
				}

				if rf.killed() {
					Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, HeartbeatFlow, "killed, exit sendHeartbeat")
					return
				}

				if rf.currentRole != LeaderRole {
					Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, HeartbeatFlow, "role changed, exit sendHeartbeat")
					return
				}

				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.votedFor = nil
					rf.currentRole = FollowerRole
					rf.runAsFollower()
					return
				}
			}(peerServerID)
		}

		unlocker.unlock(HeartbeatFlow)
		time.Sleep(heartBeatInterval)
	}

	unlocker = rf.lock(HeartbeatFlow)
	Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, HeartbeatFlow, "exit sendHeartbeats")
	unlocker.unlock(HeartbeatFlow)
}

func (rf *Raft) applyCommittedEntries() {
	Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, CommitFlow, "enter applyCommittedEntries")
	defer Log(rf.serverID, rf.currentRole, rf.currentTerm, InfoLevel, CommitFlow, "exit applyCommittedEntries")
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
	rf := &Raft{
		peers:       peers,
		persister:   persister,
		serverID:    me,
		currentTerm: 0,
		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   make([]int, len(peers)),
		matchIndex:  make([]int, len(peers)),
		currentRole: FollowerRole,
	}

	// Your initialization code here (2A, 2B, 2C).
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.applyCommittedEntries()
	rf.runAsFollower()
	// start ticker goroutine to start elections
	return rf
}
