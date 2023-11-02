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

var _ fmt.Stringer = (*AppendEntriesReply)(nil)

func (a AppendEntriesReply) String() string {
	return fmt.Sprintf("[AppendEntriesReply Term:%v, Success:%v, HasPrevLogConflict:%v, ConflictTerm:%v, ConflictIndex:%v, ConflictLogLastIndex:%v]",
		a.Term,
		a.Success,
		a.HasPrevLogConflict,
		fmtPtr(a.ConflictTerm),
		a.ConflictIndex,
		a.ConflictLogLastIndex)
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}
