package raft

import (
	"fmt"

	internalFmt "6.5840/fmt"
	"6.5840/telemetry"
)

type RequestVoteArgs struct {
	Trace        telemetry.Trace
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

var _ fmt.Stringer = (*RequestVoteArgs)(nil)

func (r RequestVoteArgs) String() string {
	return fmt.Sprintf("[RequestVoteArgs Term:%v, CandidateID:%v, LastLogIndex:%v, LastLogTerm:%v]",
		r.Term,
		r.CandidateID,
		r.LastLogIndex,
		r.LastLogTerm)
}

type RequestVoteReply struct {
	IsCanceled  bool
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Trace             telemetry.Trace
	Term              int
	LeaderID          int
	PrevLogIndex      int
	PrevLogTerm       int
	LogEntries        []LogEntry
	LeaderCommitIndex int
}

var _ fmt.Stringer = (*AppendEntriesArgs)(nil)

func (a AppendEntriesArgs) String() string {
	return fmt.Sprintf("[AppendEntriesArgs Term:%v, LeaderID:%v, PrevLogIndex:%v, PrevLogTerm:%v, LogEntries:%v, LeaderCommitIndex:%v]",
		a.Term,
		a.LeaderID,
		a.PrevLogIndex,
		a.PrevLogTerm,
		a.LogEntries,
		a.LeaderCommitIndex)
}

type AppendEntriesReply struct {
	IsCanceled           bool
	Term                 int
	Success              bool
	ConflictTerm         *int
	ConflictIndex        int
	ConflictLogLastIndex int
}

var _ fmt.Stringer = (*AppendEntriesReply)(nil)

func (a AppendEntriesReply) String() string {
	return fmt.Sprintf("[AppendEntriesReply IsCanceled:%v, Term:%v, Success:%v,  ConflictTerm:%v, ConflictIndex:%v, ConflictLogLastIndex:%v]",
		a.IsCanceled,
		a.Term,
		a.Success,
		internalFmt.FromPtr(a.ConflictTerm),
		a.ConflictIndex,
		a.ConflictLogLastIndex)
}

type InstallSnapshotArgs struct {
	Trace             telemetry.Trace
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

var _ fmt.Stringer = (*InstallSnapshotArgs)(nil)

func (i InstallSnapshotArgs) String() string {
	return fmt.Sprintf("[InstallSnapshotArgs Term:%v, LeaderID:%v, LastIncludedIndex:%v, LastIncludedTerm:%v]",
		i.Term,
		i.LeaderID,
		i.LastIncludedIndex,
		i.LastIncludedTerm)
}

type InstallSnapshotReply struct {
	IsCanceled bool
	Term       int
}
