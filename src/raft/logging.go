package raft

import (
	"fmt"
	"log"
	"path/filepath"
	"runtime"
)

func init() {
	log.SetFlags(log.Lmicroseconds)
}

type Flow string

const (
	FollowerFlow       Flow = "Follower"
	CandidateFlow      Flow = "Candidate"
	LeaderFlow         Flow = "Leader"
	SharedFlow         Flow = "Shared"
	HeartbeatFlow      Flow = "Heartbeat"
	ElectionFlow       Flow = "Election"
	LogReplicationFlow Flow = "LogReplication"
	CommitFlow         Flow = "Commit"
	StateFlow          Flow = "State"
	TerminationFlow    Flow = "Termination"
	ApplyFlow          Flow = "Apply"
	ApplyEntryFlow     Flow = "ApplyEntry"
	SnapshotFlow       Flow = "Snapshot"
)

type LogLevel string

const (
	DebugLevel LogLevel = "Debug"
	InfoLevel  LogLevel = "Info"
	WarnLevel  LogLevel = "Warn"
	ErrorLevel LogLevel = "Error"
	FatalLevel LogLevel = "Fatal"

	OffLevel LogLevel = "Off"
)

var logLevelRank = map[LogLevel]int{
	DebugLevel: 0,
	InfoLevel:  1,
	WarnLevel:  2,
	ErrorLevel: 3,
	FatalLevel: 4,
	OffLevel:   5,
}

const visibleLogLevel LogLevel = InfoLevel

var visibleFlows = map[Flow]bool{
	FollowerFlow:       true,
	CandidateFlow:      true,
	LeaderFlow:         true,
	SharedFlow:         true,
	HeartbeatFlow:      true,
	ElectionFlow:       true,
	LogReplicationFlow: true,
	CommitFlow:         true,
	StateFlow:          true,
	TerminationFlow:    true,
	ApplyFlow:          true,
	ApplyEntryFlow:     true,
	SnapshotFlow:       true,
}

type LogContext struct {
	ServerID      int
	Role          Role
	Term          int
	Flow          Flow
	NumGoroutines int
}

type MessageContext struct {
	LogContext
	SenderID   int
	ReceiverID int
	MessageID  uint64
}

func Log(ct LogContext, level LogLevel, format string, objs ...interface{}) {
	LogAndSkipCallers(ct, level, 1, format, objs...)
}

func LogAndSkipCallers(ct LogContext, level LogLevel, skipCallers int, format string, objs ...interface{}) {
	if logLevelRank[level] < logLevelRank[visibleLogLevel] {
		return
	}

	_, ok := visibleFlows[ct.Flow]
	if !ok {
		return
	}

	_, filePath, lineNum, _ := runtime.Caller(skipCallers + 1)
	_, fileName := filepath.Split(filePath)

	log.Printf("%v:%v [serverID:%v role:%v term:%v flow:%v goroutines:%v] %v\n",
		fileName,
		lineNum,
		ct.ServerID,
		ct.Role,
		ct.Term,
		ct.Flow,
		ct.NumGoroutines,
		fmt.Sprintf(format, objs...))
}

func LogMessage(ct MessageContext, level LogLevel, format string, objs ...interface{}) {
	MessageAndSkipCallers(ct, level, 1, format, objs...)
}

func MessageAndSkipCallers(ct MessageContext, level LogLevel, skipCallers int, format string, objs ...interface{}) {
	if logLevelRank[level] < logLevelRank[visibleLogLevel] {
		return
	}

	_, ok := visibleFlows[ct.Flow]
	if !ok {
		return
	}

	_, filePath, lineNum, _ := runtime.Caller(skipCallers + 1)
	_, fileName := filepath.Split(filePath)

	log.Printf("%v:%v [serverID:%v role:%v term:%v flow:%v goroutines:%v sender:%v receiver:%v messageID:%v] %v\n",
		fileName,
		lineNum,
		ct.ServerID,
		ct.Role,
		ct.Term,
		ct.Flow,
		ct.NumGoroutines,
		ct.SenderID,
		ct.ReceiverID,
		ct.MessageID,
		fmt.Sprintf(format, objs...))
}
