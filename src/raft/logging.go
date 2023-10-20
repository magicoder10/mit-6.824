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
	HeartbeatFlow      Flow = "Heartbeat"
	ElectionFlow       Flow = "Election"
	LogReplicationFlow Flow = "LogReplication"
	CommitFlow         Flow = "Commit"
	StateFlow          Flow = "State"
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
)

var logLevelRank = map[LogLevel]int{
	DebugLevel: 0,
	InfoLevel:  1,
	WarnLevel:  2,
	ErrorLevel: 3,
	FatalLevel: 4,
}

const visibleLogLevel LogLevel = DebugLevel

func Log(serverID int, role Role, term int, level LogLevel, flow Flow, format string, objs ...interface{}) {
	LogAndSkipCallers(serverID, role, term, level, flow, 1, format, objs...)
}

func LogAndSkipCallers(serverID int, role Role, term int, level LogLevel, flow Flow, skipCallers int, format string, objs ...interface{}) {
	if logLevelRank[level] < logLevelRank[visibleLogLevel] {
		return
	}

	_, filePath, lineNum, _ := runtime.Caller(skipCallers + 1)
	_, fileName := filepath.Split(filePath)
	log.Printf("%v:%v [serverID:%v role:%v term:%v flow:%v] %v\n",
		fileName,
		lineNum,
		serverID,
		role,
		term,
		flow,
		fmt.Sprintf(format, objs...))
}
