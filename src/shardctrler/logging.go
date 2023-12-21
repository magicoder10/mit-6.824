package shardctrler

import (
	"fmt"
	"log"
	"path/filepath"
	"runtime"

	internalFmt "6.5840/fmt"
	"6.5840/telemetry"
)

func init() {
	log.SetFlags(log.Lmicroseconds)
}

type Flow string

const (
	InitFlow         Flow = "Init"
	ExecuteOpFlow    Flow = "ExecuteOp"
	TerminationFlow  Flow = "Termination"
	ApplyFlow        Flow = "Apply"
	ApplyCommandFlow Flow = "ApplyCommand"
	RaftStateFlow    Flow = "RaftState"
)

type LogLevel string

const (
	DebugLevel LogLevel = "Debug"
	InfoLevel  LogLevel = "Info"
	WarnLevel  LogLevel = "Warn"
	ErrorLevel LogLevel = "Error"
	FatalLevel LogLevel = "Fatal"
	OffLevel   LogLevel = "Off"
)

var logLevelRank = map[LogLevel]int{
	DebugLevel: 0,
	InfoLevel:  1,
	WarnLevel:  2,
	ErrorLevel: 3,
	FatalLevel: 4,
	OffLevel:   5,
}

const visibleLogLevel = InfoLevel

var visibleFlows = map[Flow]bool{
	InitFlow:         true,
	ExecuteOpFlow:    true,
	ApplyFlow:        true,
	ApplyCommandFlow: true,
	RaftStateFlow:    true,
	TerminationFlow:  true,
}

type LogContext struct {
	EndpointNamespace string
	EndpointID        int
	Flow              Flow
	Trace             *telemetry.Trace
}

type MessageContext struct {
	LogContext
	SenderID   int
	ReceiverID int
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

	log.Printf("%v:%v [endpoint:(%v/%v) flow:%v trace:(%v) goroutines:%v] %v\n",
		fileName,
		lineNum,
		ct.EndpointNamespace,
		ct.EndpointID,
		ct.Flow,
		internalFmt.FromPtr(ct.Trace),
		runtime.NumGoroutine(),
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

	log.Printf("%v:%v [endpoint:(%v/%v) flow:%v trace:(%v) goroutines:%v sender:%v receiver:%v] %v\n",
		fileName,
		lineNum,
		ct.EndpointNamespace,
		ct.EndpointID,
		ct.Flow,
		internalFmt.FromPtr(ct.Trace),
		runtime.NumGoroutine(),
		ct.SenderID,
		ct.ReceiverID,
		fmt.Sprintf(format, objs...))
}
