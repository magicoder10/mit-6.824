package raft

import (
	"6.5840/telemetry"
)

type Unlocker struct {
	trace      telemetry.Trace
	lockID     uint64
	unlockFunc func(lockID uint64, flow Flow, skipCallers int)
}

func (u *Unlocker) unlock(flow Flow) {
	u.unlockFunc(u.lockID, flow, 1)
}
