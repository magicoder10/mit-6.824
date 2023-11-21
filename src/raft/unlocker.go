package raft

import (
	"time"

	"6.5840/telemetry"
)

type Unlocker struct {
	trace      telemetry.Trace
	lockID     uint64
	lockedAt   time.Time
	unlockFunc func(lockID uint64, flow Flow, lockDuration time.Duration, skipCallers int)
}

func (u *Unlocker) unlock(flow Flow) {
	u.unlockFunc(u.lockID, flow, time.Now().Sub(u.lockedAt), 1)
}
