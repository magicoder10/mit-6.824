package raft

type Unlocker struct {
	lockID     uint64
	unlockFunc func(lockID uint64, flow Flow, skipCallers int)
}

func (u *Unlocker) unlock(flow Flow) {
	u.unlockFunc(u.lockID, flow, 1)
}
