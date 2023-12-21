package kvraft

type Err string

const (
	OkErr             Err = "Ok"
	ErrWrongLeaderErr Err = "ErrWrongLeader"
	ErrCancelledErr   Err = "ErrCancelled"
)
