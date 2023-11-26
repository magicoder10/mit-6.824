package kvraft

type Err string

const (
	OkErr             Err = "Ok"
	ErrNoKey          Err = "ErrNoKey"
	ErrWrongLeaderErr Err = "ErrWrongLeader"
	ErrCancelledErr   Err = "ErrCancelled"
)
