package main

type ConnectionState string

const (
	ConnectedConnectionState    ConnectionState = "Connected"
	DisconnectedConnectionState ConnectionState = "Disconnected"
)

type ServerState string

const (
	UpServerState   ServerState = "Up"
	DownServerState ServerState = "Down"
)

type State struct {
	Network map[int]ConnectionState
	Servers map[int]ServerState
}

type Connect struct {
	ServerID int
}

type Disconnect struct {
	ServerID int
}

type Start struct {
	ServerID int
}

type Crash struct {
	ServerID int
}

func newState() State {
	return State{
		Network: make(map[int]ConnectionState),
		Servers: make(map[int]ServerState),
	}
}
