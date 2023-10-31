package main

type ConnectionState string

const (
	ConnectedConnectionState    ConnectionState = "Connected"
	DisconnectedConnectionState ConnectionState = "Disconnected"
)

type State struct {
	Network map[int]ConnectionState
}

type Connect struct {
	ServerID int
}

type Disconnect struct {
	ServerID int
}
