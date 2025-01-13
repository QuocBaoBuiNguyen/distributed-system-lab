package event

type Message struct {
	FromPeerID string
	Type       MessageType
}

type MessageType uint32

func (m *Message) IsPongMessage() bool {
	return m.Type == PONG
}

func (m *Message) IsAliveMessage() bool {
	return m.Type == ALIVE
}

const (
	PING MessageType = iota + 1
	PONG
	ALIVE
	ELECTED
	ELECTION
	OK
)
