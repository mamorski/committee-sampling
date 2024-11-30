package network

type MessageType int

const (
	MDAG MessageType = iota
	ExAnte
	ExPost
)

type Message struct {
	Type MessageType
	Data []byte
}

type Network interface {
	GetPeers() []string
	SendMessageToPeers(msg Message, peers []string)
	ReceiveMessages(t MessageType) <-chan []byte
}
