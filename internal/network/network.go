package network

import (
	"context"
	"github.com/mamorski/committee-sampling/pkg/config"
)

type MessageType int

const (
	MDAG MessageType = iota
	ExAnte
	ExPost
	NeighborRequest
)

type Message struct {
	Type   MessageType
	Data   []byte
	Sender string
}

type Network interface {
	Init(ctx context.Context, conf config.Network) error
	SendMessageToAllPeers(msg Message) error
	ReceiveMessages(t MessageType) <-chan Message
}
