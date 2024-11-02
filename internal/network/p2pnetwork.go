package network

import (
	"context"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
	"sync"

	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/mamorski/committee-sampling/pkg/config"
)

type network struct {
	Host          host.Host
	DHT           *dht.IpfsDHT
	Neighbors     map[peer.ID]struct{}
	lock          sync.Mutex
	messageQueues map[MessageType]chan Message
}

func (p *network) Connect(ctx context.Context, conf config.Network) error {
	h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0"))
	if err != nil {
		return err
	}

	p.Host = h
	p.DHT, err = dht.New(ctx, h, dht.Mode(dht.ModeClient))
	if err != nil {
		return err
	}

	if err := p.DHT.Bootstrap(ctx); err != nil {
		return err
	}

	return nil
}

func (p *network) SendMessageToAllPeers(msg Message) error {
	return nil
}

func (p *network) ReceiveMessages(t MessageType) <-chan Message {
	return nil
}

func New() Network {
	return &network{
		Neighbors: make(map[peer.ID]struct{}),
		messageQueues: map[MessageType]chan Message{
			MDAG:   make(chan Message),
			ExAnte: make(chan Message),
			ExPost: make(chan Message),
		},
	}
}
