package network

import (
	"bufio"
	"context"
	"crypto/rand"
	"fmt"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"

	"github.com/mamorski/committee-sampling/pkg/config"
)

const (
	ProtocolID = "/committee-sampling/1.0.0"
)

type node struct {
	Host          host.Host
	Neighbors     sync.Map
	messageQueues map[MessageType]chan Message
	ctx           context.Context
}

func (n *node) HandlePeerFound(info peer.AddrInfo) {
	fmt.Printf("Found peer: %s\n", info.ID)
	if info.ID > n.Host.ID() {
		fmt.Println("Found peer:", info, " id is greater than us, wait for it to connect to us")
		return
	}

	err := n.Host.Connect(n.ctx, info)
	if err != nil {
		fmt.Printf("I'm %s, got error when trying to connect to peer: %s\n", n.Host, err)
		return
	}
	n.Neighbors.Store(info.ID, info)
}

func (n *node) Init(ctx context.Context, _ config.Network) error {

	r := rand.Reader
	priv, _, err := crypto.GenerateKeyPairWithReader(crypto.RSA, 2048, r)
	if err != nil {
		panic(err)
	}

	h, err := libp2p.New(
		libp2p.Identity(priv),
	)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Host created. We are: %s, address: %s\n", h.ID(), h.Addrs())

	h.SetStreamHandler(ProtocolID, n.handleStream)

	mdnsService := mdns.NewMdnsService(h, ProtocolID, n)
	if err := mdnsService.Start(); err != nil {
		panic(err)
	}

	go func() {
		<-ctx.Done()
		_ = mdnsService.Close()
	}()

	n.Host = h
	return nil
}

func (n *node) SendMessageToAllPeers(msg Message) error {
	return nil
}

func (n *node) ReceiveMessages(t MessageType) <-chan Message {
	return nil
}

func (n *node) handleStream(s network.Stream) {
	defer func(s network.Stream) {
		_ = s.Close()
	}(s)
	buf := bufio.NewReader(s)
	for {
		str, err := buf.ReadString('\n')
		if err != nil {
			fmt.Printf("Stream closed by %s\n", s.Conn().RemotePeer().String())
			_ = s.Reset()
			return
		}
		fmt.Printf("Received message from %s: %s", s.Conn().RemotePeer().String(), str)
		if n.ctx.Done() != nil {
			return
		}
	}
}

func Run(ctx context.Context) {
	n := New(ctx)
	if err := n.Init(ctx, config.Network{}); err != nil {
		panic(err)
	}

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n.(*node).Neighbors.Range(func(key, value interface{}) bool {
				peerID := key.(peer.ID)
				addrInfo := value.(peer.AddrInfo)

				err := n.(*node).Host.Connect(ctx, addrInfo)
				if err != nil {
					fmt.Printf("Failed to connect to peer: %s\n", err)
					return true
				}

				stream, err := n.(*node).Host.NewStream(ctx, peerID, ProtocolID)
				if err != nil {
					fmt.Printf("Failed to create stream: %s\n", err)
					return true
				}
				_, err = stream.Write([]byte("Hello from " + n.(*node).Host.ID().String() + "\n"))
				if err != nil {
					fmt.Printf("Failed to send message: %s\n", err)
					_ = stream.Reset()
				} else {
					_ = stream.Close()
				}

				return true
			})
		}
	}
}

func New(ctx context.Context) Network {
	return &node{
		messageQueues: map[MessageType]chan Message{
			MDAG:   make(chan Message),
			ExAnte: make(chan Message),
			ExPost: make(chan Message),
		},
		ctx: ctx,
	}
}
