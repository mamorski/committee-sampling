package network

import (
	"bufio"
	"context"
	"crypto/rand"
	"fmt"
	"github.com/multiformats/go-multiaddr"
	"strings"
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
	nCnt          int
	lock          sync.Mutex
}

func (n *node) addNeighbor(addrInfo peer.AddrInfo) {
	// Check if already connected
	if _, ok := n.Neighbors.Load(addrInfo.ID); ok {
		return
	}

	err := n.Host.Connect(n.ctx, addrInfo)
	if err != nil {
		return
	}
	n.Neighbors.Store(addrInfo.ID, addrInfo)
	n.nCnt++
}

func (n *node) HandlePeerFound(info peer.AddrInfo) {
	if info.ID > n.Host.ID() {
		return
	}

	n.lock.Lock()
	defer n.lock.Unlock()
	if n.nCnt >= 2 {
		//fmt.Printf("I'm %s, already connected to %d peers, not connecting to %s\n", n.Host.ID(), n.nCnt, info.ID)
		return
	}

	fmt.Printf("[HandlePeerFound] I'm %s, connecting to %s\n", n.Host.ID(), info.ID)
	n.addNeighbor(info)
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

	//fmt.Printf("Host created. We are: %s, address: %s\n", h.ID(), h.Addrs())

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

func (n *node) SendMessageToPeer(peerID peer.ID, msg Message) error {
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

	n.lock.Lock()
	n.addNeighbor(peer.AddrInfo{
		ID:    s.Conn().RemotePeer(),
		Addrs: []multiaddr.Multiaddr{s.Conn().RemoteMultiaddr()},
	})
	n.lock.Unlock()

	str, err := buf.ReadString('\n')
	if err != nil {
		_ = s.Reset()
		fmt.Println("Stream closed")
		return
	}

	fmt.Printf("Received message from %s: %s", s.Conn().RemotePeer().String(), str)
}

func (n *node) serializeMessage(msg Message) []byte {
	return nil
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
			var nb []string
			n.(*node).Neighbors.Range(func(key, value interface{}) bool {
				nb = append(nb, key.(peer.ID).String())
				return true
			})
			fmt.Printf("Shutting down node %s, I have %d neighbors and they are %s\n", n.(*node).Host.ID(), n.(*node).nCnt, strings.Join(nb, ", "))
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
		ctx:  ctx,
		nCnt: 0,
	}
}
