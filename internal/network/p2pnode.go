package network

import (
	"bufio"
	"context"
	"crypto/rand"
	"fmt"
	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"
	"github.com/mamorski/committee-sampling/pkg/config"
	"sync"
	"time"
)

const (
	ProtocolID = "/committee-sampling/1.0.0"
)

type node struct {
	Host          host.Host
	DHT           *dht.IpfsDHT
	Neighbors     map[peer.ID]*bufio.ReadWriter
	lock          sync.Mutex
	messageQueues map[MessageType]chan Message
	Mutex         sync.Mutex
}

func (n *node) connectToPeer(ctx context.Context, peerAddr peer.AddrInfo) error {
	if n.Host.ID() == peerAddr.ID {
		return nil
	}

	err := n.Host.Connect(ctx, peerAddr)
	if err != nil {
		fmt.Printf("Error connecting to peer: %s\n", err)
		return err
	}

	// open a stream, this stream will be handled by handleStream other end
	s, err := n.Host.NewStream(ctx, peerAddr.ID, ProtocolID)
	if err != nil {
		fmt.Printf("Error opening stream: %s\n", err)
		return err
	}

	n.Mutex.Lock()
	if len(n.Neighbors) < 15 {
		fmt.Printf("Connected to %s\n", peerAddr)
		rw := bufio.NewReadWriter(bufio.NewReader(s), bufio.NewWriter(s))
		n.Neighbors[peerAddr.ID] = rw
	} else {
		fmt.Printf("Too many neighbors, rejecting %s\n", peerAddr)
		//_ = s.Reset()
	}
	n.Mutex.Unlock()

	return nil
}

func (n *node) HandlePeerFound(info peer.AddrInfo) {
	fmt.Printf("Found peer: %s\n", info.ID)
	if info.ID > n.Host.ID() {
		fmt.Println("Found peer:", info, " id is greater than us, wait for it to connect to us")
		return
	}

	if err := n.connectToPeer(context.Background(), info); err != nil {
		fmt.Printf("Error connecting to peer: %s\n", err)
	}
}

func (n *node) Init(ctx context.Context, _ config.Network) error {

	r := rand.Reader
	priv, _, err := crypto.GenerateKeyPairWithReader(crypto.RSA, 2048, r)
	if err != nil {
		panic(err)
	}

	h, err := libp2p.New(
		libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0"),
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
	peerID := s.Conn().RemotePeer()
	fmt.Printf("Got a new stream from %s\n", peerID)
	defer func(s network.Stream) {
		_ = s.Close()
	}(s)

	n.Mutex.Lock()
	if len(n.Neighbors) < 15 {
		rw := bufio.NewReadWriter(bufio.NewReader(s), bufio.NewWriter(s))
		n.Neighbors[peerID] = rw
	} else {
		fmt.Printf("Too many neighbors, rejecting %s\n", peerID)
		//_ = s.Reset()
	}
	n.Mutex.Unlock()

	reader := bufio.NewReader(s)
	for {
		msg, err := reader.ReadString('\n')
		if err != nil {
			fmt.Printf("Error reading from stream: %s\n", err)
			break
		}
		// TODO: implement handling message functionality
		fmt.Printf("Received message from %s: %s\n", peerID, msg)
	}

	n.Mutex.Lock()
	delete(n.Neighbors, peerID)
	n.Mutex.Unlock()
}

func Run(ctx context.Context) {
	n := New()
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
			n.RelayMessage("Hello from " + n.(*node).Host.ID().String())
		}
	}
}

func (n *node) RelayMessage(msg string) {
	n.Mutex.Lock()
	for _, rw := range n.Neighbors {
		_, err := rw.WriteString(msg)
		if err != nil {
			fmt.Printf("Error writing to buffer: %s\n", err)
			panic(err)
		}

		err = rw.Flush()
		if err != nil {
			fmt.Printf("Error flushing buffer: %s\n", err)
			panic(err)
		}
	}
	n.Mutex.Unlock()
}

func New() Network {
	return &node{
		Neighbors: make(map[peer.ID]*bufio.ReadWriter),
		messageQueues: map[MessageType]chan Message{
			MDAG:   make(chan Message),
			ExAnte: make(chan Message),
			ExPost: make(chan Message),
		},
	}
}
