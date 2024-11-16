package p2pchat

import (
	"bufio"
	"context"
	"fmt"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/p2p/discovery/routing"
	"github.com/libp2p/go-libp2p/p2p/discovery/util"
	"github.com/multiformats/go-multiaddr"
	"log"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
)

const (
	ProtocolID = "/committee-sampling/b103d410-f44e-46f2-ae9e-758a01ed3ea7/1.0.0"
)

type DiscoveryNotifier struct {
	Host      host.Host
	Neighbors sync.Map
	ctx       context.Context
	dht       *dht.IpfsDHT
}

// HandlePeerFound connects to a discovered peer
func (d *DiscoveryNotifier) HandlePeerFound(pi peer.AddrInfo) {
	fmt.Printf("Discovered peer: %s\n", pi.ID.String())
	err := d.Host.Connect(context.Background(), pi)
	if err != nil {
		fmt.Printf("Failed to connect to peer %s: %s\n", pi.ID.String(), err)
	} else {
		d.Neighbors.Store(pi.ID, pi)
	}
}

func CreateHost(ctx context.Context) (*DiscoveryNotifier, error) {
	h, err := libp2p.New()
	if err != nil {
		return nil, fmt.Errorf("failed to create h: %w", err)
	}
	return &DiscoveryNotifier{Host: h, ctx: ctx}, nil
}

func (d *DiscoveryNotifier) SetStreamHandler(host host.Host) {
	host.SetStreamHandler(ProtocolID, func(s network.Stream) {
		// Handle incoming stream
		log.Printf("Got a new stream from: %s\n", s.Conn().RemotePeer().String())
		d.handleStream(s)
	})
}

func (d *DiscoveryNotifier) handleStream(s network.Stream) {
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
		if d.ctx.Done() != nil {
			return
		}
	}
}

func (d *DiscoveryNotifier) discover() {

	fmt.Println("Announcing ourselves...")
	routingDiscovery := routing.NewRoutingDiscovery(d.dht)
	util.Advertise(d.ctx, routingDiscovery, ProtocolID)
	fmt.Println("Successfully announced!")

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-d.ctx.Done():
			return
		case <-ticker.C:
			fmt.Println("Searching for other peers...")
			peerChan, err := routingDiscovery.FindPeers(d.ctx, ProtocolID)
			if err != nil {
				panic(err)
			}
			for p := range peerChan {
				if p.ID == d.Host.ID() {
					continue
				}
				d.HandlePeerFound(p)
			}
		}
	}
}

func (d *DiscoveryNotifier) StartDHT(ctx context.Context, peers []multiaddr.Multiaddr) error {
	bootstrapPeers := make([]peer.AddrInfo, len(peers))
	for i, addr := range peers {
		peerInfo, _ := peer.AddrInfoFromP2pAddr(addr)
		bootstrapPeers[i] = *peerInfo
	}

	kDHT, err := dht.New(ctx, d.Host, dht.BootstrapPeers(bootstrapPeers...))
	if err != nil {
		panic(err)
	}

	fmt.Println("Bootstrapping the DHT")
	if err = kDHT.Bootstrap(ctx); err != nil {
		panic(err)
	}
	time.Sleep(5 * time.Second)
	d.dht = kDHT

	go d.discover()
	return nil
}

func SendPeriodicMessages(ctx context.Context, host host.Host, neighbors *sync.Map) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			fmt.Println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
			fmt.Println("Get out of here")
			fmt.Println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
			return
		case <-ticker.C:
			neighbors.Range(func(key, value interface{}) bool {
				peerID := key.(peer.ID)
				addrInfo := value.(peer.AddrInfo)

				err := host.Connect(ctx, addrInfo)
				if err != nil {
					fmt.Printf("Failed to connect to peer: %s\n", err)
					return true
				}

				stream, err := host.NewStream(ctx, peerID, ProtocolID)
				if err != nil {
					fmt.Printf("Failed to create stream: %s\n", err)
					return true
				}
				_, err = stream.Write([]byte("Hello from " + host.ID().String() + "\n"))
				if err != nil {
					fmt.Printf("Failed to send message: %s\n", err)
					stream.Reset()
				} else {
					stream.Close()
				}

				return true
			})
		}
	}
}
