package network

import (
	"context"
	"crypto/rand"
	"fmt"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"
	"github.com/mamorski/committee-sampling/pkg/config"
	"log"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	p2p "github.com/mamorski/committee-sampling/internal/network/proto"

	ggio "github.com/gogo/protobuf/io"
	"github.com/gogo/protobuf/proto"
)

// node client version
const clientVersion = "go-p2p-node/0.0.1"
const protocolID = "/committee-sampling/mdns/1.0.0"

// Node type - a p2p host implementing one or more p2p protocols
type Node struct {
	host.Host // lib-p2p host
	*ExAnteProtocol
	*ExPostProtocol
	*NeighborhoodProtocol
	*MDAGProtocol
	Neighbors    sync.Map
	ctx          context.Context
	nCnt         int
	maxNeighbors int
	lock         sync.Mutex
}

// NewNode Create a new node with its implemented protocols
// host: lib-p2p host
// ctx: context for the node
// maxNeighbors: maximum number of neighbors to connect to, used only by HandlePeerFound, all incoming connections are accepted
func NewNode(ctx context.Context, conf config.Network) *Node {
	node := &Node{ctx: ctx, nCnt: 0, maxNeighbors: conf.MaxNeighbors}
	_ = node.Init(ctx, conf)
	node.ExAnteProtocol = NewExAnteProtocol(node)
	node.ExPostProtocol = NewExPostProtocol(node)
	node.NeighborhoodProtocol = NewNeighborhoodProtocol(node)
	node.MDAGProtocol = NewMDAGProtocol(node)
	return node
}

func (n *Node) Init(ctx context.Context, _ config.Network) error {

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

	mdnsService := mdns.NewMdnsService(h, protocolID, n)
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

func (n *Node) HandlePeerFound(info peer.AddrInfo) {
	if info.ID > n.Host.ID() {
		return
	}

	n.lock.Lock()
	defer n.lock.Unlock()
	if n.nCnt >= n.maxNeighbors {
		return
	}

	fmt.Printf("[HandlePeerFound] I'm %s, connecting to %s\n", n.Host.ID(), info.ID)
	_ = n.NeighborRequest(info)
}

func (n *Node) addNeighbor(addrInfo peer.AddrInfo) error {
	// Check if already connected
	if _, ok := n.Neighbors.Load(addrInfo.ID); ok {
		return nil
	}

	err := n.Host.Connect(n.ctx, addrInfo)
	if err != nil {
		return err
	}
	n.Neighbors.Store(addrInfo.ID, addrInfo)
	return nil
}

// Authenticate incoming p2p message
// message: a protobuf go data object
// data: common p2p message data
func (n *Node) authenticateMessage(message proto.Message, data *p2p.MessageData) bool {
	// store a temp ref to signature and remove it from message data
	// sign is a string to allow easy reset to zero-value (empty string)
	sign := data.Sign
	data.Sign = nil

	// marshall data without the signature to protobufs3 binary format
	bin, err := proto.Marshal(message)
	if err != nil {
		log.Println(err, "failed to marshal pb message")
		return false
	}

	// restore sig in message data (for possible future use)
	data.Sign = sign

	// restore peer id binary format from base58 encoded node id data
	peerId, err := peer.Decode(data.NodeId)
	if err != nil {
		log.Println(err, "Failed to decode node id from base58")
		return false
	}

	// verify the data was authored by the signing peer identified by the public key
	// and signature included in the message
	return n.verifyData(bin, sign, peerId, data.NodePubKey)
}

// sign an outgoing p2p message payload
func (n *Node) signProtoMessage(message proto.Message) ([]byte, error) {
	data, err := proto.Marshal(message)
	if err != nil {
		return nil, err
	}
	return n.signData(data)
}

// sign binary data using the local node's private key
func (n *Node) signData(data []byte) ([]byte, error) {
	key := n.Peerstore().PrivKey(n.ID())
	res, err := key.Sign(data)
	return res, err
}

// Verify incoming p2p message data integrity
// data: data to verify
// signature: author signature provided in the message payload
// peerId: author peer id from the message payload
// pubKeyData: author public key from the message payload
func (n *Node) verifyData(data []byte, signature []byte, peerId peer.ID, pubKeyData []byte) bool {
	key, err := crypto.UnmarshalPublicKey(pubKeyData)
	if err != nil {
		log.Println(err, "Failed to extract key from message key data")
		return false
	}

	// extract node id from the provided public key
	idFromKey, err := peer.IDFromPublicKey(key)

	if err != nil {
		log.Println(err, "Failed to extract peer id from public key")
		return false
	}

	// verify that message author node id matches the provided node public key
	if idFromKey != peerId {
		log.Println(err, "Node id and provided public key mismatch")
		return false
	}

	res, err := key.Verify(data, signature)
	if err != nil {
		log.Println(err, "Error authenticating data")
		return false
	}

	return res
}

// NewMessageData helper method - generate message data shared between all node's p2p protocols
// messageId: unique for requests, copied from request for responses
func (n *Node) NewMessageData(messageId string, gossip bool) *p2p.MessageData {
	// Add protobuf bin data for message author public key
	// this is useful for authenticating  messages forwarded by a node authored by another node
	nodePubKey, err := crypto.MarshalPublicKey(n.Peerstore().PubKey(n.ID()))

	if err != nil {
		panic("Failed to get public key for sender from local peer store.")
	}

	return &p2p.MessageData{ClientVersion: clientVersion,
		NodeId:     n.ID().String(),
		NodePubKey: nodePubKey,
		Timestamp:  time.Now().Unix(),
		Id:         messageId,
		Gossip:     gossip}
}

// helper method - writes a protobuf go data object to a network stream
// data: reference of protobuf go data object to send (not the object itself)
// s: network stream to write the data to
func (n *Node) sendProtoMessage(id peer.ID, p protocol.ID, data proto.Message) bool {
	addrInfo, ok := n.Neighbors.Load(id)
	if !ok {
		fmt.Printf("Failed to find peer %s\n", id)
		return false
	}

	return n.send(addrInfo.(peer.AddrInfo), p, data)
}

func (n *Node) send(addrInfo peer.AddrInfo, p protocol.ID, data proto.Message) bool {
	err := n.Connect(context.Background(), addrInfo)
	if err != nil {
		fmt.Printf("Failed to connect to peer: %s\n", err)
		return false
	}

	s, err := n.NewStream(context.Background(), addrInfo.ID, p)
	if err != nil {
		log.Println(err)
		return false
	}
	defer func(s network.Stream) {
		_ = s.Close()
	}(s)

	writer := ggio.NewFullWriter(s)
	err = writer.WriteMsg(data)
	if err != nil {
		log.Println(err)
		_ = s.Reset()
		return false
	}
	return true
}
