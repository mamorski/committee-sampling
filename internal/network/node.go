package network

import (
	"context"
	"crypto/rand"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	p2p "github.com/mamorski/committee-sampling/internal/network/proto"
	"github.com/mamorski/committee-sampling/pkg/config"
)

// node client version
const clientVersion = "go-p2p-node/0.0.1"
const protocolID = "/committee-sampling/mdns/1.0.0"

// Node type - a p2p host implementing one or more p2p protocols
type Node struct {
	host.Host // lib-p2p host
	*MDAGProtocol
	*ExAnteProtocol
	*ExPostProtocol
	*NeighborhoodProtocol
	ctx          context.Context
	nCnt         int
	lock         sync.Mutex
	logger       *zap.Logger
	Neighbors    sync.Map
	maxNeighbors int
}

// New Create a new node with its implemented protocols
// host: lib-p2p host
// ctx: context for the node
// maxNeighbors: maximum number of neighbors to connect to, used only by HandlePeerFound, all incoming connections are accepted
func New(ctx context.Context, conf config.Network, logger *zap.Logger) *Node {
	l := logger.Named("network")
	node := &Node{ctx: ctx, nCnt: 0, maxNeighbors: conf.MaxNeighbors, logger: l}

	err := node.init(ctx, conf)
	if err != nil {
		l.Fatal("Failed to initialize node", zap.Error(err))
	}

	node.ExAnteProtocol = NewExAnteProtocol(node)
	node.ExPostProtocol = NewExPostProtocol(node)
	node.NeighborhoodProtocol = NewNeighborhoodProtocol(node)
	node.MDAGProtocol = NewMDAGProtocol(node)

	return node
}

func (n *Node) GetPeers() []string {
	var peers []string
	n.Neighbors.Range(func(key, value interface{}) bool {
		peers = append(peers, key.(peer.ID).String())
		return true
	})
	return peers
}

func (n *Node) SendMessageToPeers(msg Message, peers []string) {
	for _, p := range peers {
		peerID, err := peer.Decode(p)
		if err != nil {
			n.logger.Error("Failed to decode peer ID", zap.Error(err))
		}

		switch msg.Type {
		case MDAG:
			err = n.SendMDAGMessage(peerID, msg.Data)
		case ExAnte:
			err = n.SendExAnteMessage(peerID, msg.Data)
		case ExPost:
			err = n.SendExPostMessage(peerID, msg.Data)
		default:
			n.logger.Error("Unknown message type", zap.Any("type", msg.Type))
		}

		if err != nil {
			n.logger.Error("Failed to send message", zap.Error(err))
		}
	}

	return
}

func (n *Node) ReceiveMessages(t MessageType) <-chan []byte {

	switch t {
	case MDAG:
		return n.GetMDAGMessages()
	case ExAnte:
		return n.GetExAnteMessages()
	case ExPost:
		return n.GetExPostMessages()
	default:
		n.logger.Error("Unknown message type", zap.Any("type", t))
	}

	return nil
}

func (n *Node) init(ctx context.Context, _ config.Network) error {

	r := rand.Reader
	priv, _, err := crypto.GenerateKeyPairWithReader(crypto.RSA, 2048, r)
	if err != nil {
		return err
	}

	h, err := libp2p.New(
		libp2p.Identity(priv),
	)
	if err != nil {
		return err
	}

	mdnsService := mdns.NewMdnsService(h, protocolID, n)
	if err := mdnsService.Start(); err != nil {
		return err
	}

	go func() {
		<-ctx.Done()
		n.logger.Info("Shutting down mdns service", zap.Int("current", n.nCnt))
		err := mdnsService.Close()
		if err != nil {
			n.logger.Error("Failed to close mdns service", zap.Error(err))
		}
	}()

	n.Host = h
	return nil
}

func (n *Node) HandlePeerFound(info peer.AddrInfo) {
	// TODO: remove this sleep, it's only for testing purposes
	time.Sleep(2 * time.Second)

	if info.ID > n.Host.ID() {
		n.logger.Debug("Ignoring peer with higher ID", zap.String("peer", info.ID.String()))
		return
	}

	n.lock.Lock()
	defer n.lock.Unlock()
	if n.nCnt >= n.maxNeighbors {
		n.logger.Debug(
			"Already connected to max number of neighbors",
			zap.Int("max", n.maxNeighbors),
			zap.Int("current", n.nCnt),
		)
		return
	}

	n.logger.Debug(
		"Connecting to peer",
		zap.String("peer", info.ID.String()),
		zap.String("address", info.Addrs[0].String()),
	)

	err := n.NeighborRequest(info)
	if err != nil {
		n.logger.Error("Failed to send neighbor request", zap.Error(err))
	}
}

func (n *Node) addNeighbor(addrInfo peer.AddrInfo) error {
	// Check if already connected
	if _, ok := n.Neighbors.Load(addrInfo.ID); ok {
		return nil
	}

	err := n.Host.Connect(n.ctx, addrInfo)
	if err != nil {
		n.logger.Error("Failed to connect to neighbor", zap.Error(err))
		return err
	}

	n.Neighbors.Store(addrInfo.ID, addrInfo)
	n.nCnt++
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
		n.logger.Error("Failed to marshal pb message", zap.Error(err))
		return false
	}

	// restore sig in message data (for possible future use)
	data.Sign = sign

	// restore peer id binary format from base58 encoded node id data
	peerId, err := peer.Decode(data.NodeId)
	if err != nil {
		n.logger.Error("Failed to decode node id from base58", zap.Error(err))
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
		n.logger.Error("Failed to marshal pb message", zap.Error(err))
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
		n.logger.Error("Failed to extract key from message key data", zap.Error(err))
		return false
	}

	// extract node id from the provided public key
	idFromKey, err := peer.IDFromPublicKey(key)

	if err != nil {
		n.logger.Error("Failed to extract peer id from public key", zap.Error(err))
		return false
	}

	// verify that message author node id matches the provided node public key
	if idFromKey != peerId {
		n.logger.Error("Node id and provided public key mismatch")
		return false
	}

	res, err := key.Verify(data, signature)
	if err != nil {
		n.logger.Error("Failed to verify message signature", zap.Error(err))
		return false
	}

	return res
}

// newMessageData helper method - generate message data shared between all node's p2p protocols
// messageId: unique for requests, copied from request for responses
func (n *Node) newMessageData(messageId string, gossip bool) *p2p.MessageData {
	// Add protobuf bin data for message author public key
	// this is useful for authenticating  messages forwarded by a node authored by another node
	nodePubKey, err := crypto.MarshalPublicKey(n.Peerstore().PubKey(n.ID()))

	if err != nil {
		n.logger.Fatal("Failed to get public key for sender from local peer store", zap.Error(err))
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
		n.logger.Error("Failed to find peer", zap.String("peer", id.String()))
		return false
	}

	return n.send(addrInfo.(peer.AddrInfo), p, data)
}

func (n *Node) send(addrInfo peer.AddrInfo, p protocol.ID, data proto.Message) bool {
	err := n.Connect(context.Background(), addrInfo)
	if err != nil {
		n.logger.Error("Failed to connect to peer", zap.Error(err))
		return false
	}

	s, err := n.NewStream(context.Background(), addrInfo.ID, p)
	if err != nil {
		n.logger.Error("Failed to create stream", zap.Error(err))
		return false
	}

	defer func(s network.Stream) {
		_ = s.Close()
	}(s)

	// Write the protobuf message to the stream using Google's proto library
	buf, err := proto.Marshal(data)
	if err != nil {
		n.logger.Error("Failed to marshal protobuf message", zap.Error(err))
		_ = s.Reset()
		return false
	}

	_, err = s.Write(buf)
	if err != nil {
		n.logger.Error("Failed to write message to stream", zap.Error(err))
		_ = s.Reset()
		return false
	}
	return true
}
