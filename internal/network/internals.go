package network

import (
	"context"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	pproto "github.com/mamorski/committee-sampling/pkg/proto"
)

// Authenticate incoming p2p message
// message: a proto go data object
// data: common p2p message data
func (n *P2PNode) authenticateMessage(message proto.Message, data *pproto.MessageData) bool {
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
	peerID, err := peer.Decode(data.NodeId)
	if err != nil {
		n.logger.Error("Failed to decode node id from base58", zap.Error(err))
		return false
	}

	// verify the data was authored by the signing peer identified by the public key
	// and signature included in the message
	return n.verifyData(bin, sign, peerID, data.NodePubKey)
}

// sign an outgoing p2p message payload
func (n *P2PNode) signProtoMessage(message proto.Message) ([]byte, error) {
	data, err := proto.Marshal(message)
	if err != nil {
		n.logger.Error("Failed to marshal pb message", zap.Error(err))
		return nil, err
	}

	return n.signData(data)
}

// sign binary data using the local node's private key
func (n *P2PNode) signData(data []byte) ([]byte, error) {
	key := n.host.Peerstore().PrivKey(n.host.ID())
	res, err := key.Sign(data)
	return res, err
}

// Verify incoming p2p message data integrity
// data: data to verify
// signature: author signature provided in the message payload
// peerId: author peer id from the message payload
// pubKeyData: author public key from the message payload
func (n *P2PNode) verifyData(data []byte, signature []byte, peerID peer.ID, pubKeyData []byte) bool {
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
	if idFromKey != peerID {
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
func (n *P2PNode) newMessageData(messageID string, gossip bool) *pproto.MessageData {
	// Add proto bin data for a message author public key
	// this is useful for authenticating messages forwarded by a node authored by another node
	nodePubKey, err := crypto.MarshalPublicKey(n.host.Peerstore().PubKey(n.host.ID()))

	if err != nil {
		n.logger.Fatal("Failed to get public key for sender from local peer store", zap.Error(err))
	}

	return &pproto.MessageData{
		ClientVersion: clientVersion,
		NodeId:        n.host.ID().String(),
		NodePubKey:    nodePubKey,
		Timestamp:     time.Now().Unix(),
		Id:            messageID,
		Gossip:        gossip,
	}
}

// sendProtoMessage helper method - writes a proto go data object to a network stream
// data: reference of proto go data object to send (not the object itself)
// s: network stream to write the data to
func (n *P2PNode) sendProtoMessage(id peer.ID, p protocol.ID, data proto.Message) bool {
	addrInfo, ok := n.neighbors[id]
	if !ok {
		n.logger.Error("Failed to find peer", zap.String("peer", id.String()))
		return false
	}

	return n.send(addrInfo, p, data)
}

func (n *P2PNode) send(addrInfo peer.AddrInfo, p protocol.ID, data proto.Message) bool {
	s, err := n.host.NewStream(context.Background(), addrInfo.ID, p)
	if err != nil {
		// Attempt to establish a connection and retry once
		if errConn := n.host.Connect(context.Background(), addrInfo); errConn != nil {
			n.logger.Error("Failed to connect to peer", zap.Error(errConn))
			go n.verifyNeighbor(addrInfo.ID)
			return false
		}

		s, err = n.host.NewStream(context.Background(), addrInfo.ID, p)
		if err != nil {
			n.logger.Error("Failed to create stream", zap.Error(err))
			go n.verifyNeighbor(addrInfo.ID)
			return false
		}
	}

	defer func(s network.Stream) {
		_ = s.Close()
	}(s)

	// Write the proto message to the stream using Google's proto library
	buf, err := proto.Marshal(data)
	if err != nil {
		n.logger.Error("Failed to marshal proto message", zap.Error(err))
		_ = s.Reset()
		// no connectivity check for marshal failure
		return false
	}

	_, err = s.Write(buf)
	if err != nil {
		n.logger.Error("Failed to write message to stream", zap.Error(err))
		_ = s.Reset()
		go n.verifyNeighbor(addrInfo.ID)
		return false
	}
	return true
}

// verifyConnectivity tries to establish a new stream up to connectivityRetries times.
// Returns true if any attempt succeeds, otherwise false.
func (n *P2PNode) verifyConnectivity(peerID peer.ID) bool {
	retries := n.connectivityRetries
	if retries <= 0 {
		retries = 3
	}

	n.mu.Lock()
	addrInfo, ok := n.neighbors[peerID]
	n.mu.Unlock()
	if !ok {
		// If not found, no need to drop
		return true
	}

	for attempt := 0; attempt < retries; attempt++ {
		// try to connect
		// use a short context to avoid long blocking
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		// attempt connect
		err := n.host.Connect(ctx, addrInfo)
		if err == nil {
			cancel()
			return true
		}

		cancel()
	}
	n.logger.Warn("Connectivity verification failed; dropping neighbor", zap.String("peer_id", peerID.String()))
	return false
}

func (n *P2PNode) verifyNeighbor(peerID peer.ID) {
	if !n.verifyConnectivity(peerID) {
		n.dropNeighbor(peerID)
	}
}
