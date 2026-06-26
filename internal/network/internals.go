package network

import (
	"context"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-msgio"
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
func (n *P2PNode) newMessageData() *pproto.MessageData {
	// NodeId and NodePubKey are constant per node and cached at construction
	// (n.nodeID, n.nodePubKey); the author pubkey lets nodes authenticate
	// messages forwarded on behalf of another node. Each call returns a fresh
	// struct so the per-message Sign can be set under concurrent fan-out.
	return &pproto.MessageData{
		NodeId:     n.nodeID,
		NodePubKey: n.nodePubKey,
	}
}

// send marshals and writes data to peer over protocol p. It returns the number of
// bytes written (the marshaled envelope size) and true on success, or 0 and false
// on any failure.
func (n *P2PNode) send(addrInfo peer.AddrInfo, p protocol.ID, data proto.Message) (int64, bool) {
	s, ok := n.openStream(addrInfo, p)
	if !ok {
		return 0, false
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
		return 0, false
	}

	_, err = s.Write(buf)
	if err != nil {
		n.logger.Error("Failed to write message to stream", zap.Error(err))
		_ = s.Reset()
		return 0, false
	}

	return int64(len(buf)), true
}

// openStream opens a stream to the peer, dialing and retrying once if the first
// attempt fails. Stream creation is bounded by sendTimeout so a stuck peer
// cannot block the caller indefinitely.
func (n *P2PNode) openStream(addrInfo peer.AddrInfo, p protocol.ID) (network.Stream, bool) {
	ctx, cancel := context.WithTimeout(n.ctx, n.sendTimeout)
	defer cancel()

	s, err := n.host.NewStream(ctx, addrInfo.ID, p)
	if err != nil {
		// Attempt to establish a connection and retry once.
		if errConn := n.host.Connect(ctx, addrInfo); errConn != nil {
			n.logger.Error("Failed to connect to peer", zap.Error(errConn))
			return nil, false
		}

		s, err = n.host.NewStream(ctx, addrInfo.ID, p)
		if err != nil {
			n.logger.Error("Failed to create stream", zap.Error(err))
			return nil, false
		}
	}

	return s, true
}

// sendFramed writes data as a length-prefixed frame over a reusable stream kept
// in the pool, keyed by (peer, protocol). The stream stays open so the receiver
// reads successive frames without per-message negotiation/close overhead. On a
// write error the stream is reset and evicted, then reopened once; the next send
// reopens lazily. Returns the marshaled envelope size and whether the send
// succeeded.
func (n *P2PNode) sendFramed(addrInfo peer.AddrInfo, p protocol.ID, data proto.Message) (int64, bool) {
	buf, err := proto.Marshal(data)
	if err != nil {
		n.logger.Error("Failed to marshal proto message", zap.Error(err))
		return 0, false
	}

	key := streamKey{peer: addrInfo.ID, proto: p}
	n.poolMu.Lock()
	ps := n.streamPool[key]
	if ps == nil {
		ps = &pooledStream{}
		n.streamPool[key] = ps
	}
	n.poolMu.Unlock()

	ps.mu.Lock()
	defer ps.mu.Unlock()

	// Try once on the existing stream, then once more after a forced reopen.
	// A pooled stream whose connection was torn down (peer churn, connmgr trim)
	// fails the first write; that is expected and recovered by reopening, so the
	// per-attempt failure is logged at debug. Only a failure that survives the
	// reopen is a real send error.
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		if ps.s == nil {
			s, ok := n.openStream(addrInfo, p)
			if !ok {
				return 0, false
			}
			ps.s = s
			ps.w = msgio.NewVarintWriter(s)
		}

		if err := ps.w.WriteMsg(buf); err != nil {
			lastErr = err
			n.logger.Debug("Framed write failed, reopening stream", zap.Error(err))
			_ = ps.s.Reset()
			ps.s = nil
			ps.w = nil
			continue
		}

		return int64(len(buf)), true
	}

	n.logger.Error("Failed to write framed message after reopen", zap.Error(lastErr))
	return 0, false
}

func (n *P2PNode) remainingOutboundCapacity() int {
	n.mu.Lock()
	currentNeighbors := len(n.neighbors)
	n.mu.Unlock()

	return n.maxOutbound - currentNeighbors
}
