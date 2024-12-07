package network

import (
	"fmt"
	"io"

	"github.com/google/uuid"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	p2p "github.com/mamorski/committee-sampling/internal/network/proto"
)

const neighborhoodRequest = "/neighborhood/req/1.0.0"
const neighborhoodResponse = "/neighborhood/resp/1.0.0"

type NeighborhoodProtocol struct {
	node   NodeInterface
	logger *zap.Logger
}

func NewNeighborhoodProtocol(node NodeInterface) *NeighborhoodProtocol {
	l := node.GetLogger().Named("neighborhood")
	n := NeighborhoodProtocol{node: node, logger: l}

	node.SetStreamHandler(neighborhoodRequest, n.onNeighborhoodRequest)
	node.SetStreamHandler(neighborhoodResponse, n.onNeighborhoodResponse)

	return &n
}

func (n *NeighborhoodProtocol) onNeighborhoodRequest(s network.Stream) {
	data := &p2p.NeighborhoodMessage{}
	buf, err := io.ReadAll(s)
	if err != nil {
		n.logger.Error("Failed to read negotiation message", zap.Error(err))
		return
	}
	_ = s.Close()

	err = proto.Unmarshal(buf, data)
	if err != nil {
		n.logger.Error("Failed to unmarshal negotiation message", zap.Error(err))
		return
	}

	n.logger.Debug("Received negotiation request", zap.Any("data", data))

	if !n.node.authenticateMessage(data, data.MessageData) {
		n.logger.Error("Failed to authenticate message")
		return
	}

	resp := &p2p.NeighborhoodMessage{
		MessageData: n.node.newMessageData(data.MessageData.Id, false),
		Accepted:    true,
	}

	err = n.node.addNeighbor(peer.AddrInfo{
		ID:    s.Conn().RemotePeer(),
		Addrs: []multiaddr.Multiaddr{s.Conn().RemoteMultiaddr()},
	})
	if err != nil {
		resp.Accepted = false
	}

	signature, err := n.node.signProtoMessage(resp)
	if err != nil {
		n.logger.Error("Failed to sign response", zap.Error(err))
		return
	}

	resp.MessageData.Sign = signature
	ok := n.node.sendProtoMessage(s.Conn().RemotePeer(), neighborhoodResponse, resp)
	if !ok {
		n.logger.Error("Failed to send response")
	}
}

func (n *NeighborhoodProtocol) onNeighborhoodResponse(s network.Stream) {
	data := &p2p.NeighborhoodMessage{}
	buf, err := io.ReadAll(s)
	if err != nil {
		n.logger.Error("Failed to read negotiation message", zap.Error(err))
		return
	}
	_ = s.Close()

	err = proto.Unmarshal(buf, data)
	if err != nil {
		n.logger.Error("Failed to unmarshal negotiation message", zap.Error(err))
		return
	}

	n.logger.Debug("Received negotiation response", zap.Any("data", data))

	if !n.node.authenticateMessage(data, data.MessageData) {
		n.logger.Error("Failed to authenticate message")
		return
	}

	if !data.Accepted {
		n.logger.Debug("Neighbor rejected", zap.String("peer", s.Conn().RemotePeer().String()))
		return
	}

	err = n.node.addNeighbor(peer.AddrInfo{
		ID:    s.Conn().RemotePeer(),
		Addrs: []multiaddr.Multiaddr{s.Conn().RemoteMultiaddr()},
	})
	if err != nil {
		n.logger.Error("Failed to add neighbor", zap.Error(err))
	}
}

func (n *NeighborhoodProtocol) NeighborRequest(addrInfo peer.AddrInfo) error {
	msg := &p2p.NeighborhoodMessage{
		MessageData: n.node.newMessageData(uuid.New().String(), false),
		Accepted:    true,
	}

	signature, err := n.node.signProtoMessage(msg)
	if err != nil {
		n.logger.Error("Failed to sign request", zap.Error(err))
		return err
	}

	msg.MessageData.Sign = signature
	ok := n.node.send(addrInfo, neighborhoodRequest, msg)
	if !ok {
		n.logger.Error("Failed to send request")
		return fmt.Errorf("failed to send request")
	}

	return nil
}
