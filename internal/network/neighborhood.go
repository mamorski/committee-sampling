package network

import (
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/google/uuid"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"io"

	p2p "github.com/mamorski/committee-sampling/internal/network/proto"
)

const neighborhoodRequest = "/neighborhood/req/1.0.0"
const neighborhoodResponse = "/neighborhood/resp/1.0.0"

type NeighborhoodProtocol struct {
	node *Node
}

func NewNeighborhoodProtocol(node *Node) *NeighborhoodProtocol {
	n := NeighborhoodProtocol{node: node}

	node.SetStreamHandler(neighborhoodRequest, n.onNeighborhoodRequest)
	node.SetStreamHandler(neighborhoodResponse, n.onNeighborhoodResponse)

	return &n
}

func (n *NeighborhoodProtocol) onNeighborhoodRequest(s network.Stream) {
	data := &p2p.NeighborhoodMessage{}
	buf, err := io.ReadAll(s)
	if err != nil {
		fmt.Println("Failed to read negotiation message: ", err)
		return
	}
	_ = s.Close()

	err = proto.Unmarshal(buf, data)
	if err != nil {
		fmt.Println("Failed to unmarshal negotiation message: ", err)
		return
	}

	fmt.Println("Received negotiation request: ", data)

	if !n.node.authenticateMessage(data, data.MessageData) {
		fmt.Println("Failed to authenticate message")
		return
	}

	resp := &p2p.NeighborhoodMessage{
		MessageData: n.node.NewMessageData(data.MessageData.Id, false),
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
		fmt.Println("Failed to sign response: ", err)
		return
	}

	resp.MessageData.Sign = signature
	ok := n.node.sendProtoMessage(s.Conn().RemotePeer(), neighborhoodResponse, resp)
	if !ok {
		fmt.Println("Failed to send response")
	}
}

func (n *NeighborhoodProtocol) onNeighborhoodResponse(s network.Stream) {
	data := &p2p.NeighborhoodMessage{}
	buf, err := io.ReadAll(s)
	if err != nil {
		fmt.Println("Failed to read negotiation message: ", err)
		return
	}
	_ = s.Close()

	err = proto.Unmarshal(buf, data)
	if err != nil {
		fmt.Println("Failed to unmarshal negotiation message: ", err)
		return
	}

	fmt.Println("Received negotiation response: ", data)

	if !n.node.authenticateMessage(data, data.MessageData) {
		fmt.Println("Failed to authenticate message")
		return
	}

	if !data.Accepted {
		fmt.Println("Neighbor rejected")
		return
	}

	err = n.node.addNeighbor(peer.AddrInfo{
		ID:    s.Conn().RemotePeer(),
		Addrs: []multiaddr.Multiaddr{s.Conn().RemoteMultiaddr()},
	})
	if err != nil {
		fmt.Println("Failed to add neighbor")
	}
}

func (n *NeighborhoodProtocol) NeighborRequest(addrInfo peer.AddrInfo) error {
	msg := &p2p.NeighborhoodMessage{
		MessageData: n.node.NewMessageData(uuid.New().String(), false),
	}

	signature, err := n.node.signProtoMessage(msg)
	if err != nil {
		return err
	}

	msg.MessageData.Sign = signature
	ok := n.node.send(addrInfo, neighborhoodRequest, msg)
	if !ok {
		return fmt.Errorf("failed to send request")
	}

	return nil
}
