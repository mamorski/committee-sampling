package network

import (
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/google/uuid"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	p2p "github.com/mamorski/committee-sampling/internal/network/proto"
	"io"
)

const mDAGProtocol = "/merkle_dag/1.0.0"

type MDAGProtocol struct {
	node     *Node
	messages chan *p2p.MDAGMessage
}

func (m *MDAGProtocol) onMDAG(s network.Stream) {
	data := &p2p.MDAGMessage{}
	buf, err := io.ReadAll(s)
	if err != nil {
		fmt.Println("Failed to read MDAG message: ", err)
		return
	}
	_ = s.Close()

	err = proto.Unmarshal(buf, data)
	if err != nil {
		fmt.Println("Failed to unmarshal MDAG message: ", err)
		return
	}

	fmt.Println("Received MDAG message: ", data)

	if !m.node.authenticateMessage(data, data.MessageData) {
		fmt.Println("Failed to authenticate message")
		return
	}
	m.messages <- data
}

func NewMDAGProtocol(node *Node) *MDAGProtocol {
	m := MDAGProtocol{node: node}

	node.SetStreamHandler(mDAGProtocol, m.onMDAG)

	return &m
}

// SendMDAGMessage TODO: Add specific message data instead of byte array
func (m *MDAGProtocol) SendMDAGMessage(peerID peer.ID, data []byte) error {
	msg := &p2p.MDAGMessage{
		MessageData: m.node.NewMessageData(uuid.New().String(), false),
		Data:        data,
	}

	signature, err := m.node.signProtoMessage(msg)
	if err != nil {
		return err
	}

	msg.MessageData.Sign = signature
	ok := m.node.sendProtoMessage(peerID, exAnteProtocol, msg)
	if !ok {
		return fmt.Errorf("failed to send request")
	}
	return nil
}
