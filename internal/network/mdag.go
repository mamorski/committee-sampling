package network

import (
	"fmt"
	"io"

	p2p "github.com/mamorski/committee-sampling/internal/network/proto"

	"github.com/google/uuid"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

const mDAGProtocol = "/merkle_dag/1.0.0"

type MDAGProtocol struct {
	node     *Node
	messages chan []byte
	logger   *zap.Logger
}

func (m *MDAGProtocol) onMDAG(s network.Stream) {
	data := &p2p.MDAGMessage{}
	buf, err := io.ReadAll(s)
	if err != nil {
		m.logger.Error("Failed to read MDAG message", zap.Error(err))
		return
	}
	_ = s.Close()

	err = proto.Unmarshal(buf, data)
	if err != nil {
		m.logger.Error("Failed to unmarshal MDAG message", zap.Error(err))
		return
	}

	m.logger.Debug("Received MDAG message", zap.Any("data", data))

	if !m.node.authenticateMessage(data, data.MessageData) {
		m.logger.Error("Failed to authenticate message")
		return
	}
	m.messages <- data.Data
}

func NewMDAGProtocol(node *Node) *MDAGProtocol {
	l := node.logger.Named("mdag")
	m := MDAGProtocol{node: node, logger: l, messages: make(chan []byte)}

	node.SetStreamHandler(mDAGProtocol, m.onMDAG)

	return &m
}

// SendMDAGMessage TODO: Add specific message data instead of byte array
func (m *MDAGProtocol) SendMDAGMessage(peerID peer.ID, data []byte) error {
	msg := &p2p.MDAGMessage{
		MessageData: m.node.newMessageData(uuid.New().String(), false),
		Data:        data,
	}

	signature, err := m.node.signProtoMessage(msg)
	if err != nil {
		m.logger.Error("Failed to sign message", zap.Error(err))
		return err
	}

	msg.MessageData.Sign = signature
	ok := m.node.sendProtoMessage(peerID, mDAGProtocol, msg)
	if !ok {
		m.logger.Error("Failed to send message")
		return fmt.Errorf("failed to send request")
	}
	return nil
}

func (m *MDAGProtocol) GetMDAGMessages() <-chan []byte {
	return m.messages
}
