package network

import (
	"fmt"
	"io"

	"github.com/google/uuid"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	p2p "github.com/mamorski/committee-sampling/internal/network/proto"
)

const exAnteProtocol = "/ex_ante/1.0.0"

type ExAnteProtocol struct {
	node     *Node
	messages chan []byte
	logger   *zap.Logger
}

func (e *ExAnteProtocol) onExAnte(s network.Stream) {
	data := &p2p.ExAnteMessage{}
	buf, err := io.ReadAll(s)
	if err != nil {
		e.logger.Error("Failed to read EX ANTE message", zap.Error(err))
		return
	}
	_ = s.Close()

	err = proto.Unmarshal(buf, data)
	if err != nil {
		e.logger.Error("Failed to unmarshal EX ANTE message", zap.Error(err))
		return
	}

	e.logger.Debug("Received EX ANTE message", zap.Any("data", data))

	if !e.node.authenticateMessage(data, data.MessageData) {
		e.logger.Error("Failed to authenticate message")
		return
	}
	e.messages <- data.Data
}

func NewExAnteProtocol(node *Node) *ExAnteProtocol {
	l := node.logger.Named("ex_ante")
	e := ExAnteProtocol{node: node, logger: l}

	node.SetStreamHandler(exAnteProtocol, e.onExAnte)

	return &e
}

// SendExAnteMessage TODO: Add specific message data instead of byte array
func (e *ExAnteProtocol) SendExAnteMessage(peerID peer.ID, data []byte) error {
	msg := &p2p.ExAnteMessage{
		MessageData: e.node.newMessageData(uuid.New().String(), false),
		Data:        data,
	}

	signature, err := e.node.signProtoMessage(msg)
	if err != nil {
		e.logger.Error("Failed to sign message", zap.Error(err))
		return err
	}

	msg.MessageData.Sign = signature
	ok := e.node.sendProtoMessage(peerID, exAnteProtocol, msg)
	if !ok {
		e.logger.Error("Failed to send message")
		return fmt.Errorf("failed to send request")
	}
	return nil
}

func (e *ExAnteProtocol) GetExAnteMessages() <-chan []byte {
	return e.messages
}
