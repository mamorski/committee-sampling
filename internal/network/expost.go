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

const exPostProtocol = "/ex_post/1.0.0"

type ExPostProtocol struct {
	node     NodeInterface
	messages chan []byte
	logger   *zap.Logger
}

func NewExPostProtocol(node NodeInterface) *ExPostProtocol {
	l := node.GetLogger().Named("ex_post")
	e := ExPostProtocol{node: node, logger: l}

	node.SetStreamHandler(exPostProtocol, e.onExPost)

	return &e
}

func (e *ExPostProtocol) onExPost(s network.Stream) {
	data := &p2p.ExPostMessage{}
	buf, err := io.ReadAll(s)
	if err != nil {
		e.logger.Error("Failed to read EX POST message", zap.Error(err))
		return
	}
	_ = s.Close()

	err = proto.Unmarshal(buf, data)
	if err != nil {
		e.logger.Error("Failed to unmarshal EX POST message", zap.Error(err))
		return
	}

	e.logger.Debug("Received EX POST message", zap.Any("data", data))

	if !e.node.authenticateMessage(data, data.MessageData) {
		e.logger.Error("Failed to authenticate message")
		return
	}
	e.messages <- data.Data
}

// SendExPostMessage TODO: Add specific message data instead of byte array
func (e *ExPostProtocol) SendExPostMessage(peerID peer.ID, data []byte) error {
	msg := &p2p.ExPostMessage{
		MessageData: e.node.newMessageData(uuid.New().String(), false),
		Data:        data,
	}

	signature, err := e.node.signProtoMessage(msg)
	if err != nil {
		e.logger.Error("Failed to sign message", zap.Error(err))
		return err
	}

	msg.MessageData.Sign = signature
	ok := e.node.sendProtoMessage(peerID, exPostProtocol, msg)
	if !ok {
		e.logger.Error("Failed to send message")
		return fmt.Errorf("failed to send request")
	}

	return nil
}

func (e *ExPostProtocol) GetExPostMessages() <-chan []byte {
	return e.messages
}
