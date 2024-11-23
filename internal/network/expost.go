package network

import (
	"fmt"
	"io"

	"github.com/gogo/protobuf/proto"
	"github.com/google/uuid"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"

	p2p "github.com/mamorski/committee-sampling/internal/network/proto"
)

const exPostProtocol = "/ex_post/1.0.0"

type ExPostProtocol struct {
	node     *Node
	messages chan *p2p.ExPostMessage
}

func (e *ExPostProtocol) onExPost(s network.Stream) {
	data := &p2p.ExPostMessage{}
	buf, err := io.ReadAll(s)
	if err != nil {
		fmt.Println("Failed to read EX POST message: ", err)
		return
	}
	_ = s.Close()

	err = proto.Unmarshal(buf, data)
	if err != nil {
		fmt.Println("Failed to unmarshal EX POST message: ", err)
		return
	}

	fmt.Println("Received EX POST message: ", data)

	if !e.node.authenticateMessage(data, data.MessageData) {
		fmt.Println("Failed to authenticate message")
		return
	}
	e.messages <- data
}

func NewExPostProtocol(node *Node) *ExPostProtocol {
	e := ExPostProtocol{node: node}

	node.SetStreamHandler(exPostProtocol, e.onExPost)

	return &e
}

// SendExPostMessage TODO: Add specific message data instead of byte array
func (e *ExPostProtocol) SendExPostMessage(peerID peer.ID, data []byte) error {
	msg := &p2p.ExPostMessage{
		MessageData: e.node.NewMessageData(uuid.New().String(), false),
		Data:        data,
	}

	signature, err := e.node.signProtoMessage(msg)
	if err != nil {
		return err
	}

	msg.MessageData.Sign = signature
	ok := e.node.sendProtoMessage(peerID, exPostProtocol, msg)
	if !ok {
		return fmt.Errorf("failed to send request")
	}
	return nil
}
