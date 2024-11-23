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

const exAnteProtocol = "/ex_ante/1.0.0"

type ExAnteProtocol struct {
	node     *Node
	messages chan *p2p.ExAnteMessage
}

func (e *ExAnteProtocol) onExAnte(s network.Stream) {
	data := &p2p.ExAnteMessage{}
	buf, err := io.ReadAll(s)
	if err != nil {
		fmt.Println("Failed to read EX ANTE message: ", err)
		return
	}
	_ = s.Close()

	err = proto.Unmarshal(buf, data)
	if err != nil {
		fmt.Println("Failed to unmarshal EX ANTE message: ", err)
		return
	}

	fmt.Println("Received EX ANTE message: ", data)

	if !e.node.authenticateMessage(data, data.MessageData) {
		fmt.Println("Failed to authenticate message")
		return
	}
	e.messages <- data
}

func NewExAnteProtocol(node *Node) *ExAnteProtocol {
	e := ExAnteProtocol{node: node}

	node.SetStreamHandler(exAnteProtocol, e.onExAnte)

	return &e
}

// SendExAnteMessage TODO: Add specific message data instead of byte array
func (e *ExAnteProtocol) SendExAnteMessage(peerID peer.ID, data []byte) error {
	msg := &p2p.ExAnteMessage{
		MessageData: e.node.NewMessageData(uuid.New().String(), false),
		Data:        data,
	}

	signature, err := e.node.signProtoMessage(msg)
	if err != nil {
		return err
	}

	msg.MessageData.Sign = signature
	ok := e.node.sendProtoMessage(peerID, exAnteProtocol, msg)
	if !ok {
		return fmt.Errorf("failed to send request")
	}
	return nil
}
