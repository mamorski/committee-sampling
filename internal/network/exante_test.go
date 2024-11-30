package network

import (
	"bytes"
	"errors"
	"testing"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	p2p "github.com/mamorski/committee-sampling/internal/network/proto"
)

func TestNewExAnteProtocol(t *testing.T) {
	handlerSet := false

	mockNode := &MockNode{
		SetStreamHandlerFunc: func(protocolID protocol.ID, handler network.StreamHandler) {
			if protocolID != exAnteProtocol {
				t.Errorf("Expected protocolID %s, got %s", exAnteProtocol, protocolID)
			}
			handlerSet = true
		},
		GetLoggerFunc: func() *zap.Logger {
			return zap.NewNop()
		},
	}

	exAnte := NewExAnteProtocol(mockNode)

	if exAnte == nil {
		t.Error("Expected ExAnteProtocol instance, got nil")
	}

	if exAnte.node != mockNode {
		t.Error("Expected node to be set")
	}

	if !handlerSet {
		t.Error("Expected SetStreamHandler to be called")
	}
}

func TestExAnteProtocol_onExAnte(t *testing.T) {
	testData := []byte("test data")
	exAnteMsg := &p2p.ExAnteMessage{
		MessageData: &p2p.MessageData{},
		Data:        testData,
	}
	buf, err := proto.Marshal(exAnteMsg)
	if err != nil {
		t.Fatalf("Failed to marshal exAnteMsg: %v", err)
	}

	mockStream := &MockStream{
		Data: buf,
	}

	messages := make(chan []byte, 1)

	mockNode := &MockNode{
		AuthenticateMessageFunc: func(msg proto.Message, data *p2p.MessageData) bool {
			return true
		},
		GetLoggerFunc: func() *zap.Logger {
			return zap.NewNop()
		},
	}

	exAnte := &ExAnteProtocol{
		node:     mockNode,
		messages: messages,
		logger:   zap.NewNop(),
	}

	exAnte.onExAnte(mockStream)

	select {
	case msg := <-messages:
		if !bytes.Equal(msg, testData) {
			t.Errorf("Expected message %s, got %s", testData, msg)
		}
	default:
		t.Error("Expected message in channel, got none")
	}
}

func TestExAnteProtocol_onExAnte_ReadAllError(t *testing.T) {
	mockStream := &MockStream{
		ReadFunc: func(p []byte) (n int, err error) {
			return 0, errors.New("read error")
		},
	}

	messages := make(chan []byte, 1)

	mockNode := &MockNode{
		GetLoggerFunc: func() *zap.Logger {
			return zap.NewNop()
		},
	}

	exAnte := &ExAnteProtocol{
		node:     mockNode,
		messages: messages,
		logger:   zap.NewNop(),
	}

	exAnte.onExAnte(mockStream)

	select {
	case <-messages:
		t.Error("Expected no message in channel, but got one")
	default:
		// Expected behavior
	}
}

func TestExAnteProtocol_onExAnte_UnmarshalError(t *testing.T) {
	invalidData := []byte("invalid data")

	mockStream := &MockStream{
		Data: invalidData,
	}

	messages := make(chan []byte, 1)

	mockNode := &MockNode{
		GetLoggerFunc: func() *zap.Logger {
			return zap.NewNop()
		},
	}

	exAnte := &ExAnteProtocol{
		node:     mockNode,
		messages: messages,
		logger:   zap.NewNop(),
	}

	exAnte.onExAnte(mockStream)

	select {
	case <-messages:
		t.Error("Expected no message in channel, but got one")
	default:
		// Expected behavior
	}
}

func TestExAnteProtocol_onExAnte_AuthenticateFail(t *testing.T) {
	testData := []byte("test data")
	exAnteMsg := &p2p.ExAnteMessage{
		MessageData: &p2p.MessageData{},
		Data:        testData,
	}
	buf, err := proto.Marshal(exAnteMsg)
	if err != nil {
		t.Fatalf("Failed to marshal exAnteMsg: %v", err)
	}

	mockStream := &MockStream{
		Data: buf,
	}

	messages := make(chan []byte, 1)

	mockNode := &MockNode{
		AuthenticateMessageFunc: func(msg proto.Message, data *p2p.MessageData) bool {
			return false
		},
		GetLoggerFunc: func() *zap.Logger {
			return zap.NewNop()
		},
	}

	exAnte := &ExAnteProtocol{
		node:     mockNode,
		messages: messages,
		logger:   zap.NewNop(),
	}

	exAnte.onExAnte(mockStream)

	select {
	case <-messages:
		t.Error("Expected no message in channel, but got one")
	default:
		// Expected behavior
	}
}

func TestExAnteProtocol_SendExAnteMessage(t *testing.T) {
	mockNode := &MockNode{
		NewMessageDataFunc: func(id string, gossip bool) *p2p.MessageData {
			return &p2p.MessageData{Id: id}
		},
		SignProtoMessageFunc: func(msg proto.Message) ([]byte, error) {
			return []byte("signature"), nil
		},
		SendProtoMessageFunc: func(peerID peer.ID, protocolID protocol.ID, msg proto.Message) bool {
			if protocolID != exAnteProtocol {
				t.Errorf("Expected protocolID %s, got %s", exAnteProtocol, protocolID)
			}
			return true
		},
		GetLoggerFunc: func() *zap.Logger {
			return zap.NewNop()
		},
	}

	exAnte := &ExAnteProtocol{
		node:   mockNode,
		logger: zap.NewNop(),
	}

	peerID := peer.ID("testpeerid")
	data := []byte("test data")

	err := exAnte.SendExAnteMessage(peerID, data)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
}

func TestExAnteProtocol_SendExAnteMessage_SignError(t *testing.T) {
	mockNode := &MockNode{
		NewMessageDataFunc: func(id string, gossip bool) *p2p.MessageData {
			return &p2p.MessageData{Id: id}
		},
		SignProtoMessageFunc: func(msg proto.Message) ([]byte, error) {
			return nil, errors.New("sign error")
		},
		GetLoggerFunc: func() *zap.Logger {
			return zap.NewNop()
		},
	}

	exAnte := &ExAnteProtocol{
		node:   mockNode,
		logger: zap.NewNop(),
	}

	peerID := peer.ID("testpeerid")
	data := []byte("test data")

	err := exAnte.SendExAnteMessage(peerID, data)
	if err == nil {
		t.Error("Expected error, got nil")
	}
}

func TestExAnteProtocol_SendExAnteMessage_SendFail(t *testing.T) {
	mockNode := &MockNode{
		NewMessageDataFunc: func(id string, gossip bool) *p2p.MessageData {
			return &p2p.MessageData{Id: id}
		},
		SignProtoMessageFunc: func(msg proto.Message) ([]byte, error) {
			return []byte("signature"), nil
		},
		SendProtoMessageFunc: func(peerID peer.ID, protocolID protocol.ID, msg proto.Message) bool {
			return false
		},
		GetLoggerFunc: func() *zap.Logger {
			return zap.NewNop()
		},
	}

	exAnte := &ExAnteProtocol{
		node:   mockNode,
		logger: zap.NewNop(),
	}

	peerID := peer.ID("testpeerid")
	data := []byte("test data")

	err := exAnte.SendExAnteMessage(peerID, data)
	if err == nil {
		t.Error("Expected error, got nil")
	}
}

func TestExAnteProtocol_GetExAnteMessages(t *testing.T) {
	messages := make(chan []byte)
	exAnte := &ExAnteProtocol{
		messages: messages,
	}

	ch := exAnte.GetExAnteMessages()
	if ch != messages {
		t.Error("Expected messages channel to be returned")
	}
}
