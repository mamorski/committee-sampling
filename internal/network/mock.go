package network

import (
	"bytes"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	p2p "github.com/mamorski/committee-sampling/internal/network/proto"
)

// MockStream simulates a network stream for testing purposes
type MockStream struct {
	network.Stream
	Data      []byte
	Reader    *bytes.Reader
	ReadFunc  func(p []byte) (n int, err error)
	CloseFunc func() error
}

func (m *MockStream) Read(p []byte) (n int, err error) {
	if m.ReadFunc != nil {
		return m.ReadFunc(p)
	}
	if m.Reader == nil {
		m.Reader = bytes.NewReader(m.Data)
	}
	return m.Reader.Read(p)
}

func (m *MockStream) Close() error {
	if m.CloseFunc != nil {
		return m.CloseFunc()
	}
	return nil
}

// MockNode simulates the Node struct for testing purposes
type MockNode struct {
	Host                    host.Host
	AuthenticateMessageFunc func(message proto.Message, data *p2p.MessageData) bool
	NewMessageDataFunc      func(messageId string, gossip bool) *p2p.MessageData
	SignProtoMessageFunc    func(message proto.Message) ([]byte, error)
	SendProtoMessageFunc    func(id peer.ID, p protocol.ID, data proto.Message) bool
	GetLoggerFunc           func() *zap.Logger
	SetStreamHandlerFunc    func(protocolID protocol.ID, handler network.StreamHandler)
}

// Implement the methods used in exante.go

func (m *MockNode) authenticateMessage(message proto.Message, data *p2p.MessageData) bool {
	if m.AuthenticateMessageFunc != nil {
		return m.AuthenticateMessageFunc(message, data)
	}
	return true
}

func (m *MockNode) newMessageData(messageId string, gossip bool) *p2p.MessageData {
	if m.NewMessageDataFunc != nil {
		return m.NewMessageDataFunc(messageId, gossip)
	}
	return &p2p.MessageData{}
}

func (m *MockNode) signProtoMessage(message proto.Message) ([]byte, error) {
	if m.SignProtoMessageFunc != nil {
		return m.SignProtoMessageFunc(message)
	}
	return []byte("signature"), nil
}

func (m *MockNode) sendProtoMessage(id peer.ID, p protocol.ID, data proto.Message) bool {
	if m.SendProtoMessageFunc != nil {
		return m.SendProtoMessageFunc(id, p, data)
	}
	return true
}

func (m *MockNode) GetLogger() *zap.Logger {
	if m.GetLoggerFunc != nil {
		return m.GetLoggerFunc()
	}
	return zap.NewNop()
}

func (m *MockNode) SetStreamHandler(protocolID protocol.ID, handler network.StreamHandler) {
	if m.SetStreamHandlerFunc != nil {
		m.SetStreamHandlerFunc(protocolID, handler)
	}
}
