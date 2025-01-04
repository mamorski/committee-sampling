package mdag

import (
	"sync"

	"github.com/mamorski/committee-sampling/internal/network"
)

const protocolID = "/mdag/1.0.0"

type MerkleDAG interface {
	Gen() error
	Verify() bool
}

type MDAG struct {
	round    int
	rounds   int
	oracle   func([]byte) []byte
	labels   map[int][]byte
	network  network.Network
	messages sync.Map
	ch       chan string
}

func New(rounds int, oracle func([]byte) []byte, network network.Network) *MDAG {
	return &MDAG{
		rounds:  rounds,
		oracle:  oracle,
		network: network,
		labels:  make(map[int][]byte),
		round:   0,
		ch:      make(chan string),
	}
}

func (m *MDAG) HandleMessage(from string, payload []byte) error {
	m.messages.Store(from, payload)
	m.ch <- from
	return nil
}

func (m *MDAG) Gen(sid, vk, s string) error {
	l := m.oracle([]byte(s + vk + sid))
	m.labels[m.round] = l
	m.network.SendProtocolMessage(protocolID, l)
	m.round++

	for {
		if m.round == m.rounds {
			break
		}
	}
	return nil
}

func (m *MDAG) roundWatchdog() {
	for {
		select {
		case <-m.ch:

		}
	}
}
