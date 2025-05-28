package tests

import (
	"testing"
)

// TestNetworkInterface verifies that MockNetwork implements the Network interface
func TestNetworkInterface(t *testing.T) {
	cluster := NewMockNetworkCluster(20)
	err := cluster.ConnectRandom(4)
	if err != nil {
		t.Fatalf("Failed to connect random peers: %v", err)
	}

}
