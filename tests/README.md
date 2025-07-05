# Mock Network Implementation

This directory contains a mock network implementation using `github.com/libp2p/go-libp2p/p2p/net/mock` that implements the `Network` interface from `internal/network/host.go`.

## Features

The mock network provides:

- **MockNetwork**: A single mock network node that implements the `Network` interface
- **MockNetworkCluster**: A cluster of connected mock networks for testing distributed scenarios
- **Thread-safe operations**: All operations are protected with mutexes
- **Flexible topology**: Support for full mesh, ring, random, and custom topologies
- **Configurable connectivity**: Control the maximum number of neighbors per node in random topologies

## Usage

### Single Mock Network

```go
mockNet := NewMockNetwork()
defer mockNet.Close()

// Register a message handler
mockNet.RegisterHandler("echo", func(from string, payload []byte) error {
    fmt.Printf("Received from %s: %s\n", from, string(payload))
    return nil
})

// Add neighbors manually
mockNet.AddNeighbor("peer1")
mockNet.AddNeighbor("peer2")

// Send messages to all neighbors
mockNet.SendProtocolMessage("echo", []byte("Hello, network!"))
```

### Network Cluster

```go
// Create a cluster of 5 nodes
cluster := NewMockNetworkCluster(5)
defer cluster.Close()

// Connect all nodes in a full mesh
err := cluster.ConnectAll()
if err != nil {
    log.Fatal(err)
}

// Or connect in a ring topology
err = cluster.ConnectRing()
if err != nil {
    log.Fatal(err)
}

// Or create random connections with max 3 neighbors per node
err = cluster.ConnectRandom(3)
if err != nil {
    log.Fatal(err)
}

// Get individual networks
node0 := cluster.GetNetwork(0)
node1 := cluster.GetNetwork(1)
```

### Two-Node Communication

```go
node1 := NewMockNetwork()
node2 := NewMockNetwork()
defer node1.Close()
defer node2.Close()

// Connect the two nodes
err := node1.ConnectTo(node2)
if err != nil {
    log.Fatal(err)
}

// Set up message handler on node2
node2.RegisterHandler("ping", func(from string, payload []byte) error {
    fmt.Printf("Node2 received ping from %s: %s\n", from, string(payload))
    return nil
})

// Send message from node1 to node2
node1.SendProtocolMessage("ping", []byte("ping message"))
```

## Interface Compliance

The `MockNetwork` struct implements the `Network` interface:

```go
type Network interface {
    RegisterHandler(protocolID string, handler MessageHandler)
    SendProtocolMessage(protocolID string, data []byte)
    GetNeighbors() []string
    GetNodeID() string
    Close() error
}
```

## Additional Methods

The mock implementation provides additional methods for testing:

- `AddNeighbor(peerID string)`: Manually add a neighbor
- `RemoveNeighbor(peerID string)`: Remove a neighbor
- `ConnectTo(other *MockNetwork)`: Connect to another mock network
- `SimulateMessage(fromPeerID, protocolID string, data []byte)`: Simulate receiving a message
- `GetHost()`: Get the underlying libp2p host
- `GetMocknet()`: Get the underlying mocknet instance

## Testing Utilities

### CreateTestNetwork

```go
// Create different network topologies
cluster, err := CreateTestNetwork(5, "full")  // Full mesh
cluster, err := CreateTestNetwork(4, "ring")  // Ring topology
cluster, err := CreateTestNetwork(3, "")      // No connections
```

### CreateRandomNetwork

```go
// Create a network with random connections
// 10 nodes, each with at most 3 neighbors
cluster, err := CreateRandomNetwork(10, 3)
if err != nil {
    log.Fatal(err)
}
defer cluster.Close()

// Verify the network properties
for i := 0; i < cluster.Size(); i++ {
    net := cluster.GetNetwork(i)
    neighbors := net.GetNeighbors()
    fmt.Printf("Node %d has %d neighbors\n", i, len(neighbors))
}
```

### SetupEchoHandlers

```go
cluster := NewMockNetworkCluster(3)
SetupEchoHandlers(cluster)  // Sets up echo handlers on all nodes
```

## Running Tests

```bash
go test ./tests/ -v
```

## Example Test Scenarios

The mock network is perfect for testing:

- **Message broadcasting**: Test how messages propagate through the network
- **Network partitions**: Simulate network splits and merges
- **Protocol implementations**: Test custom protocols without real networking
- **Failure scenarios**: Test how the system handles node failures
- **Performance testing**: Measure message latency and throughput in controlled environments

## Thread Safety

All operations on the mock network are thread-safe. You can safely:

- Send messages from multiple goroutines
- Add/remove neighbors concurrently
- Register handlers while messages are being processed

## Limitations

- The mock network doesn't simulate real network latency or failures
- All communication is in-memory and instantaneous
- No actual libp2p protocols are used (just the interface)
- Network topology changes are immediate without discovery delays 