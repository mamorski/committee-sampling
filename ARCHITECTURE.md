# Framework Architecture Description

## 1. Introduction

This document provides a comprehensive architectural description of the Committee Sampling Framework, a distributed system implementation for setup-free committee election protocols. The framework combines Verifiable Random Functions (VRF), Verifiable Delay Functions (VDF), and resource-bounded proofs to form short-lived committees from large-scale networks while maintaining subquadratic communication complexity.

The implementation is written in Go and leverages the libp2p networking stack to provide a robust, production-ready system suitable for research and deployment in decentralized environments.

## 2. System Architecture Overview

### 2.1 Architectural Style

The framework follows a modular, layered architecture with clear separation of concerns. The design adheres to the following principles:

- **Modularity**: Each protocol component is encapsulated in its own package with well-defined interfaces
- **Concurrent Execution**: Protocol phases execute concurrently where possible, utilizing Go's goroutine model
- **Time-based Synchronization**: Global clock synchronization via NTP enables coordinated protocol progression
- **Event-driven Communication**: Network interactions follow asynchronous, event-driven patterns
- **Observable Execution**: Comprehensive logging and metrics enable detailed protocol analysis

### 2.2 High-Level System Structure

The system architecture comprises four primary layers:

1. **Application Layer**: Binary entry point and bootstrap orchestration
2. **Protocol Layer**: Core committee election and timestamp protocol implementations
3. **Infrastructure Layer**: Networking, synchronization, and cryptographic primitives
4. **Configuration Layer**: Runtime parameter management and validation

```
┌─────────────────────────────────────────────────────────────┐
│                    Application Layer                         │
│           (main.go, bootstrap.go)                           │
└─────────────────────────────────────────────────────────────┘
                            │
┌─────────────────────────────────────────────────────────────┐
│                     Protocol Layer                           │
│   ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  │
│   │   MDAG   │  │ Ex-Ante  │  │ Ex-Post  │  │   GCE    │  │
│   └──────────┘  └──────────┘  └──────────┘  └──────────┘  │
│   ┌──────────┐  ┌──────────────────────────────────────┐  │
│   │ RB-ExP   │  │      Resource Proof Layer            │  │
│   └──────────┘  └──────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
                            │
┌─────────────────────────────────────────────────────────────┐
│                 Infrastructure Layer                         │
│   ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  │
│   │ Network  │  │   Sync   │  │   VRF    │  │   VDF    │  │
│   └──────────┘  └──────────┘  └──────────┘  └──────────┘  │
│   ┌──────────┐  ┌──────────────────────────────────────┐  │
│   │ Metrics  │  │        Thread Pool                   │  │
│   └──────────┘  └──────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
                            │
┌─────────────────────────────────────────────────────────────┐
│                 Configuration Layer                          │
│                   (config.go)                               │
└─────────────────────────────────────────────────────────────┘
```

## 3. Core Modules

### 3.1 Bootstrap Module

**Location**: `internal/boot/bootstrap.go`

**Purpose**: Orchestrates protocol initialization and execution by wiring together all subsystem components.

**Key Responsibilities**:

- **Component Initialization**: Instantiates and configures all protocol modules with appropriate parameters
- **Filter Function Construction**: Implements the VRF-Sortition-Grading filter function that validates VDF and VRF proofs
- **Grade Function Implementation**: Computes participant grades based on VRF output and weight estimates
- **Protocol Orchestration**: Coordinates the initialization and committee election phases
- **Dependency Injection**: Provides dependencies to protocol components through constructor injection

**Implementation Details**:

The Bootstrap module implements two core cryptographic functions:

1. **Filter Function** (`filterF`): Verifies both VDF and VRF proofs according to the specification:
   ```
   VDF.Verify(H(id||vk||ch), φ_vdf, π_vdf) ∧ VRF.Verify(H(φ_vdf||sid), φ_vrf, π_vrf, vk)
   ```

2. **Grade Function** (`gradeF`): Computes participant grade using the formula:
   ```
   g_i = d + 1 − (W_i − n·2^λ/(φ+1))·(1/ΔW)
   grade = min{d+1, ⌊g_i⌋}
   ```
   Where φ is parsed from the VRF output as a big-endian integer.

**Protocol Flow**:

1. Initialize VRF keypair
2. Execute RB-ExP.Generate() to obtain challenge and proofs
3. Evaluate VDF on challenge
4. Execute GCE.CommitteeElection() with local state
5. Output elected committee members

### 3.2 Network Module

**Location**: `internal/network/host.go`, `internal/network/discovery/`

**Purpose**: Provides peer-to-peer communication infrastructure using libp2p with custom graph building algorithms.

**Key Components**:

#### 3.2.1 P2PNode

Implements the core networking functionality with the following features:

- **Identity Management**: Ed25519 keypair generation for peer identification
- **Connection Management**: Maintains neighbor set with configurable degree bounds
- **Message Authentication**: Cryptographic signing and verification of all protocol messages
- **Protocol Multiplexing**: Support for multiple protocol streams per connection

**Graph Building Algorithm**:

The network implements a multi-round graph building protocol:

1. **Discovery Phase**: 
   - DHT-based peer discovery
   - Collection of potential neighbors
   - Duration: Configurable via `graph_discovery_timeout`

2. **Building Phase** (Even/Odd Round Pattern):
   - **Even Rounds**: Process incoming graph proposals
   - **Odd Rounds**: Send graph proposals to candidates
   - Target degree: Randomly selected from [2×max_outbound, 3×max_outbound]
   - Supports graceful rejection via graph drop messages

3. **Finalization**:
   - Process pending drop messages
   - Freeze neighbor set for protocol execution

**Security Features**:

- Message size limits (1 MiB per message)
- Cryptographic message authentication using private key signatures
- Session-based protocol isolation via session ID
- Neighbor validation for all incoming protocol messages

**Simulation Capabilities**:

- Configurable packet loss simulation (`drop_on_send`)
- Cryptographically secure randomness for drop decisions
- Per-recipient drop probability configuration

#### 3.2.2 Discovery Subsystem

**Location**: `internal/network/discovery/`

Implements Kademlia DHT-based peer discovery:

- Bootstrap node connectivity
- Periodic peer announcements
- Continuous peer discovery stream
- Graceful degradation on bootstrap failure

### 3.3 MDAG (Merkle Directed Acyclic Graph) Module

**Location**: `internal/mdag/mdag.go`

**Purpose**: Implements the Merkle DAG construction used by both Ex-Ante and Ex-Post timestamp protocols for communication-efficient distributed timestamping.

**Algorithm**:

The MDAG protocol operates in rounds, computing labels through iterative hash aggregation:

```
Round 0: L_0 = H(sid || vk || v_1 || ... || v_n)
Round r: L_r = H(sort([L_{r-1}^local] ∪ {L_{r-1}^j | j ∈ neighbors}))
```

**Implementation Characteristics**:

- **Concurrent Message Collection**: Asynchronous message reception during round execution
- **Deterministic Ordering**: Lexicographic sorting of labels ensures consistency
- **State Management**: Thread-safe access to received messages and computed labels
- **Round Synchronization**: Integration with global synchronizer for coordinated progression
- **Metrics Integration**: Per-round message counting for analysis

**Data Structures**:

- `messages`: Map of round number to received labels
- `state`: Three-dimensional slice storing labels per round
- `computedLabels`: Sequence of locally computed labels
- `currentLabel`: Most recent label computation

**Protocol Lifecycle**:

1. **Initialization**: Compute L_0 from session parameters
2. **Broadcast**: Send L_0 to all neighbors
3. **For each round r ∈ [1, rounds]**:
   - Wait for synchronizer signal
   - Collect messages from round r-1
   - Sort and concatenate labels
   - Compute L_r = H(concatenated labels)
   - Broadcast L_r (except in final round)
4. **Output**: Return state matrix

### 3.4 Ex-Post Timestamp Module

**Location**: `internal/expost/expost.go`

**Purpose**: Implements the Ex-Post timestamp protocol for distributed timestamp generation with post-hoc verification.

**Architecture**:

#### 3.4.1 Generation Phase

**Algorithm**:
1. Generate random string r_i of length λ
2. Execute MDAG.Generate(sid, vk, r_i) for R = d·D rounds
3. Output (σ_i, ℓ_R) where ℓ_R is the label at round R

**Implementation**: Uses cryptographically secure randomness for challenge generation.

#### 3.4.2 Verification Phase

**Parallel Message Processing**:

The verification phase employs a thread pool for concurrent message validation:

- Thread pool size: min(2×NumCPU, 4)
- Tasks submitted per round for parallel execution
- Synchronization point between rounds

**Message Validation Pipeline**:

For each received message, the verifier checks:

1. **Filter Function**: Verify resource proof and VRF/VDF proofs
2. **Grade Function**: Compute sender grade (g > 0 required)
3. **Value Verification**: ℓ_{R-r+1} = H(merkle_path[last])
4. **Merkle Path Validation**:
   - Local label ℓ_{R-r} must be in merkle_path[0]
   - Each layer i: H(merkle_path[i-1]) ∈ merkle_path[i]

**Grade Computation**:
```
g = min(d - r/D, grade_function(sid, vk, value, aux_key, aux_local))
```

**Message Propagation**:

Valid messages are propagated with augmented Merkle paths:
- Prepend local label σ_{R-r} to merkle_path
- Forward to all neighbors
- Update committee with (vk, value, id, g)

**Performance Optimizations**:

- Object pooling for protobuf messages (reduces GC pressure)
- Reuse of auxiliary data structures
- Explicit memory management for large message batches

### 3.5 Ex-Ante Timestamp Module

**Location**: `internal/exante/exante.go`

**Purpose**: Implements the Ex-Ante timestamp protocol for distributed timestamp generation with pre-determined challenges.

**Key Differences from Ex-Post**:

1. **Challenge Generation**: Receives challenge as input (from resource proof) rather than generating randomly
2. **Merkle Path Construction**: Forward-building path (round 0 to r) vs. backward (R to R-r)
3. **Grade Decay**: Different grade calculation: `g = min(grade_function, d - r/D)`

#### 3.5.1 Generation Phase

**Algorithm**:
1. Receive challenge and resource proof from calling protocol
2. Execute MDAG.Generate(sid, vk, challenge, piRP) for R rounds
3. Return state matrix σ

#### 3.5.2 Verification Phase

**Message Validation**:

Similar validation pipeline to Ex-Post with key differences:

1. **Initial Label Verification**:
   ```
   H(sid || vk || value || piRP) ∈ merkle_path[0]
   ```

2. **Merkle Path Structure**:
   - Builds from round 0 forward
   - Layer i must contain H(layer_{i-1})
   - Validates consistency with local MDAG state

**Thread Pool Integration**:

Identical concurrent processing architecture:
- Worker pool for parallel message validation
- Barrier synchronization between rounds
- Graceful shutdown on completion

### 3.6 Resource-Bounded Ex-Post (RB-ExP) Module

**Location**: `internal/resourcebound/rbexp.go`

**Purpose**: Orchestrates the combination of resource proofs with timestamp protocols, implementing the RB-ExP construction.

#### 3.6.1 Generate Phase

**Algorithm**:
```
1. aux_RP ← ResourceProof.Setup(vk)
2. (σ_exp, challenge) ← ExPost.Generate(sid, vk)
3. π_RP ← ResourceProof.Prove(vk, ω, challenge, aux_RP)
4. σ_exa ← ExAnte.Generate(sid, vk, challenge, π_RP)
5. return (challenge, {π_RP, σ_exp, σ_exa})
```

**Timing Characteristics**:
- Resource proof generation is computationally intensive
- Duration measured and logged for analysis
- Weight parameter ω determines proof complexity

#### 3.6.2 Verify Phase

**Parallel Verification Architecture**:

Executes Ex-Post and Ex-Ante verification concurrently:

```go
go func() { committee_exp, err ← ExPost.Verify(...) }()
go func() { committee_exa, err ← ExAnte.Verify(...) }()
```

**Set Intersection Logic**:

Computes final committee as intersection of Ex-Post and Ex-Ante outputs:

```
For each (vk, ch) in O_P:
    if (vk, ch) in O_A:
        g = min(grade_P(vk, ch), grade_A(vk, ch))
        output (id, vk, g)
```

**Filter Function Composition**:

Combines resource proof verification with VRF/VDF verification:
```
f_tag(sid, id, vk, ch, aux) = 
    ResourceProof.Ver(vk, ω, ch, aux.π_RP) ∧ f_filter(sid, id, vk, ch, aux.aux_key)
```

### 3.7 Graded Committee Election (GCE) Module

**Location**: `internal/gce/gce.go`

**Purpose**: Implements the two-phase committee election protocol combining initialization and election phases.

#### 3.7.1 Initialize Phase

**Algorithm**:
```
1. (sk_vrf, vk_vrf) ← VRF.Generate(λ)
2. (challenge, proof_rbexp) ← RBExp.Generate(sid, vk_vrf)
3. vdf_input ← H(id || vk_vrf || challenge)
4. (φ_vdf, π_vdf) ← VDF.Eval(vdf_input, vk_vrf, delay)
5. return LocalState{sk_vrf, vk_vrf, challenge, proof_rbexp, φ_vdf, π_vdf}
```

**Cryptographic Operations**:
- VRF keypair generation uses configurable security parameter λ
- VDF evaluation provides time-delay guarantee
- All operations logged with timing measurements

#### 3.7.2 Committee Election Phase

**Algorithm**:
```
1. hash_input ← H(φ_vdf || sid)
2. (φ_vrf, π_vrf) ← VRF.Eval(hash_input, sk_vrf)
3. aux_key ← {φ_vrf, π_vrf, φ_vdf, π_vdf}
4. outputs ← RBExp.Verify(sid, vk_vrf, challenge, proof_rbexp, aux_key, weight)
5. return outputs
```

**Output Format**:
Each committee member represented as:
- `ID`: Node identifier
- `VK`: Base64-encoded verification key
- `Grade`: Computed grade ∈ [0, d+1]

### 3.8 Synchronizer Module

**Location**: `internal/synchronizer/synchronizer.go`

**Purpose**: Provides global time synchronization and round coordination across all protocol phases.

#### 3.8.1 Time Synchronization

**NTP Integration**:
- Queries configured NTP server (default: `time.google.com`)
- Computes local clock offset
- Adjusts all protocol start times by offset
- Fallback to local time if NTP unavailable

**Start Time Calculation**:

Computes start times for all protocol phases:

```
t_discovery_end = t_start + graph_discovery_timeout
t_building_start = t_discovery_end
t_building_end = t_building_start + (building_rounds × round_timeout)
t_expost_mdag = t_building_end + 1 minute
t_exante_mdag = t_expost_mdag + (R × mdag_timeout) + 1 minute
t_expost_verify = t_exante_mdag + (R × mdag_timeout) + delay + 1 minute
t_exante_verify = t_expost_verify
```

**Buffer Periods**: 1-minute buffers between phases ensure completion of asynchronous operations.

#### 3.8.2 Round Coordination

**Channel-based Synchronization**:

For each protocol step, maintains array of channels:
- `GraphDiscovery`: 1 channel
- `Network`: building_rounds + 1 channels
- `ExPostMDAG`, `ExAnteMDAG`, `ExPostVerify`, `ExAnteVerify`: R + 1 channels

**Wait Mechanism**:
```go
waitChan, err := synchronizer.WaitForRound(step, round)
<-waitChan  // Blocks until scheduled time
```

**Goroutine Architecture**:

One goroutine per protocol step:
- Schedules timer for each round
- Closes channel at scheduled time
- Enables non-blocking `select` on multiple rounds

### 3.9 Cryptographic Primitives

#### 3.9.1 VRF Module

**Location**: `internal/vrf/vrf.go`

**Implementation**: Uses VeChain's ECVRF implementation (ECDSA-based VRF).

**Supported Curves and Hash Functions**:
- λ = 224: P-224 curve, SHA-224
- λ = 256: P-256 curve, SHA-256 (default)
- λ = 384: P-384 curve, SHA-384

**Key Generation**:
1. Select elliptic curve based on λ
2. Generate ECDSA keypair
3. Serialize keys using X.509 encoding
4. Return (secret_key_bytes, verification_key_bytes)

**Evaluation**:
```
Input: message, secret_key
Output: (φ, π)
Process:
  1. Parse ECDSA secret key
  2. phi, pi ← ecvrf.Prove(sk, message)
  3. return (phi, pi)
```

**Verification**:
```
Input: message, phi, pi, verification_key
Output: boolean
Process:
  1. Parse ECDSA public key
  2. beta ← ecvrf.Verify(vk, message, pi)
  3. return (beta == phi)
```

#### 3.9.2 VDF Module

**Location**: `internal/vdf/vdf.go`

**Implementation**: Simplified VDF using hash chains (for development/simulation).

**Note**: This is a placeholder implementation. Production deployments should use proper VDF constructions (e.g., Wesolowski's VDF).

**Evaluation**:
```
Input: message, vk, delay
Process:
  1. Sleep(delay seconds)
  2. phi ← SHA256(message || vk)
  3. pi ← SHA256(phi)
  4. return (phi, pi)
```

**Verification**:
```
Input: message, phi, pi, vk
Process:
  1. expected_phi ← SHA256(message || vk)
  2. if phi ≠ expected_phi: return false
  3. expected_pi ← SHA256(phi)
  4. return (pi == expected_pi)
```

#### 3.9.3 Resource Proof Module

**Location**: `internal/resourceproof/rp.go`, `internal/resourceproof/pow.go`

**Implementation**: Proof-of-Work based resource certificate.

**Proof Generation**:
```
Input: challenge, weight ω
Process:
  1. target ← 2^256 / ω
  2. nonce ← 0
  3. repeat:
       hash ← SHA256(challenge || nonce)
       if hash < target: return nonce
       nonce++
```

**Verification**:
```
Input: challenge, weight ω, nonce
Process:
  1. target ← 2^256 / ω
  2. hash ← SHA256(challenge || nonce)
  3. return (hash < target)
```

**Performance Characteristics**:
- Expected iterations: ω
- Easily parallelizable
- Verification is constant time
- Timing logged for analysis

### 3.10 Metrics and Observability

**Location**: `internal/metrics/metrics.go`, `internal/metrics/collector.go`

**Purpose**: Comprehensive protocol monitoring and analysis capabilities.

#### 3.10.1 Metric Collection

**Prometheus Metrics**:

1. **total_messages**: Counter with labels:
   - `round`: Protocol round number
   - `protocol`: Protocol type (mdag/expost/exante)
   - `node_id`: Node identifier
   - `sid`: Session identifier

2. **valid_messages**: Counter with same labels
   - Subset of total_messages passing all validation checks

**Collection Points**:
- MDAG: Message reception and validation
- Ex-Post: Message reception and validation per round
- Ex-Ante: Message reception and validation per round

#### 3.10.2 Export Mechanisms

**Push Gateway Mode**:
- Periodic push to Prometheus Pushgateway
- Configurable push interval
- Optional basic authentication
- Cleanup on shutdown

**HTTP Server Mode**:
- Exposes `/metrics` endpoint
- Configurable port
- Suitable for Prometheus scraping

**Dual Mode Support**: Can enable both simultaneously for different monitoring architectures.

### 3.11 Configuration Management

**Location**: `pkg/config/config.go`

**Purpose**: Centralized configuration with strong typing and validation.

#### 3.11.1 Configuration Structure

**Major Configuration Sections**:

1. **Network Configuration**:
   - Connection parameters (ports, degree bounds)
   - Discovery settings (protocol ID, bootstrap peers)
   - Simulation options (packet loss)

2. **Graph Configuration**:
   - Diameter bound (D)
   - Grading levels (d)
   - Building rounds

3. **Committee Configuration**:
   - Session identifier
   - Security parameter (λ)
   - Weight parameters (ω, total_W, ΔW)
   - Committee size
   - VDF delay

4. **Synchronization Configuration**:
   - Start time (Unix timestamp)
   - Per-phase timeouts
   - NTP server

5. **Logging Configuration**:
   - Log level (debug/info/warn/error)

6. **Metrics Configuration**:
   - Enable/disable metrics
   - Push gateway settings
   - HTTP server settings

#### 3.11.2 Configuration Loading

**Sources** (priority order):
1. Command-line flag (`-config path/to/config.json`)
2. Environment variable (`ENV`)
3. Default (`./configs/dev.json`)

**Validation**:
- Type checking via mapstructure
- Duration parsing for timeout values
- Automatic correction (e.g., ensuring even building_rounds)

**Format**: JSON with support for duration strings (e.g., `"5s"`, `"100ms"`).

### 3.12 Supporting Modules

#### 3.12.1 Thread Pool

**Location**: `internal/threadpool/threadpool.go`

**Purpose**: Bounded concurrency for parallel message processing.

**Implementation**:
- Fixed-size worker pool
- Task queue with synchronization
- Wait() method for barrier synchronization
- Graceful shutdown support

**Usage Pattern**:
```go
pool := threadpool.New(numWorkers)
for _, task := range tasks {
    pool.Submit(func() { processTask(task) })
}
pool.Wait()  // Barrier: wait for all tasks
pool.Close()
```

#### 3.12.2 Hash Module

**Location**: `internal/hash/hash.go`

**Purpose**: Cryptographic hash function abstraction.

**Functions**:
- `Oracle(bytes...)`: Random oracle implementation using SHA-256
- `Sum(bytes...)`: Hash concatenation of inputs

**Usage**: Consistent hashing throughout protocol implementation.

## 4. Protocol Execution Flow

### 4.1 Startup Sequence

```
1. Configuration Loading
   └─> Parse command-line arguments
   └─> Load JSON configuration
   └─> Validate parameters

2. Logger Initialization
   └─> Create zap logger with configured level
   └─> Set encoding format (JSON for production)

3. Synchronizer Creation
   └─> Query NTP server for clock offset
   └─> Calculate protocol phase start times
   └─> Initialize round coordination channels

4. Network Initialization
   └─> Generate Ed25519 keypair
   └─> Create libp2p host
   └─> Start DHT discovery
   └─> Register protocol handlers

5. Metrics Initialization
   └─> Register Prometheus metrics
   └─> Start push gateway (if enabled)
   └─> Start HTTP server (if enabled)

6. Bootstrap Module Creation
   └─> Instantiate all protocol components
   └─> Wire dependencies
   └─> Register filter and grade functions

7. Protocol Execution
   └─> Start bootstrap.Run() in goroutine
   └─> Wait for completion or shutdown signal
```

### 4.2 Protocol Execution Phases

#### Phase 1: Discovery (Duration: graph_discovery_timeout)

```
└─> DHT announces to network
└─> Collect potential neighbors
└─> Store candidates for graph building
```

#### Phase 2: Graph Building (Duration: building_rounds × round_timeout)

```
For r = 0 to building_rounds-1:
    if r is even:
        └─> Process incoming proposals
        └─> Accept up to target_degree
        └─> Send drop messages to excess
    else:
        └─> Send proposals to candidates
        └─> Track sent proposals
└─> Finalize neighbor set
```

#### Phase 3: Initialization (Duration: VDF delay + processing time)

```
1. VRF.Generate(λ)
   └─> Generate keypair
   └─> Estimated duration: ~1-5ms

2. RBExp.Generate(sid, vk_vrf)
   2.1. ResourceProof.Setup()
   2.2. ExPost.Generate()
        └─> ExPost MDAG (R rounds)
        └─> Duration: R × mdag_timeout
   2.3. ResourceProof.Prove()
        └─> Duration: ~ω hash operations
   2.4. ExAnte.Generate()
        └─> ExAnte MDAG (R rounds)
        └─> Duration: R × mdag_timeout
   
3. VDF.Eval()
   └─> Duration: delay seconds (configured)
```

#### Phase 4: Committee Election (Duration: 2R × verify_timeout)

```
1. VRF.Eval()
   └─> Compute election randomness

2. RBExp.Verify() [parallel execution]
   2.1. ExPost.Verify()
        └─> For r = 1 to R:
            - Wait for round synchronization
            - Process messages in parallel
            - Validate and propagate
            - Update committee
   
   2.2. ExAnte.Verify()
        └─> For r = 1 to R:
            - Wait for round synchronization
            - Process messages in parallel
            - Validate and propagate
            - Update committee
   
3. Intersect Results
   └─> Compute min grades
   └─> Output final committee
```

### 4.3 Shutdown Sequence

```
1. Signal Reception (SIGINT/SIGTERM)
   └─> Log shutdown initiation

2. Metrics Cleanup
   └─> Final push to gateway
   └─> Delete metrics (if configured)
   └─> Stop HTTP server

3. Network Cleanup
   └─> Close all streams
   └─> Disconnect from peers
   └─> Stop DHT discovery

4. Resource Cleanup
   └─> Close goroutines
   └─> Flush logs
   └─> Exit process
```

## 5. Concurrency and Synchronization

### 5.1 Concurrency Model

The framework employs structured concurrency with clear ownership:

**Goroutine Hierarchy**:
```
main goroutine
├─> synchronizer.Start()
│   ├─> runTimeSyncForStep(GraphDiscovery)
│   ├─> runTimeSyncForStep(Network)
│   ├─> runTimeSyncForStep(ExPostMDAG)
│   ├─> runTimeSyncForStep(ExAnteMDAG)
│   ├─> runTimeSyncForStep(ExPostVerify)
│   └─> runTimeSyncForStep(ExAnteVerify)
├─> network.Start()
│   ├─> graphBuilder()
│   └─> handleDiscoveredPeers()
├─> metrics.Start()
│   └─> pushMetrics() [periodic]
└─> bootstrap.Run()
    ├─> RBExp.Generate()
    │   ├─> ExPost.Generate()
    │   │   └─> MDAG.Generate()
    │   └─> ExAnte.Generate()
    │       └─> MDAG.Generate()
    └─> RBExp.Verify()
        ├─> ExPost.Verify()
        │   └─> threadpool workers (per round)
        └─> ExAnte.Verify()
            └─> threadpool workers (per round)
```

### 5.2 Synchronization Mechanisms

#### 5.2.1 Global Time Synchronization

**Mechanism**: Channel-based round triggers
**Granularity**: Per-step, per-round
**Coordination**: Close channel at scheduled time

**Usage Pattern**:
```go
ch, _ := sync.WaitForRound(step, round)
<-ch  // Blocks until scheduled time
// Proceed with round logic
```

#### 5.2.2 Message Passing Synchronization

**Mechanism**: Mutex-protected message queues
**Pattern**: Producer-consumer with round-based consumption

**Implementation**:
```go
// Producer (network handler)
m.mu.Lock()
m.messages[round] = append(m.messages[round], msg)
m.mu.Unlock()

// Consumer (protocol logic)
m.mu.Lock()
msgs := m.messages[round-1]
m.mu.Unlock()
// Process msgs
```

#### 5.2.3 Parallel Verification Synchronization

**Mechanism**: Thread pool with barrier synchronization

**Implementation**:
```go
pool := threadpool.New(numWorkers)
for _, msg := range msgs {
    pool.Submit(func() { processMessage(msg) })
}
pool.Wait()  // Barrier: wait for all
```

#### 5.2.4 Neighbor Set Synchronization

**Mechanism**: RWMutex for neighbor map

**Pattern**: Multiple readers, single writer
```go
// Read path (frequent)
n.mu.RLock()
info := n.neighbors[peerID]
n.mu.RUnlock()

// Write path (rare)
n.mu.Lock()
n.neighbors[peerID] = info
n.mu.Unlock()
```

### 5.3 Deadlock Prevention

**Strategies**:

1. **Consistent Lock Ordering**: Always acquire locks in the same order
2. **Bounded Blocking**: All channel operations have timeout contexts
3. **Non-blocking Checks**: Atomic boolean flags for state queries
4. **Channel-based Coordination**: Prefer channels over condition variables

## 6. Performance Optimizations

### 6.1 Memory Management

#### 6.1.1 Object Pooling

**Protobuf Message Pools**:
```go
var messagePool = sync.Pool{
    New: func() interface{} {
        return &pb.Message{}
    },
}
```

**Benefits**:
- Reduced GC pressure
- Lower allocation rate
- Improved latency consistency

**Usage**: Ex-Post and Ex-Ante modules pool all protobuf messages.

#### 6.1.2 Preallocation

**Strategy**: Preallocate slices with known capacity
```go
outputs := make([]*Output, 0, expectedSize)
```

**Application**: Committee outputs, message buffers, label arrays.

### 6.2 Computational Optimizations

#### 6.2.1 Parallel Message Validation

**Approach**: Bound concurrency with thread pools
**Benefit**: Utilize all CPU cores while preventing oversubscription

**Configuration**: `min(2×NumCPU, 4)` workers per protocol

#### 6.2.2 Efficient Serialization

**Format**: Protocol Buffers for all network messages
**Benefits**:
- Compact binary encoding
- Fast serialization/deserialization
- Schema evolution support

### 6.3 Network Optimizations

#### 6.3.1 Connection Reuse

**Strategy**: Maintain persistent connections to neighbors
**Benefit**: Amortize connection establishment overhead

#### 6.3.2 Message Batching

**Approach**: Single broadcast call per round
**Implementation**: Snapshot neighbor list, iterate once

#### 6.3.3 Bounded Message Sizes

**Limit**: 1 MiB per inbound message
**Enforcement**: Stream reading with limit
**Benefit**: Prevent resource exhaustion attacks

## 7. Error Handling and Resilience

### 7.1 Error Propagation

**Strategy**: Explicit error returns throughout call chain
**Pattern**: Wrap errors with context

```go
if err := operation(); err != nil {
    return fmt.Errorf("operation failed: %w", err)
}
```

### 7.2 Validation Layers

#### 7.2.1 Network Layer Validation

- Session ID matching
- Neighbor verification
- Message authentication
- Size limits

#### 7.2.2 Protocol Layer Validation

- Merkle path verification
- Cryptographic proof verification
- Grade computation validation
- Round number bounds checking

### 7.3 Graceful Degradation

**NTP Failure**: Fall back to local system time
**Discovery Failure**: Log warning, continue with available peers
**Verification Failure**: Skip invalid messages, continue protocol

### 7.4 Resource Cleanup

**Pattern**: Defer-based cleanup
```go
func operation() error {
    resource := acquire()
    defer resource.Release()
    // ... use resource ...
}
```

**Application**: Network streams, file handles, goroutine cancellation.

## 8. Testing and Simulation

### 8.1 Unit Testing

**Coverage**: Core protocol logic, cryptographic primitives, network utilities

**Location**: `*_test.go` files in each module

**Approach**:
- Table-driven tests for parameterized validation
- Mock interfaces for dependency injection
- Property-based testing for invariants

### 8.2 Integration Testing

**Scope**: Multi-node protocol execution

**Files**: `*_integration_test.go`

**Scenarios**:
- Two-node committee election
- Message validation end-to-end
- Synchronization correctness

### 8.3 Simulation Framework

**Location**: `scripts/run_simulations.py`

**Capabilities**:

1. **Multi-Node Orchestration**:
   - Spawn N nodes with generated configurations
   - Coordinate startup times
   - Collect logs per node

2. **Failure Injection**:
   - Random node kills (`--kill-random-up-to`)
   - Packet loss simulation (`--drop-on-send-percent`)
   - Byzantine behavior (future work)

3. **Result Analysis**:
   - Aggregated metrics
   - Log archival
   - Committee convergence analysis

**Configuration**: `configs/sample_simulation_plan.json`

### 8.4 Observability

**Structured Logging**:
- JSON format for machine parsing
- Contextual fields (node_id, round, protocol)
- Configurable verbosity levels

**Metrics**:
- Per-round message counts
- Timing measurements
- Committee sizes

**Analysis Tools**:
- Jupyter notebook: `simulation_analysis.ipynb`
- Visualizations: Message propagation, grade distribution

## 9. Deployment Considerations

### 9.1 Configuration Management

**Environments**: Development, Staging, Production

**Files**:
- `configs/dev.json`: Local testing
- `configs/stg.json`: Pre-production validation
- `configs/prod.json`: Production deployment

**Best Practices**:
- Version control all configurations
- Document parameter choices
- Use consistent session IDs per experiment

### 9.2 Network Requirements

**Ports**:
- Configurable listen port (default: dynamic)
- Metrics HTTP port (if enabled)

**Connectivity**:
- Outbound: Bootstrap DHT peers
- Inbound: P2P connections from other nodes

**Firewall**: Allow TCP connections on configured ports

### 9.3 Resource Requirements

**CPU**: 
- Minimum: 2 cores
- Recommended: 4+ cores for parallel verification

**Memory**:
- Base: ~100 MB per node
- Peak: Scales with committee size and network degree

**Storage**:
- Logs: ~10 MB per hour (info level)
- Metrics: Ephemeral (pushed to external system)

### 9.4 Monitoring

**Health Checks**:
- Metrics endpoint availability
- Log output continuity
- Expected completion time

**Alerts**:
- Node dropout detection
- Committee size deviation
- Protocol timeout violations

## 10. Security Considerations

### 10.1 Cryptographic Assumptions

**VRF Security**: Relies on ECDSA security (discrete log assumption)

**VDF Security**: Current implementation is placeholder; production requires secure VDF (e.g., RSA-based construction)

**Hash Function**: SHA-256 modeled as random oracle

### 10.2 Network Security

**Message Authentication**: All protocol messages signed with Ed25519

**Replay Protection**: Session IDs provide per-run isolation

**Sybil Resistance**: Resource proofs provide computational barrier

### 10.3 Attack Vectors and Mitigations

#### 10.3.1 Network-Level Attacks

**Attack**: Message flooding
**Mitigation**: Message size limits, rate limiting by neighbor

**Attack**: Eclipse attacks
**Mitigation**: DHT-based discovery, diverse bootstrap peers

#### 10.3.2 Protocol-Level Attacks

**Attack**: Invalid proof submission
**Mitigation**: Comprehensive validation at each layer

**Attack**: Selective message dropping
**Mitigation**: Simulation mode for testing, future work on detection

### 10.4 Future Security Enhancements

1. **Secure VDF**: Integrate Wesolowski or Pietrzak VDF
2. **Byzantine Fault Tolerance**: Extend protocols for malicious behavior
3. **Adaptive Security**: Dynamic difficulty adjustment for resource proofs
4. **Privacy**: Zero-knowledge proofs for committee membership

## 11. Design Rationale

### 11.1 Technology Choices

**Go Language**:
- Native concurrency support (goroutines)
- Strong standard library
- Efficient binary compilation
- Cross-platform support

**libp2p**:
- Production-ready P2P stack
- Flexible transport abstraction
- Built-in multiplexing and encryption
- Active development community

**Protocol Buffers**:
- Efficient serialization
- Language-agnostic schemas
- Forward/backward compatibility

**Prometheus**:
- Industry-standard metrics
- Rich ecosystem
- Flexible collection models

### 11.2 Architectural Decisions

#### 11.2.1 Time-Based Synchronization

**Decision**: Use global time synchronization via NTP

**Rationale**:
- Simplifies protocol logic
- Avoids complex distributed consensus
- Sufficient for research experiments

**Trade-offs**: Requires loose clock synchronization (achievable with NTP)

#### 11.2.2 Parallel Verification

**Decision**: Use bounded thread pools for message processing

**Rationale**:
- Maximize CPU utilization
- Prevent resource exhaustion
- Maintain predictable latency

**Alternative**: Sequential processing (simpler but slower)

#### 11.2.3 Object Pooling

**Decision**: Pool protobuf messages in hot paths

**Rationale**:
- Reduce GC pressure during verification
- Improve latency consistency
- Minimal code complexity increase

**Alternative**: Rely on generational GC (less predictable)

#### 11.2.4 Separate Ex-Post and Ex-Ante

**Decision**: Implement timestamp protocols as independent modules

**Rationale**:
- Clear separation of concerns
- Testability
- Flexibility for future variants

**Alternative**: Unified timestamp module (more code reuse but less clear)

### 11.3 Protocol-Level Decisions

#### 11.3.1 Graph Building Algorithm

**Decision**: Multi-round proposal/acceptance protocol

**Rationale**:
- Achieves bounded degree
- Symmetric neighbor relationships
- Tolerates asynchrony

**Parameters**: Randomized target degree prevents clustering

#### 11.3.2 Merkle Path Validation

**Decision**: Validate full path from local label to message root

**Rationale**:
- Ensures consistency with local view
- Prevents forging of invalid timestamps
- Enables early rejection of invalid messages

#### 11.3.3 Grade Computation

**Decision**: Compute grades using big integer arithmetic

**Rationale**:
- Avoids precision loss in 2^λ / (φ+1) calculation
- Ensures consistency across nodes
- Matches theoretical specification

## 12. Limitations and Future Work

### 12.1 Current Limitations

#### 12.1.1 VDF Implementation

**Limitation**: Uses sleep-based placeholder instead of secure VDF

**Impact**: No security guarantee for delay

**Future Work**: Integrate Wesolowski's VDF or Chia's implementation

#### 12.1.2 Graph Building

**Limitation**: Assumes honest participation in graph construction

**Impact**: Malicious nodes can disrupt topology

**Future Work**: Byzantine-resilient graph building protocol

#### 12.1.3 Synchronization

**Limitation**: Relies on loose time synchronization

**Impact**: Clock skew can cause round misalignment

**Future Work**: Hybrid synchronization (time + message-driven)

### 12.2 Scalability Considerations

**Current Target**: 1000-10000 nodes

**Bottlenecks**:
- DHT discovery latency
- Message validation throughput
- Memory for message storage

**Optimization Opportunities**:
- Bloom filters for duplicate detection
- Incremental Merkle tree updates
- Pipelined verification stages

### 12.3 Research Extensions

1. **Adaptive Protocols**: Dynamic parameter adjustment based on network conditions
2. **Privacy-Preserving Elections**: Anonymous committee membership
3. **Cross-Chain Integration**: Committee as bridge validators
4. **Formal Verification**: Mechanized proof of protocol correctness

## 13. Conclusion

The Committee Sampling Framework provides a comprehensive, production-quality implementation of setup-free committee election protocols. The modular architecture separates concerns effectively, enabling independent development and testing of protocol components. The use of Go and libp2p provides a solid foundation for distributed systems research and deployment.

Key strengths include:
- Clear separation of protocol layers
- Comprehensive observability and metrics
- Flexible simulation capabilities
- Concurrent execution with structured synchronization

The framework serves as both a research tool for analyzing committee election protocols and a foundation for building decentralized applications requiring dynamic committee selection.

