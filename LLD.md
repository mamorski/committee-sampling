# Low-Level Design Document

## Committee Sampling Framework

**Version:** 1.0.0  
**Last Updated:** January 2026

---

## Table of Contents

1. [Module Specifications](#1-module-specifications)
2. [Data Structures](#2-data-structures)
3. [Interface Definitions](#3-interface-definitions)
4. [Protocol Message Formats](#4-protocol-message-formats)
5. [Algorithm Specifications](#5-algorithm-specifications)
6. [Goroutine Lifecycle](#6-goroutine-lifecycle)
7. [Memory Management](#7-memory-management)
8. [Error Handling](#8-error-handling)
9. [Configuration Schema](#9-configuration-schema)
10. [Metrics Specification](#10-metrics-specification)

---

## 1. Module Specifications

### 1.1 Main Entry Point

**File:** `cmd/committee-sampling/main.go`

**Initialization Sequence:**

"""
1. Parse command-line flags (-config)
2. Load configuration via config.Load()
3. Create zap logger with production config
4. Initialize Synchronizer
5. Create P2PNode network layer
6. Initialize MetricsCollector
7. Create Bootstrap instance
8. Register signal handlers (SIGINT, SIGTERM)
9. Execute bootstrap.Run() in goroutine
10. Block on completion or signal
11. Graceful shutdown sequence
"""

**Logger Configuration:**

| Field | Value |
|-------|-------|
| TimeKey | "timestamp" |
| LevelKey | "level" |
| MessageKey | "msg" |
| CallerKey | "caller" |
| TimeEncoder | ISO8601 |
| LevelEncoder | CapitalLevel |
| CallerEncoder | ShortCaller |

---

### 1.2 Bootstrap Module

**File:** `internal/boot/bootstrap.go`

**Struct Definition:**

"""go
type Bootstrap struct {
    id         string              // Node identifier (peer ID)
    MDagExAnte exante.MDAG         // MDAG for Ex-Ante protocol
    MDagExPost expost.MDAG         // MDAG for Ex-Post protocol
    ExAnte     resourcebound.ExAnte
    ExPost     resourcebound.ExPost
    RP         resourcebound.ResourceProof
    RbExp      gce.RBExp
    VDF        gce.VDF
    VRF        gce.VRF
    GCE        GCE
    Config     *config.Config
    Network    network.Network
    Logger     *zap.Logger
    Context    context.Context
}
"""

**Filter Function Specification:**

"""
Input:
  - sid: string (session identifier)
  - id: string (sender node ID)
  - vk: []byte (verification key)
  - ch: []byte (challenge)
  - auxKey: *pb.AuxKeyMessage

Output: bool

Algorithm:
  1. Return false if auxKey is nil
  2. vdfInput ← Hash(id || vk || ch)
  3. vrfInput ← Hash(auxKey.PhiVdf || sid)
  4. vdfResult ← VDF.Verify(vdfInput, auxKey.PhiVdf, auxKey.PiVdf, vk)
  5. If !vdfResult: return false
  6. vrfResult ← VRF.Verify(vrfInput, auxKey.PhiVrf, auxKey.PiVrf, vk)
  7. Return vrfResult
"""

**Grade Function Specification:**

"""
Input:
  - sid: string
  - vk: []byte
  - ch: []byte
  - auxKey: *pb.AuxKeyMessage
  - weight: float64 (Wi)

Output: int (grade ∈ [0, d+1])

Algorithm:
  1. If auxKey is nil: return 0
  2. φ ← BigInt(auxKey.PhiVrf)  // big-endian parse
  3. φ+1 ← φ + 1
  4. 2^λ ← BigInt(1) << λ
  5. ratio ← Float64(2^λ / (φ+1))
  6. sub ← Wi - n × ratio
  7. term ← sub / ΔW
  8. g ← (d + 1) - term
  9. Return min(d+1, floor(g))
"""

---

### 1.3 Network Module

**File:** `internal/network/host.go`

**Constants:**

| Constant | Value | Description |
|----------|-------|-------------|
| graphProposal | "/graph/proposal/1.0.0" | Graph proposal protocol ID |
| graphDrop | "/graph/drop/1.0.0" | Graph drop protocol ID |
| clientVersion | "go-p2p-node/0.0.1" | Client version string |
| maxInboundMessageSize | 1 << 20 (1 MiB) | Maximum inbound message size |

**P2PNode Struct:**

"""go
type P2PNode struct {
    host                        Host
    ctx                         context.Context
    cancel                      context.CancelFunc
    logger                      *zap.Logger
    discovery                   discovery.PeerDiscovery
    sync                        common.Synchronizer
    mu                          sync.Mutex
    key                         crypto.PrivKey
    neighbors                   map[peer.ID]peer.AddrInfo
    potentialNeighbors          sync.Map
    sid                         string
    maxOutbound                 int
    buildingRounds              int
    dropOnSendProbability       float64
    acceptingPotentialNeighbors atomic.Bool
    dropOnSend                  bool
    proposalQueue               []queuedProposal
    dropQueue                   []queuedDrop
    queueMu                     sync.Mutex
    acceptingGraphMessages      atomic.Bool
    shuffledPotentialNeighbors  []peer.AddrInfo
    selectedNeighborTarget      int
    sentProposalsTo             map[peer.ID]bool
}
"""

**Graph Building Algorithm:**

"""
Phase 1: Discovery
  1. Start DHT-based peer discovery
  2. Collect discovered peers into potentialNeighbors map
  3. Wait for GraphDiscovery round completion

Phase 2: Building (Multi-round)
  For round in [0, buildingRounds):
    IF round is EVEN:
      Process proposal queue:
        For each proposal:
          If sender is already neighbor: skip
          If current_neighbors < target:
            Add neighbor
          Else:
            Send drop message
    IF round is ODD:
      Process drop queue:
        For each drop:
          Remove from neighbors
      Send proposals to unsent candidates:
        For each candidate in shuffled list:
          If not neighbor AND not already proposed:
            Add as neighbor
            Send GraphProposal message
            Mark as sent

Phase 3: Finalization
  1. Stop accepting graph messages
  2. Process remaining drop queue
  3. Clear queues
  4. Freeze neighbor set
"""

**Target Degree Selection:**

"""
Input: limit (maxOutbound)
Output: target ∈ [2×limit, 3×limit]

Algorithm:
  1. minimum ← limit × 2
  2. maximum ← limit × 3
  3. range ← maximum - minimum + 1
  4. n ← crypto/rand.Int(range)
  5. Return minimum + n
"""

---

### 1.4 MDAG Module

**File:** `internal/mdag/mdag.go`

**Struct Definition:**

"""go
type MDAG struct {
    rounds         int                 // R = d × D
    oracle         HashOracle          // H: {0,1}* → {0,1}^256
    network        network.Network
    synchronizer   common.Synchronizer
    logger         *zap.Logger
    mu             sync.Mutex
    messages       map[int][][]byte    // round → received labels
    state          [][][]byte          // σ[r-1] = bucket of labels
    computedLabels [][]byte            // L[r] for r ∈ [0, R]
    currentLabel   []byte              // Most recent L
    sessionID      string
    protocolType   string              // "expostMDAG" or "exanteMDAG"
    step           common.Step
    validMessages  []int               // Per-round valid count
    totalMessages  []int               // Per-round total count
    isRunning      bool
}
"""

**Generate Algorithm:**

"""
Input:
  - sid: session ID
  - vki: verification key
  - vi: variadic additional inputs

Output: σ (state matrix), error

Algorithm:
  1. Lock and check isRunning
  2. Compute L₀ ← H(sid || vki || v₁ || ... || vₙ)
  3. computedLabels[0] ← L₀
  4. Wait for round 0 synchronization
  5. Broadcast(0, L₀)
  
  For r ∈ [1, R]:
    6. Wait for round r synchronization
    7. prevMsgs ← messages[r-1]
    8. sortedLabels ← sort([currentLabel] ∪ prevMsgs)
    9. state[r-1] ← sortedLabels
    10. concatenated ← concat(sortedLabels)
    11. L_r ← H(concatenated)
    12. currentLabel ← L_r
    13. computedLabels[r] ← L_r
    14. If r < R: Broadcast(r, L_r)
  
  15. Return state
"""

**Message Handler:**

"""
Input: from (peer.ID), payload ([]byte)
Output: error

Validation:
  1. Protocol must be running
  2. Unmarshal protobuf MDAGMessage
  3. Round ∈ [0, rounds]
  4. Sender must be neighbor
  5. Session ID must match

Action:
  6. Append label to messages[round]
  7. Increment metrics
"""

---

### 1.5 Ex-Post Module

**File:** `internal/expost/expost.go`

**Object Pools:**

"""go
var (
    timestampMessagePool = sync.Pool{New: func() interface{} { return &pb.TimestampMessage{} }}
    auxPool              = sync.Pool{New: func() interface{} { return &pb.Aux{} }}
    auxKeyMessagePool    = sync.Pool{New: func() interface{} { return &pb.AuxKeyMessage{} }}
    statePool            = sync.Pool{New: func() interface{} { return &pb.State{} }}
)
"""

**Struct Definition:**

"""go
type ExPost struct {
    network      network.Network
    logger       *zap.Logger
    mdag         MDAG
    synchronizer common.Synchronizer
    mu           sync.Mutex
    messages     map[int][]*pb.TimestampMessage
    state        [][][]byte
    gradeFunc    common.GradeFunc
    threadPool   *threadpool.ThreadPool
    protocolID   string
    nodeID       string
    sid          string
    vk           []byte
    validMessages []int
    totalMessages []int
    d            int     // grading levels
    D            int     // diameter bound
    lambda       int     // security parameter
    R            int     // d × D
    isRunning    bool
}
"""

**Generate Phase:**

"""
Input: session (string), vk ([]byte)
Output: σ ([][][]byte), challenge ([]byte), error

Algorithm:
  1. r_i ← SecureRandomBytes(λ, charset)
  2. σ, err ← MDAG.Generate(session, vk, r_i)
  3. ℓ_R ← MDAG.GetComputedLabel(R)
  4. Return (σ, ℓ_R, nil)
"""

**Verify Phase:**

"""
Input:
  - session: string
  - vk: []byte
  - fSigmaExp: *FSigmaExp (challenge, sigma)
  - auxTag: *AuxTag (PiRP, AuxKey)
  - auxLocal: float64 (weight)
  - filterFn: FilterTagF

Output: *Committee, error

Algorithm:
  1. Validate session ID
  2. Verify sigma length ≥ R
  3. Initialize threadpool(min(NumCPU×2, 4))
  
  Prover Logic (round 0):
    4. grade ← gradeFunc(session, vk, challenge, auxKey, auxLocal)
    5. If grade ≥ (d+1) AND filterFn passes:
       a. Construct TimestampMessage with merkle_path[0] = σ[R-1]
       b. Add self to results
       c. Broadcast message
  
  Verification Loop:
  For r ∈ [1, R):
    6. Wait for round r synchronization
    7. msgs ← messages[r-1]
    8. For each msg in parallel:
       processMessage(msg, session, σ, auxLocal, filterFn, r, results)
    9. Wait for threadpool completion
  
  10. Return results
"""

**Message Validation:**

"""
Input: msg, auxLocal, filterFn, r
Output: bool

Checks:
  1. msg != nil
  2. len(merklePath) ≥ r
  3. filterFn(msg.SessionId, msg.Id, msg.VK, msg.Value, msg.Aux) == true
  4. gradeFunc(...) > 0
  5. msg.Value == H(merklePath[last].Row)
  6. validateMerklePath(merklePath, r) == true
"""

**Merkle Path Validation:**

"""
Input: merklePath ([]*pb.State), round (int)
Output: bool

Algorithm:
  1. labelRound ← R - round
  2. localLabel ← MDAG.GetComputedLabel(labelRound)
  3. If localLabel ∉ merklePath[0].Row: return false
  
  For i ∈ [1, round):
    4. prevHash ← H(merklePath[i-1].Row)
    5. If prevHash ∉ merklePath[i].Row: return false
  
  6. Return true
"""

---

### 1.6 Ex-Ante Module

**File:** `internal/exante/exante.go`

**Struct Definition:**

"""go
type ExAnte struct {
    network       network.Network
    logger        *zap.Logger
    mdag          MDAG
    synchronizer  common.Synchronizer
    mu            sync.Mutex
    messages      map[int][]*pb.TimestampMessage
    state         [][][]byte
    threadPool    *threadpool.ThreadPool
    gradeFunction common.GradeFunc
    protocolID    string
    nodeID        string
    sid           string
    challenge     []byte
    validMessages []int
    totalMessages []int
    d             int
    D             int
    R             int
    isRunning     bool
}
"""

**Generate Phase:**

"""
Input:
  - session: string
  - vk: []byte
  - challenge: []byte
  - piRP: []byte

Output: σ ([][][]byte), error

Algorithm:
  1. σ, err ← MDAG.Generate(session, vk, challenge, piRP)
  2. Store challenge for verification
  3. Return σ
"""

**Merkle Path Validation (Ex-Ante specific):**

"""
Input: merklePath ([][][]byte), round (int)
Output: bool

Algorithm:
  For i ∈ [0, round):
    1. h ← H(merklePath[i])
    2. If i == round-1:
       Return h ∈ state[round]
    3. Else if h ∉ merklePath[i+1]:
       Return false
  
  Return true
"""

**Initial Label Validation:**

"""
Check: H(sid || vk || value || PiRP) ∈ merklePath[0]
"""

---

### 1.7 Resource-Bounded Ex-Post Module

**File:** `internal/resourcebound/rbexp.go`

**Struct Definition:**

"""go
type RbExp struct {
    rp      ResourceProof
    exp     ExPost
    exa     ExAnte
    weight  float64
    ffilter common.FilterF
    logger  *zap.Logger
}
"""

**Generate Phase:**

"""
Input: sid (string), vk ([]byte)
Output: challenge ([]byte), proof (*RBExpProof), error

Algorithm:
  1. auxRP, _ ← rp.Setup(vk)
  2. sigmaExp, challenge, err ← exp.Generate(sid, vk)
  3. piRP, err ← rp.Prove(vk, weight, challenge, auxRP)
  4. sigmaExa, err ← exa.Generate(sid, vk, challenge, piRP)
  5. proof ← {PiRP: piRP, SigmaExp: sigmaExp, SigmaExa: sigmaExa}
  6. Return (challenge, proof, nil)
"""

**Verify Phase:**

"""
Input:
  - sid, vk, ch: identifiers
  - proof: *RBExpProof
  - auxKey: *AuxKey
  - auxLocal: float64

Output: []*CommitteeOutput, error

Algorithm:
  1. Define fTag filter combining RP verification and ffilter
  2. Create auxTag from proof and auxKey
  3. Create fSigmaExp from challenge and proof.SigmaExp
  
  Parallel Execution:
  4. Go: oP ← exp.Verify(sid, vk, fSigmaExp, auxTag, auxLocal, fTag)
  5. Go: oA ← exa.Verify(sid, vk, proof.SigmaExa, auxTag, auxLocal, fTag)
  6. Wait for both results
  
  Set Intersection:
  7. For each (vk, ch) in oP:
       If (vk, ch) ∈ oA:
         grade ← min(oP.grade, oA.grade)
         outputs.append({ID, VK, grade})
  
  8. Return outputs
"""

---

### 1.8 GCE Module

**File:** `internal/gce/gce.go`

**LocalState Struct:**

"""go
type LocalState struct {
    VRFSecret  []byte    // sk_vrf
    VRFPublic  []byte    // vk_vrf
    Challenge  []byte    // ch
    RBExpProof *common.RBExpProof
    PhiVDF     []byte    // φ_vdf
    PiVDF      []byte    // π_vdf
}
"""

**Initialize Phase:**

"""
Input: id, sid, vrf, rbexp, vdf, delay, lambda
Output: *LocalState, error

Algorithm:
  1. sk, vk ← VRF.Generate(λ)
  2. challenge, rbExpProof ← RBExp.Generate(sid, vk)
  3. vdfInput ← H(id || vk || challenge)
  4. φ_vdf, π_vdf ← VDF.Eval(vdfInput, vk, delay)
  5. Return LocalState{sk, vk, challenge, rbExpProof, φ_vdf, π_vdf}
"""

**Committee Election Phase:**

"""
Input: sid, state, weight, vrf, rbexp
Output: []*CommitteeOutput, error

Algorithm:
  1. hashInput ← H(state.PhiVDF || sid)
  2. φ_vrf, π_vrf ← VRF.Eval(hashInput, state.VRFSecret)
  3. auxKey ← {φ_vrf, π_vrf, state.PhiVDF, state.PiVDF}
  4. outputs ← RBExp.Verify(sid, state.VRFPublic, state.Challenge, 
                            state.RBExpProof, auxKey, weight)
  5. Return outputs
"""

---

### 1.9 Synchronizer Module

**File:** `internal/synchronizer/synchronizer.go`

**Struct Definition:**

"""go
type Synchronizer struct {
    cfg           *config.Config
    logger        *zap.Logger
    stopChan      chan struct{}
    roundChannels map[common.Step][]chan struct{}
    rounds        int      // d × D
    mu            sync.Mutex
    stopOnce      sync.Once
}
"""

**Channel Allocation:**

| Step | Channel Count |
|------|---------------|
| GraphDiscovery | 1 |
| Network | buildingRounds + 1 |
| ExPostMDAG | R + 1 |
| ExAnteMDAG | R + 1 |
| ExPostVerify | R + 1 |
| ExAnteVerify | R + 1 |

**Start Time Calculation:**

"""
Input: cfg (*Config), logger
Output: *StartTimes

Algorithm:
  1. Query NTP server for clockOffset
  2. graphDiscoveryStart ← Unix(cfg.StartTime) + clockOffset
  3. graphDiscoveryEnd ← graphDiscoveryStart + GraphDiscoveryTimeout
  4. graphBuildingStart ← graphDiscoveryEnd
  5. graphBuildingTime ← GraphDiscoveryTimeout + (BuildingRoundTimeout × BuildingRounds)
  6. exPostMDAG ← graphDiscoveryStart + graphBuildingTime + 1min
  7. exAnteMDAG ← exPostMDAG + (MDAGRoundTimeout × R) + 1min
  8. exPostVerify ← exAnteMDAG + (MDAGRoundTimeout × R) + delay + 1min
  9. exAnteVerify ← exPostVerify
"""

**Round Triggering:**

"""
For each step:
  For round ∈ [0, numRounds]:
    1. roundStartTime ← stepStartTime + (round × roundTimeout)
    2. timer ← time.NewTimer(until(roundStartTime))
    3. Wait on timer.C or stopChan
    4. Close roundChannels[step][round]
"""

---

### 1.10 Cryptographic Modules

#### 1.10.1 VRF Module

**File:** `internal/vrf/vrf.go`

**Curve Selection:**

| λ | Curve | Hash | SuiteString |
|---|-------|------|-------------|
| 224 | P-224 | SHA-224 | 0x02 |
| 256 | P-256 | SHA-256 | 0x01 |
| 384 | P-384 | SHA-384 | 0x03 |

**Generate:**

"""
Input: λ
Output: sk ([]byte), vk ([]byte), error

Algorithm:
  1. curve, hash, suite ← selectCurveAndHash(λ)
  2. ecvrf ← ecvrf.New(curve, suite, hash)
  3. secretKey ← ecdsa.GenerateKey(curve, rand.Reader)
  4. sk ← x509.MarshalECPrivateKey(secretKey)
  5. vk ← x509.MarshalPKIXPublicKey(&secretKey.PublicKey)
  6. Return (sk, vk, nil)
"""

**Eval:**

"""
Input: x ([]byte), sk ([]byte)
Output: φ ([]byte), π ([]byte), error

Algorithm:
  1. secretKey ← x509.ParseECPrivateKey(sk)
  2. φ, π ← ecvrf.Prove(secretKey, x)
  3. Return (φ, π, nil)
"""

**Verify:**

"""
Input: x, φ, π, vk (all []byte)
Output: bool, error

Algorithm:
  1. pk ← x509.ParsePKIXPublicKey(vk)
  2. beta ← ecvrf.Verify(pk, x, π)
  3. Return (beta == φ), nil
"""

#### 1.10.2 VDF Module

**File:** `internal/vdf/vdf.go`

**Note:** This is a placeholder implementation using sleep-based delay.

**Eval:**

"""
Input: x ([]byte), vk ([]byte), delta (int seconds)
Output: φ ([]byte), π ([]byte), error

Algorithm:
  1. time.Sleep(delta seconds)
  2. hash ← SHA256(x || vk)
  3. φ ← hash[:]
  4. π ← SHA256(φ)[:]
  5. Return (φ, π, nil)
"""

**Verify:**

"""
Input: x, φ, π, vk (all []byte)
Output: bool, error

Algorithm:
  1. expectedPhi ← SHA256(x || vk)
  2. If φ ≠ expectedPhi: return false
  3. expectedPi ← SHA256(φ)
  4. Return (π == expectedPi), nil
"""

#### 1.10.3 Resource Proof Module

**File:** `internal/resourceproof/rp.go`, `internal/resourceproof/pow.go`

**Prove (Proof of Work):**

"""
Input: challenge ([]byte), difficulty (int)
Output: proof ([]byte), error

Algorithm:
  1. nonce ← 0
  2. Loop:
     a. hash ← BLAKE2b(challenge || nonce)
     b. If hasLeadingZeros(hash, difficulty):
        Return uint64ToBytes(nonce)
     c. nonce++
     d. If overflow: return error
"""

**Verify:**

"""
Input: challenge ([]byte), difficulty (int), proof ([]byte)
Output: bool

Algorithm:
  1. hash ← BLAKE2b(challenge || proof)
  2. Return hasLeadingZeros(hash, difficulty)
"""

**hasLeadingZeros:**

"""
Input: hash ([]byte), required (int)
Output: bool

Algorithm:
  bits ← 0
  For each byte b in hash:
    For i ∈ [7, 0] (MSB to LSB):
      If bit i of b is set:
        Return (bits ≥ required)
      bits++
  Return (bits ≥ required)
"""

#### 1.10.4 Hash Module

**File:** `internal/hash/hash.go`

"""go
// Sum concatenates inputs and returns BLAKE2b-256 hash
func Sum(data ...[]byte) []byte {
    hasher, _ := blake2b.New256(nil)
    for _, d := range data {
        hasher.Write(d)
    }
    return hasher.Sum(nil)
}

// Oracle returns BLAKE2b-256 hash of input
func Oracle(data []byte) []byte {
    hash := blake2b.Sum256(data)
    return hash[:]
}
"""

---

## 2. Data Structures

### 2.1 Common Types

**File:** `internal/common/structs.go`

"""go
// RBExpProof contains proof data from RB-ExP generation
type RBExpProof struct {
    PiRP     []byte      // Resource proof
    SigmaExp [][][]byte  // Ex-Post state matrix
    SigmaExa [][][]byte  // Ex-Ante state matrix
}

// AuxKey holds auxiliary cryptographic values
type AuxKey struct {
    PhiVRF []byte  // VRF output
    PiVRF  []byte  // VRF proof
    PhiVDF []byte  // VDF output
    PiVDF  []byte  // VDF proof
}

// AuxTag combines resource proof with auxiliary key
type AuxTag struct {
    PiRP   []byte
    AuxKey *AuxKey
}

// O represents a committee member output
type O struct {
    ID    string
    Grade int
}

// CommitteeOutput is the final committee member representation
type CommitteeOutput struct {
    ID    string  // Node identifier
    VK    string  // Base64-encoded verification key
    Grade int     // Grade ∈ [0, d+1]
}
"""

### 2.2 Committee Structure

**Thread-Safe Map Implementation:**

"""go
type Committee struct {
    mu        sync.RWMutex
    committee map[string]O  // key = vk + "\x00" + ch
    len       int
}
"""

**Key Format:** `base64(vk) + "\x00" + base64(ch)`

**Add Method Behavior:**
- Returns `true` if member added or grade updated
- Returns `false` if existing member has equal or higher grade
- Thread-safe with mutex protection

### 2.3 Synchronization Steps

"""go
const (
    GraphDiscovery Step = "GraphDiscovery"
    Network        Step = "Network"
    ExPostMDAG     Step = "ExPostMDAG"
    ExAnteMDAG     Step = "ExAnteMDAG"
    ExPostVerify   Step = "ExPostVerify"
    ExAnteVerify   Step = "ExAnteVerify"
)
"""

---

## 3. Interface Definitions

### 3.1 Network Interface

"""go
type Network interface {
    RegisterHandler(protocolID string, handler MessageHandler)
    SendProtocolMessage(protocolID string, data []byte)
    GetNeighbors() []string
    GetNodeID() string
    Close() error
    IsNeighbor(peerID peer.ID) bool
}

type MessageHandler func(from peer.ID, payload []byte) error
"""

### 3.2 Synchronizer Interface

"""go
type Synchronizer interface {
    WaitForRound(step Step, round int) (<-chan struct{}, error)
}
"""

### 3.3 VRF Interface

"""go
type VRF interface {
    Generate(lambda int) (sk []byte, vk []byte, err error)
    Eval(message, sk []byte) (output []byte, proof []byte, err error)
}
"""

### 3.4 VDF Interface

"""go
type VDF interface {
    Eval(message, vk []byte, delay int) (phiVDF []byte, piVDF []byte, err error)
}
"""

### 3.5 RBExp Interface

"""go
type RBExp interface {
    Generate(sid string, vk []byte) (challenge []byte, proof *common.RBExpProof, err error)
    Verify(sid string, vk, ch []byte, proof *common.RBExpProof, 
           auxKey *common.AuxKey, auxLocal float64) ([]*common.CommitteeOutput, error)
}
"""

### 3.6 Filter Functions

"""go
// FilterF validates VRF/VDF proofs
type FilterF func(sid string, id string, vk []byte, ch []byte, auxKey *pb.AuxKeyMessage) bool

// FilterTagF validates resource proofs and auxiliary data
type FilterTagF func(sid string, id string, vk []byte, ch []byte, aux *pb.Aux) bool

// GradeFunc computes participant grade
type GradeFunc func(sid string, vk []byte, ch []byte, auxKey *pb.AuxKeyMessage, weight float64) int
"""

---

## 4. Protocol Message Formats

### 4.1 MDAG Message

**File:** `pkg/proto/mdag.proto`

"""protobuf
message MDAGMessage {
  string session_id = 1;  // Protocol session identifier
  uint32 round = 2;       // Round number [0, R]
  bytes label = 3;        // Computed label (32 bytes)
  string id = 4;          // Sender node ID
}
"""

**Size Estimate:** ~100-200 bytes per message

### 4.2 Timestamp Message

**File:** `pkg/proto/timestamp.proto`

"""protobuf
message TimestampMessage {
  string session_id = 1;
  bytes verification_key = 2;  // X.509 encoded public key (~90 bytes)
  bytes value = 3;             // Challenge/label (32 bytes)
  Aux aux = 4;
  repeated State merkle_path = 5;
  uint32 round = 6;
  string id = 7;
}

message State {
  repeated bytes row = 1;  // Array of labels (32 bytes each)
}

message Aux {
  bytes PiRP = 1;           // PoW nonce (8 bytes)
  AuxKeyMessage aux_key = 2;
}

message AuxKeyMessage {
  bytes phi_vrf = 1;  // VRF output (~32 bytes)
  bytes pi_vrf = 2;   // VRF proof (~80 bytes)
  bytes phi_vdf = 3;  // VDF output (32 bytes)
  bytes pi_vdf = 4;   // VDF proof (32 bytes)
}
"""

**Size Estimate:** Varies by merkle path depth, typically 500 bytes - 10KB

### 4.3 Network Messages

**File:** `pkg/proto/network.proto`

"""protobuf
message MessageData {
  string clientVersion = 1;
  int64 timestamp = 2;
  string id = 3;
  bool gossip = 4;
  string nodeId = 5;
  bytes nodePubKey = 6;
  bytes sign = 7;  // Ed25519 signature (64 bytes)
}

message GraphProposal {
  MessageData messageData = 1;
  int32 round = 2;
}

message GraphDrop {
  MessageData messageData = 1;
}

message ProtocolMessage {
  MessageData messageData = 1;
  bytes payload = 2;  // Nested serialized message
}
"""

---

## 5. Algorithm Specifications

### 5.1 Complete Protocol Flow

"""
INITIALIZATION PHASE:
┌────────────────────────────────────────┐
│ 1. VRF.Generate(λ) → (sk, vk)          │
│ 2. RBExp.Generate(sid, vk)             │
│    ├─ RP.Setup(vk)                     │
│    ├─ ExPost.Generate(sid, vk)         │
│    │   └─ MDAG.Generate (R rounds)     │
│    ├─ RP.Prove(vk, ω, challenge)       │
│    └─ ExAnte.Generate(sid, vk, ch, π)  │
│        └─ MDAG.Generate (R rounds)     │
│ 3. VDF.Eval(H(id||vk||ch), vk, delay)  │
└────────────────────────────────────────┘
                  ↓
COMMITTEE ELECTION PHASE:
┌────────────────────────────────────────┐
│ 4. VRF.Eval(H(φ_vdf||sid), sk)         │
│ 5. RBExp.Verify (parallel)             │
│    ├─ ExPost.Verify (R rounds)         │
│    └─ ExAnte.Verify (R rounds)         │
│ 6. Intersect results → Committee       │
└────────────────────────────────────────┘
"""

### 5.2 Message Processing Pipeline

"""
INCOMING MESSAGE:
  ↓
┌─────────────────┐
│ Size Check      │ > 1 MiB → Reject
│ (1 MiB limit)   │
└────────┬────────┘
         ↓
┌─────────────────┐
│ Unmarshal       │ Parse error → Reject
│ (protobuf)      │
└────────┬────────┘
         ↓
┌─────────────────┐
│ Authenticate    │ Invalid signature → Reject
│ (Ed25519)       │
└────────┬────────┘
         ↓
┌─────────────────┐
│ Session Check   │ Mismatch → Reject
│                 │
└────────┬────────┘
         ↓
┌─────────────────┐
│ Neighbor Check  │ Non-neighbor → Reject
│                 │
└────────┬────────┘
         ↓
┌─────────────────┐
│ Round Check     │ Invalid range → Reject
│ [0, R)          │
└────────┬────────┘
         ↓
┌─────────────────┐
│ Queue Message   │ Store for processing
└─────────────────┘
"""

### 5.3 Grade Computation Details

"""
Parameters:
  d = grading levels
  n = committee size
  λ = security parameter
  Wi = local weight estimate
  ΔW = weight disagreement bound
  φ = VRF output (interpreted as big-endian integer)

Formula:
  g_i = (d + 1) - (Wi - n × 2^λ/(φ+1)) × (1/ΔW)

Precision Handling:
  1. Parse φ as big.Int from VRF output bytes
  2. Compute 2^λ as big.Int using bit shift
  3. Compute ratio as big.Float for precision
  4. Convert to float64 for final calculation
  5. Floor and clamp to [0, d+1]
"""

---

## 6. Goroutine Lifecycle

### 6.1 Main Goroutine Hierarchy

"""
main()
├── synchronizer.Start()
│   ├── runTimeSyncForStep(GraphDiscovery)
│   ├── runTimeSyncForStep(Network)
│   ├── runTimeSyncForStep(ExPostMDAG)
│   ├── runTimeSyncForStep(ExAnteMDAG)
│   ├── runTimeSyncForStep(ExPostVerify)
│   └── runTimeSyncForStep(ExAnteVerify)
│
├── network.graphBuilder()
│   └── handleDiscoveredPeers()
│
├── metrics.Start() [if enabled]
│   └── pushMetrics() [periodic]
│
└── bootstrap.Run()
    ├── GCE.Initialize()
    │   └── RBExp.Generate()
    │       ├── ExPost.Generate()
    │       │   └── MDAG.Generate() [R iterations]
    │       └── ExAnte.Generate()
    │           └── MDAG.Generate() [R iterations]
    │
    └── GCE.CommitteeElection()
        └── RBExp.Verify()
            ├── ExPost.Verify() [goroutine]
            │   └── threadpool workers [per round]
            └── ExAnte.Verify() [goroutine]
                └── threadpool workers [per round]
"""

### 6.2 ThreadPool Lifecycle

"""
Creation:
  pool := threadpool.New(numWorkers)
  → Creates numWorkers goroutines
  → Each goroutine loops on taskQueue or quit channel

Usage:
  pool.Submit(task)  // Adds task to queue, increments WaitGroup
  pool.Wait()        // Blocks until all tasks complete
  pool.Close()       // Signals workers to stop (once.Do)

Worker Loop:
  for {
    select {
    case task := <-taskQueue:
      task()
      wg.Done()
    case <-quit:
      return
    }
  }
"""

### 6.3 Synchronizer Channel Lifecycle

"""
Initialization:
  For each step:
    channels[step] = make([]chan struct{}, numRounds)
    For each round:
      channels[step][round] = make(chan struct{})

Triggering:
  At scheduled time:
    select {
    case <-channels[step][round]:  // Already closed
    default:
      close(channels[step][round])  // Unblock waiters
    }

Waiting:
  ch := channels[step][round]
  <-ch  // Blocks until channel closed
"""

---

## 7. Memory Management

### 7.1 Object Pooling Strategy

**Pooled Objects:**
- `pb.TimestampMessage`
- `pb.Aux`
- `pb.AuxKeyMessage`
- `pb.State`

**Pool Usage Pattern:**

"""go
// Acquire from pool
msg := getTimestampMessage()
defer putTimestampMessage(msg)

// Configure object
msg.SessionId = session
msg.VerificationKey = vk
// ... set other fields

// Use object
data, _ := proto.Marshal(msg)
network.SendProtocolMessage(protocolID, data)
"""

**Reset Behavior:**

"""go
func getTimestampMessage() *pb.TimestampMessage {
    msg := timestampMessagePool.Get().(*pb.TimestampMessage)
    msg.Reset()  // Clear all fields
    return msg
}
"""

### 7.2 Slice Preallocation

"""go
// Committee output preallocation
outputs := make([]*CommitteeOutput, 0, c.len)

// Message storage preallocation
messages[round] = make([][]byte, 0, 16)

// Candidate list preallocation
candidates := make([]peer.AddrInfo, 0, expectedCount)
"""

### 7.3 Buffer Management

"""go
// MDAG label concatenation
var buffer bytes.Buffer
buffer.WriteString(sid)
buffer.Write(vki)
for _, v := range vi {
    buffer.Write(v)
}
hash := oracle(buffer.Bytes())
"""

---

## 8. Error Handling

### 8.1 Error Categories

| Category | Example | Handling |
|----------|---------|----------|
| Configuration | Invalid config file | Panic at startup |
| Cryptographic | VRF generation failure | Return error, abort |
| Network | Connection failure | Log, retry, skip peer |
| Protocol | Invalid message | Log, drop message |
| Synchronization | Channel error | Return error, abort |

### 8.2 Error Propagation Pattern

"""go
// Wrap errors with context
if err := operation(); err != nil {
    return fmt.Errorf("operation failed: %w", err)
}

// Check specific errors
if errors.Is(err, ErrSessionMismatch) {
    // Handle specifically
}
"""

### 8.3 Graceful Degradation

"""go
// NTP fallback
response, err := ntp.Query(timeServer)
if err != nil {
    logger.Warn("Failed to query NTP server, using local time")
    clockOffset = 0
} else {
    clockOffset = response.ClockOffset
}

// Peer discovery failure
if !network.IsNeighbor(from) {
    logger.Warn("Received message from unknown neighbor")
    return errNotNeighbor  // Message dropped, protocol continues
}
"""

---

## 9. Configuration Schema

### 9.1 Full Configuration Structure

"""json
{
  "network": {
    "listen_port": 0,
    "max_outbound_degree": 10,
    "discovery_config": {
      "protocol_id": "/committee/1.0.0",
      "interval": "5s",
      "bootstrap_peers": ["/ip4/.../tcp/.../p2p/..."]
    },
    "connectivity_retries": 3,
    "drop_on_send": false,
    "drop_on_send_probability": 0.0
  },
  "graph": {
    "diameter": 5,
    "grading_levels": 3,
    "building_rounds": 10
  },
  "committee": {
    "session_id": "session-001",
    "lambda": 256,
    "weight": 16.0,
    "total_weight": 1000.0,
    "delta_w": 100.0,
    "committee_size": 100,
    "delay": 60
  },
  "synchronization": {
    "type": 0,
    "ex_ante_round_timeout": "500ms",
    "ex_post_round_timeout": "500ms",
    "mdag_round_timeout": "300ms",
    "graph_discovery_timeout": "30s",
    "graph_building_round_timeout": "2s",
    "start_time": 1737014400,
    "time_server": "time.google.com"
  },
  "logger": {
    "level": "info"
  },
  "metrics": {
    "enabled": true,
    "push_gateway": {
      "enabled": false,
      "url": "http://localhost:9091",
      "username": "",
      "password": "",
      "delete_on_stop": true
    },
    "http_server": {
      "enabled": true,
      "port": 9090,
      "path": "/metrics"
    },
    "push_interval": "10s"
  }
}
"""

### 9.2 Configuration Validation

"""go
// Ensure even building rounds
if cfg.Graph.BuildingRounds%2 != 0 {
    cfg.Graph.BuildingRounds++
}

// Validate probability bounds
if p < 0 { p = 0 }
if p > 1 { p = 1 }

// Duration parsing
// Supported formats: "5s", "500ms", "1m", "100µs"
"""

---

## 10. Metrics Specification

### 10.1 Prometheus Metrics

**Counter: total_messages**

"""
Name: total_messages
Help: Total number of messages processed
Labels:
  - round: Protocol round number
  - protocol: Protocol type (mdag/expost/exante)
  - node_id: Node identifier
  - sid: Session identifier
"""

**Counter: valid_messages**

"""
Name: valid_messages
Help: Number of valid messages processed
Labels:
  - round: Protocol round number
  - protocol: Protocol type (mdag/expost/exante)
  - node_id: Node identifier
  - sid: Session identifier
"""

### 10.2 Internal Tracking

"""go
// Per-module message tracking
type ExPost struct {
    validMessages []int  // validMessages[round] = count
    totalMessages []int  // totalMessages[round] = count
}

// Logged at protocol completion
logger.Info("Message counts", 
    zap.Ints("valid_messages", e.validMessages),
    zap.Ints("total_messages", e.totalMessages))
"""

### 10.3 Timing Measurements

"""go
// Function-level timing
start := time.Now()
defer func() {
    elapsed := time.Since(start)
    logger.Info("Operation completed", zap.Duration("elapsed", elapsed))
}()

// Phase-level timing in logs
// Example output:
// {"level":"INFO","timestamp":"...","msg":"Generate completed","elapsed":"2.5s"}
"""

---

## Appendix A: Protocol IDs

| Protocol | Format | Example |
|----------|--------|---------|
| MDAG | `/mdag/1.0.0/{type}/{sid}` | `/mdag/1.0.0/expostMDAG/session-001` |
| ExPost | `/expost/1.0.0/{sid}` | `/expost/1.0.0/session-001` |
| ExAnte | `/exante/1.0.0/{sid}` | `/exante/1.0.0/session-001` |
| Graph Proposal | `/graph/proposal/1.0.0/{sid}` | `/graph/proposal/1.0.0/session-001` |
| Graph Drop | `/graph/drop/1.0.0/{sid}` | `/graph/drop/1.0.0/session-001` |

---

## Appendix B: Hash Functions

| Context | Algorithm | Output Size |
|---------|-----------|-------------|
| Random Oracle (MDAG) | BLAKE2b-256 | 32 bytes |
| General Hashing | BLAKE2b-256 | 32 bytes |
| PoW Verification | BLAKE2b-256 | 32 bytes |
| VDF (placeholder) | SHA-256 | 32 bytes |
| VRF (λ=256) | SHA-256 | 32 bytes |

---

## Appendix C: Security Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| λ | 256 | Security parameter (bits) |
| d | 3 | Grading levels |
| D | 5 | Graph diameter bound |
| R | d × D = 15 | Total MDAG rounds |
| ω | 16 | PoW difficulty (leading zero bits) |
| Message size limit | 1 MiB | Maximum inbound message |

---

## Appendix D: File Structure

"""
committee-sampling/
├── cmd/committee-sampling/
│   └── main.go                    # Entry point
├── internal/
│   ├── boot/bootstrap.go          # Protocol orchestration
│   ├── common/
│   │   ├── interfaces.go          # Shared interfaces
│   │   └── structs.go             # Common data structures
│   ├── exante/exante.go           # Ex-Ante protocol
│   ├── expost/expost.go           # Ex-Post protocol
│   ├── gce/gce.go                 # Committee election
│   ├── hash/hash.go               # Hash utilities
│   ├── mdag/mdag.go               # Merkle DAG
│   ├── metrics/
│   │   ├── collector.go           # Metrics collector
│   │   └── metrics.go             # Prometheus metrics
│   ├── network/
│   │   ├── host.go                # P2P node
│   │   ├── internals.go           # Internal helpers
│   │   └── discovery/             # DHT discovery
│   ├── resourcebound/rbexp.go     # RB-ExP
│   ├── resourceproof/
│   │   ├── rp.go                  # Resource proof wrapper
│   │   └── pow.go                 # Proof of work
│   ├── synchronizer/synchronizer.go
│   ├── threadpool/threadpool.go
│   ├── vdf/vdf.go
│   └── vrf/vrf.go
├── pkg/
│   ├── config/config.go           # Configuration
│   └── proto/                     # Protocol buffers
│       ├── *.proto                # Schema definitions
│       └── *.pb.go                # Generated Go code
└── configs/
    ├── dev.json
    ├── stg.json
    └── prod.json
"""
