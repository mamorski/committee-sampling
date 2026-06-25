package common

import (
	"encoding/base64"
	"sync"

	pb "github.com/mamorski/committee-sampling/pkg/proto"
)

type RBExpProof struct {
	PiRP     []byte
	SigmaExp [][][]byte
	SigmaExa [][][]byte
}

// AuxKey holds the auxiliary public values used in the RB-ExP verification.
type AuxKey struct {
	// Output and proof from the VRF evaluation in the committee-election phase.
	PhiVRF []byte
	PiVRF  []byte

	// VDF values from the initialization phase.
	PhiVDF []byte
	PiVDF  []byte
}

func (a *AuxKey) ToProto() *pb.AuxData {
	return &pb.AuxData{
		PhiVrf: a.PhiVRF,
		PiVrf:  a.PiVRF,
		PhiVdf: a.PhiVDF,
		PiVdf:  a.PiVDF,
	}
}

type AuxTag struct {
	PiRP   []byte
	AuxKey *AuxKey
}

type O struct {
	ID    string
	Grade int
}

type FSigmaExp struct {
	Challenge []byte
	Sigma     [][][]byte
}

type FSigmaRBExp struct {
	PiRP     []byte
	SigmaExp [][][]byte
	SigmaExa [][][]byte
}

// RoundAcc holds per-round stats accumulated locally under a protocol's own
// mutex. Flushed (logged) once per round via flushRoundStats instead of per message.
type RoundAcc struct {
	Total, Valid, LateCount, MaxLateness int
	LagSumNs, LagMaxNs, LagLastNs       int64
	LagCount                             int
}

// CommitteeOutput is the final output for an elected candidate: a triple (id, vk, grade).
type CommitteeOutput struct {
	ID    string
	VK    string // Base64-encoded verification key
	Grade int
}

// Committee holds the committee members' information.
type Committee struct {
	mu        sync.RWMutex // Protects concurrent access to committee map
	committee map[string]O
	len       int
}

// Add adds a new member to the committee.
// If the member already exists with a lower grade, it will overwrite it.
// If the member exists with a higher or equal grade, it will not update the entry.
// Returns true if the member was added or updated, false if not.
// This method is thread-safe.
func (c *Committee) Add(vk []byte, id string, grade int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.committee == nil {
		c.committee = make(map[string]O)
		c.len = 0
	}

	key := string(vk)

	existing, exists := c.committee[key]
	if exists && existing.Grade >= grade {
		return false
	} else if !exists {
		c.len++
	}

	c.committee[key] = O{ID: id, Grade: grade}

	return true
}

// ToCommitteeOutput converts the committee to a slice of CommitteeOutput.
// This method is thread-safe.
func (c *Committee) ToCommitteeOutput() []*CommitteeOutput {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.committee == nil {
		return nil
	}

	outputs := make([]*CommitteeOutput, 0, c.len)

	for key, member := range c.committee {
		vkStr := base64.StdEncoding.EncodeToString([]byte(key))
		outputs = append(outputs, &CommitteeOutput{
			ID:    member.ID,
			VK:    vkStr,
			Grade: member.Grade,
		})
	}

	return outputs
}

// Range calls fn for every (vk, value) in the committee.
// This method is thread-safe.
func (c *Committee) Range(fn func(vk string, val O)) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for key, val := range c.committee {
		fn(key, val)
	}
}

// Get retrieves a committee member by verification key.
// This method is thread-safe.
func (c *Committee) Get(vk string) (O, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.committee == nil {
		return O{}, false
	}

	key := string(vk)
	if member, exists := c.committee[key]; exists {
		return member, true
	}

	return O{}, false
}

// Len returns the number of members in the committee.
// This method is thread-safe.
func (c *Committee) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.len
}
