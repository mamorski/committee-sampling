package common

import (
	"encoding/base64"
	"strings"
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

// CommitteeOutput is the final output for an elected candidate: a triple (id, vk, grade).
type CommitteeOutput struct {
	ID    string
	VK    string // Base64-encoded verification key
	Grade int
}

// Committee holds the committee members' information.
// It maps a flattened key (vkStr + "\x00" + chStr) to their outputs (O).
type Committee struct {
	mu        sync.RWMutex // Protects concurrent access to committee map
	committee map[string]O
	len       int
}

// makeKey creates a flattened key from vk and ch strings.
func makeKey(vkStr, chStr string) string {
	return vkStr + "\x00" + chStr
}

// splitKey splits a flattened key back into vk and ch strings.
func splitKey(key string) (vkStr, chStr string) {
	parts := strings.SplitN(key, "\x00", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return key, ""
}

// Add adds a new member to the committee.
// It takes a verification key (vk), a challenge (ch), an identifier (id), and a grade.
// If the member already exists with a lower grade, it will overwrite it.
// If the member exists with a higher or equal grade, it will not update the entry.
// Returns true if the member was added or updated, false if not.
// This method is thread-safe.
func (c *Committee) Add(vk, ch []byte, id string, grade int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.committee == nil {
		c.committee = make(map[string]O)
		c.len = 0
	}
	vkStr := base64.StdEncoding.EncodeToString(vk)
	chStr := base64.StdEncoding.EncodeToString(ch)
	key := makeKey(vkStr, chStr)

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

	// Pre-allocate slice with exact capacity to avoid reallocations
	outputs := make([]*CommitteeOutput, 0, c.len)

	for key, member := range c.committee {
		vk, _ := splitKey(key)
		outputs = append(outputs, &CommitteeOutput{
			ID:    member.ID,
			VK:    vk,
			Grade: member.Grade,
		})
	}

	return outputs
}

// Range calls fn for every (vk, ch, value) in the underlying maps.
// This method is thread-safe.
func (c *Committee) Range(fn func(vk, ch string, val O)) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for key, val := range c.committee {
		vk, ch := splitKey(key)
		fn(vk, ch, val)
	}
}

// Get retrieves a committee member by verification key and challenge.
// This method is thread-safe.
func (c *Committee) Get(vk, ch string) (O, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.committee == nil {
		return O{}, false
	}

	key := makeKey(vk, ch)
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
