package common

import (
	"encoding/base64"

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
// It maps a base64-encoded verification key (vk) to a map of base64-encoded challenges (ch) to their outputs (O).
type Committee struct {
	committee map[string]map[string]O
	len       int
}

// Add adds a new member to the committee.
// It takes a verification key (vk), a challenge (ch), an identifier (id), and a grade.
// If the member already exists with a lower grade, it will overwrite it.
// If the member exists with a higher or equal grade, it will not update the entry.
// Returns true if the member was added or updated, false if not.
func (c *Committee) Add(vk, ch []byte, id string, grade int) bool {
	if c.committee == nil {
		c.committee = make(map[string]map[string]O)
		c.len = 0
	}
	vkStr := base64.StdEncoding.EncodeToString(vk)
	chStr := base64.StdEncoding.EncodeToString(ch)

	if _, exists := c.committee[vkStr]; !exists {
		c.committee[vkStr] = make(map[string]O)
	} else if existing, exists := c.committee[vkStr][chStr]; exists {
		// Update only if the existing member has a lower grade
		if existing.Grade > grade {
			return false // Do not overwrite with a lower grade
		}
	}

	c.committee[vkStr][chStr] = O{ID: id, Grade: grade}
	c.len++
	return true
}

// ToCommitteeOutput converts the committee to a slice of CommitteeOutput.
func (c *Committee) ToCommitteeOutput() []*CommitteeOutput {
	if c.committee == nil {
		return nil
	}

	var outputs []*CommitteeOutput

	for vk, members := range c.committee {
		for _, member := range members {
			outputs = append(outputs, &CommitteeOutput{
				ID:    member.ID,
				VK:    vk,
				Grade: member.Grade,
			})
		}
	}

	return outputs
}

// Range calls fn for every (vk, ch, value) in the underlying maps.
func (c *Committee) Range(fn func(vk, ch string, val O)) {
	for vk, inner := range c.committee {
		for ch, val := range inner {
			fn(vk, ch, val)
		}
	}
}

func (c *Committee) Get(vk, ch string) (O, bool) {
	if c.committee == nil {
		return O{}, false
	}

	if members, exists := c.committee[vk]; exists {
		if member, exists := members[ch]; exists {
			return member, true
		}
	}

	return O{}, false
}

// Len returns the number of members in the committee.
func (c *Committee) Len() int {
	return c.len
}
