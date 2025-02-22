package resource_bound

import (
	"errors"

	"github.com/mamorski/committee-sampling/internal/common"
)

type filterFunc func(string, []byte, []byte, *common.AuxKey) bool

type filterTagFunc func(string, []byte, []byte, *common.AuxTag) bool

type ResourceProof interface {
	Setup(vk []byte) ([]byte, error)
	Prove(vk []byte, weight float64, challenge []byte, aux []byte) ([]byte, error)
	Ver(vk []byte, weight float64, challenge []byte, rpProof []byte) bool
}

type ExPost interface {
	Generate(session string, vk []byte) ([][][]byte, []byte, error)
	Verify(
		session string,
		vk []byte,
		fSigmaExp *common.FSigmaExp,
		auxTag *common.AuxTag,
		auxLocal float64,
		filter filterTagFunc) (map[common.Key]common.O, error)
}

type ExAnte interface {
	Generate(session string, vk []byte, challenge []byte, rpProof []byte) ([][][]byte, error)
	Verify(
		session string,
		vk []byte,
		sigma [][][]byte,
		auxTag *common.AuxTag,
		auxLocal float64,
		filter filterTagFunc) (map[common.Key]common.O, error)
}

type RbExp struct {
	rp      ResourceProof
	exp     ExPost
	exa     ExAnte
	weight  float64
	ffilter filterFunc
}

func New(rp ResourceProof, exp ExPost, exa ExAnte, ffilter filterFunc, weight float64) *RbExp {
	return &RbExp{
		rp:      rp,
		exp:     exp,
		exa:     exa,
		weight:  weight,
		ffilter: ffilter,
	}
}

func (r *RbExp) Gen(sid string, vk []byte) ([]byte, *common.RBExpProof, error) {
	// Step 1
	auxRP, err := r.rp.Setup(vk)
	if err != nil {
		return nil, nil, err
	}

	// Step 2
	sigmaExp, challenge, err := r.exp.Generate(sid, vk)
	if err != nil {
		return nil, nil, err
	}

	// Step 3
	piRP, err := r.rp.Prove(vk, r.weight, challenge, auxRP)
	if err != nil {
		return nil, nil, err
	}

	// Step 4
	sigmaExa, err := r.exa.Generate(sid, vk, challenge, piRP)
	if err != nil {
		return nil, nil, err
	}

	proof := &common.RBExpProof{
		PiRP:     piRP,
		SigmaExp: sigmaExp,
		SigmaExa: sigmaExa,
	}

	return challenge, proof, nil
}

func (r *RbExp) Ver(
	sid string,
	vk, ch []byte,
	proof *common.RBExpProof,
	auxKey *common.AuxKey,
	auxLocal float64) ([]*common.RBExpOutput, error) {

	fTag := func(sid string, vk []byte, ch []byte, tag *common.AuxTag) bool {
		return r.rp.Ver(vk, r.weight, ch, tag.PiRP) && r.ffilter(sid, vk, ch, tag.AuxKey)
	}
	// Step 1
	auxTag := &common.AuxTag{
		PiRP:   proof.PiRP,
		AuxKey: auxKey,
	}
	fSigmaExp := &common.FSigmaExp{
		Challenge: ch,
		Sigma:     proof.SigmaExp,
	}
	oP, err := r.exp.Verify(sid, vk, fSigmaExp, auxTag, auxLocal, fTag)
	if err != nil {
		return nil, err
	}

	// Step 2
	oA, err := r.exa.Verify(sid, vk, proof.SigmaExa, auxTag, auxLocal, fTag)
	if err != nil {
		return nil, err
	}

	var outputs []*common.RBExpOutput
	for key, value := range oP {
		oAValues, ok := oA[key]
		if !ok {
			continue
		}
		g := min(value.Grade, oAValues.Grade)
		outputs = append(outputs, &common.RBExpOutput{
			SID:       sid,
			VK:        value.VK,
			Challenge: value.Challenge,
			AuxKey:    value.Aux,
			Grade:     g,
		})
	}
	if len(outputs) == 0 {
		return nil, errors.New("verification failed: no matching output")
	}
	return outputs, nil
}
