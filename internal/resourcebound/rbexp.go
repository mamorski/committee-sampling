package resourcebound

import (
	"errors"

	"github.com/mamorski/committee-sampling/internal/common"
	"go.uber.org/zap"
)

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
		filter common.FilterTagF) (map[common.Key]common.O, error)
}

type ExAnte interface {
	Generate(session string, vk []byte, challenge []byte, rpProof []byte) ([][][]byte, error)
	Verify(
		session string,
		vk []byte,
		sigma [][][]byte,
		auxTag *common.AuxTag,
		auxLocal float64,
		filter common.FilterTagF) (map[common.Key]common.O, error)
}

type RbExp struct {
	rp      ResourceProof
	exp     ExPost
	exa     ExAnte
	weight  float64
	ffilter common.FilterF
	logger  *zap.Logger
}

func New(rp ResourceProof, exp ExPost, exa ExAnte, ffilter common.FilterF, weight float64, logger *zap.Logger) *RbExp {
	return &RbExp{
		rp:      rp,
		exp:     exp,
		exa:     exa,
		weight:  weight,
		ffilter: ffilter,
		logger:  logger.Named("rbexp"),
	}
}

func (r *RbExp) Generate(sid string, vk []byte) ([]byte, *common.RBExpProof, error) {
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

func (r *RbExp) Verify(
	sid string,
	vk, ch []byte,
	proof *common.RBExpProof,
	auxKey *common.AuxKey,
	auxLocal float64) ([]*common.RBExpOutput, error) {

	fTag := func(sid, id string, vk []byte, ch []byte, tag *common.AuxTag) bool {
		return r.rp.Ver(vk, r.weight, ch, tag.PiRP) && r.ffilter(sid, id, vk, ch, tag.AuxKey)
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
	r.logger.Debug("RBExp outputs from ExPost verification",
		zap.String("sid", sid),
		zap.Int("num_outputs", len(oP)),
	)

	// Step 2
	oA, err := r.exa.Verify(sid, vk, proof.SigmaExa, auxTag, auxLocal, fTag)
	if err != nil {
		return nil, err
	}
	r.logger.Debug("RBExp outputs from ExAnte verification",
		zap.String("sid", sid),
		zap.Int("num_outputs", len(oA)),
	)

	var outputs []*common.RBExpOutput
	for key, value := range oP {
		r.logger.Debug("Key, value from ExPost",
			zap.String("sid", sid),
			zap.Any("key", key),
			zap.Any("value", value),
		)
		oAValues, ok := oA[key]
		if !ok {
			continue
		}
		g := min(value.Grade, oAValues.Grade)
		outputs = append(outputs, &common.RBExpOutput{
			SID:       sid,
			ID:        value.ID,
			VK:        value.VK,
			Challenge: value.Challenge,
			AuxTag:    value.Aux,
			Grade:     g,
		})
	}
	if len(outputs) == 0 {
		return nil, errors.New("verification failed: no matching output")
	}
	return outputs, nil
}
