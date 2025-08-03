package resourcebound

import (
	"errors"
	"time"

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
		filterFn common.FilterTagF) (*common.Committee, error)
}

type ExAnte interface {
	Generate(session string, vk []byte, challenge []byte, rpProof []byte) ([][][]byte, error)
	Verify(
		session string,
		vk []byte,
		sigma [][][]byte,
		auxTag *common.AuxTag,
		auxLocal float64,
		filter common.FilterTagF) (*common.Committee, error)
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
	r.logger.Info("Generate started")
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		r.logger.Info("Generate completed",
			zap.Duration("elapsed", elapsed),
		)
	}()

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
	startTime := time.Now()
	piRP, err := r.rp.Prove(vk, r.weight, challenge, auxRP)
	r.logger.Debug("Resource Proof took: %d ms",
		zap.Int64("runtime", time.Since(startTime).Milliseconds()))
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
	auxLocal float64,
) ([]*common.CommitteeOutput, error) {
	r.logger.Info("Verify started")
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		r.logger.Info("Verify completed",
			zap.Duration("elapsed", elapsed),
		)
	}()

	fTag := func(sid, id string, vk []byte, ch []byte, tag *common.AuxTag) bool {
		r.logger.Debug("Filter tag function called",
			zap.String("sender_id", id),
			zap.Binary("vk", vk),
		)
		return r.rp.Ver(vk, r.weight, ch, tag.PiRP) && r.ffilter(sid, id, vk, ch, tag.AuxKey)
	}
	auxTag := &common.AuxTag{
		PiRP:   proof.PiRP,
		AuxKey: auxKey,
	}
	fSigmaExp := &common.FSigmaExp{
		Challenge: ch,
		Sigma:     proof.SigmaExp,
	}

	type verifyResult struct {
		committee *common.Committee
		err       error
	}

	// Run verifications in parallel
	exPostCh := make(chan verifyResult, 1)
	exAnteCh := make(chan verifyResult, 1)

	go func() {
		committee, err := r.exp.Verify(sid, vk, fSigmaExp, auxTag, auxLocal, fTag)
		exPostCh <- verifyResult{committee, err}
	}()

	go func() {
		committee, err := r.exa.Verify(sid, vk, proof.SigmaExa, auxTag, auxLocal, fTag)
		exAnteCh <- verifyResult{committee, err}
	}()

	// Wait for both results
	exPostResult := <-exPostCh
	exAnteResult := <-exAnteCh

	if exPostResult.err != nil {
		return nil, exPostResult.err
	}
	if exAnteResult.err != nil {
		return nil, exAnteResult.err
	}

	oP := exPostResult.committee
	oA := exAnteResult.committee

	r.logger.Debug("RBExp outputs from ExPost verification",
		zap.String("sid", sid),
		zap.Int("num_outputs", oP.Len()),
	)
	r.logger.Debug("RBExp outputs from ExAnte verification",
		zap.String("sid", sid),
		zap.Int("num_outputs", oA.Len()),
	)

	var outputs []*common.CommitteeOutput
	oP.Range(func(key, ch string, val common.O) {
		if exAnteValue, ok := oA.Get(key, ch); ok {
			g := min(val.Grade, exAnteValue.Grade)
			outputs = append(outputs, &common.CommitteeOutput{
				ID:    val.ID,
				VK:    key,
				Grade: g,
			})
		} else {
			r.logger.Debug("No matching ExAnte value for ExPost",
				zap.String("sid", sid),
				zap.String("vk", key),
				zap.String("challenge", ch),
			)
		}
	})

	if len(outputs) == 0 {
		return nil, errors.New("verification failed: no matching output")
	}
	return outputs, nil
}
