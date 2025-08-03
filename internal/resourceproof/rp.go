package resourceproof

import (
	"bytes"
	"fmt"
	"time"

	"go.uber.org/zap"
)

type ResourceProof struct {
	proofOfWork ProofOfWork
	logger      *zap.Logger
}

// lint:ignore U1000 This function is not used with PoW
func (r *ResourceProof) Setup(_ []byte) ([]byte, error) {
	return nil, nil
}

func (r *ResourceProof) Prove(vk []byte, omega float64, ch []byte, _ []byte) ([]byte, error) {
	r.logger.Info("Prove started")
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		r.logger.Info("Prove completed",
			zap.Duration("elapsed", elapsed),
		)
	}()

	var challenge bytes.Buffer
	challenge.Write(vk)
	challenge.Write(ch)
	proof, err := r.proofOfWork.Prove(challenge.Bytes(), int(omega))
	if err != nil {
		return nil, fmt.Errorf("failed to prove: %w", err)
	}

	return proof, nil
}

func (r *ResourceProof) Ver(vk []byte, omega float64, ch []byte, pi []byte) bool {
	r.logger.Info("Verify started")
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		r.logger.Info("Verify completed",
			zap.Duration("elapsed", elapsed),
		)
	}()

	var challenge bytes.Buffer
	challenge.Write(vk)
	challenge.Write(ch)
	result := r.proofOfWork.Verify(challenge.Bytes(), int(omega), pi)
	r.logger.Debug("Verifying proof of work",
		zap.Binary("vk", vk),
		zap.Bool("result", result),
		zap.Int("omega", int(omega)),
		zap.Binary("challenge", ch),
		zap.Binary("proof", pi),
	)
	return result
}

func New(logger *zap.Logger) *ResourceProof {
	return &ResourceProof{
		proofOfWork: &pow{},
		logger:      logger.Named("resource_proof"),
	}
}
