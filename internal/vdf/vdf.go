package vdf

import (
	"time"

	"github.com/mamorski/committee-sampling/internal/hash"
	"go.uber.org/zap"
)

type Vdf struct {
	logger *zap.Logger
}

func New(logger *zap.Logger) *Vdf {
	return &Vdf{
		logger: logger.Named("vdf"),
	}
}

func (v *Vdf) Eval(x, vk []byte, delta int) ([]byte, []byte, error) {
	v.logger.Info("Eval started")
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		v.logger.Info("Eval completed",
			zap.Duration("elapsed", elapsed),
		)
	}()

	time.Sleep(time.Duration(delta) * time.Second)
	phi := hash.Sum(x, vk)

	proof := hash.Sum(phi)

	return phi, proof, nil
}

func (v *Vdf) Verify(x, phi, pi, vk []byte) (bool, error) {
	v.logger.Debug("Verify started")
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		v.logger.Debug("Verify completed",
			zap.Duration("elapsed", elapsed),
		)
	}()

	expectedPhi := hash.Sum(x, vk)

	if string(phi) != string(expectedPhi) {
		return false, nil
	}

	proof := hash.Sum(phi)
	if string(proof) != string(pi) {
		return false, nil
	}

	return true, nil
}
