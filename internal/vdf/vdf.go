package vdf

import (
	"crypto/sha256"
	"time"

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
	hash := sha256.Sum256(append(x, vk...))
	phi := hash[:]

	proof := sha256.Sum256(phi)

	return phi, proof[:], nil
}

func (v *Vdf) Verify(x, phi, pi, vk []byte) (bool, error) {
	v.logger.Info("Verify started")
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		v.logger.Info("Verify completed",
			zap.Duration("elapsed", elapsed),
		)
	}()

	hash := sha256.Sum256(append(x, vk...))
	expectedPhi := hash[:]

	if string(phi) != string(expectedPhi) {
		return false, nil
	}

	proof := sha256.Sum256(phi)
	if string(proof[:]) != string(pi) {
		return false, nil
	}

	return true, nil
}
