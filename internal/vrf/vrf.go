package vrf

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/x509"
	"fmt"
	"hash"
	"time"

	"github.com/vechain/go-ecvrf"
	"go.uber.org/zap"
)

type Vrf struct {
	ecvrf  ecvrf.VRF
	lambda int
	logger *zap.Logger
}

func selectCurveAndHash(lambda int) (elliptic.Curve, func() hash.Hash, byte, error) {
	switch lambda {
	case 224:
		return elliptic.P224(), sha256.New224, 0x02, nil
	case 256:
		return elliptic.P256(), sha256.New, 0x01, nil
	case 384:
		return elliptic.P384(), sha512.New384, 0x03, nil
	default:
		return nil, nil, 0x00, fmt.Errorf("invalid lambda: %d", lambda)
	}
}

func (v *Vrf) Generate(lambda int) ([]byte, []byte, error) {
	v.logger.Info("Generate started")
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		v.logger.Info("Generate completed",
			zap.Duration("elapsed", elapsed),
		)
	}()

	c, h, suiteString, err := selectCurveAndHash(lambda)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to select c: %w", err)
	}
	v.ecvrf = ecvrf.New(&ecvrf.Config{
		Curve:       c,
		SuiteString: suiteString,
		Cofactor:    0x01,
		NewHasher:   h,
		Decompress:  elliptic.UnmarshalCompressed,
	})
	secretKey, err := ecdsa.GenerateKey(c, rand.Reader)
	if err != nil {
		return nil, nil, err
	}

	secretKeyBytes, err := x509.MarshalECPrivateKey(secretKey)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal private key: %w", err)
	}

	verificationKey, err := x509.MarshalPKIXPublicKey(&secretKey.PublicKey)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal public key: %w", err)
	}

	v.lambda = lambda
	return secretKeyBytes, verificationKey, nil
}

func (v *Vrf) Eval(x []byte, secretKey []byte) ([]byte, []byte, error) {
	v.logger.Info("Eval started")
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		v.logger.Info("Eval completed",
			zap.Duration("elapsed", elapsed),
		)
	}()

	sk, err := x509.ParseECPrivateKey(secretKey)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse private key: %w", err)
	}

	phi, pi, err := v.ecvrf.Prove(sk, x)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to prove: %w", err)
	}

	return phi, pi, nil

}

func (v *Vrf) Verify(x, phi, pi, verificationKey []byte) (bool, error) {
	v.logger.Debug("Verify started")
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		v.logger.Debug("Verify completed",
			zap.Duration("elapsed", elapsed),
		)
	}()

	pk, err := x509.ParsePKIXPublicKey(verificationKey)
	if err != nil {
		return false, fmt.Errorf("failed to parse public key: %w", err)
	}

	verificationKeyECDSA, ok := pk.(*ecdsa.PublicKey)
	if !ok {
		return false, fmt.Errorf("failed to cast public key to ECDSA")
	}
	beta, err := v.ecvrf.Verify(verificationKeyECDSA, x, pi)
	if err != nil {
		return false, fmt.Errorf("failed to verify: %w", err)
	}

	return string(beta) == string(phi), nil
}

func New(logger *zap.Logger) *Vrf {
	return &Vrf{
		lambda: 256,
		logger: logger.Named("vrf"),
	}
}
