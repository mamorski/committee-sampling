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

	"github.com/vechain/go-ecvrf"
)

type Vrf struct {
	ecvrf ecvrf.VRF
}

func selectCurveAndHash(lambda int) (elliptic.Curve, func() hash.Hash, error) {
	switch lambda {
	case 28:
		return elliptic.P224(), sha256.New224, nil
	case 32:
		return elliptic.P256(), sha256.New, nil
	case 48:
		return elliptic.P384(), sha512.New384, nil
	default:
		return nil, nil, fmt.Errorf("invalid lambda: %d", lambda)
	}
}

func (v *Vrf) Generate(lambda int) ([]byte, []byte, error) {
	c, h, err := selectCurveAndHash(lambda)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to select c: %w", err)
	}
	v.ecvrf = ecvrf.New(&ecvrf.Config{
		Curve:       c,
		SuiteString: 0x01,
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

	return secretKeyBytes, verificationKey, nil
}

func (v *Vrf) Eval(x []byte, secretKey []byte) ([]byte, []byte, error) {
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

func New() *Vrf {
	return &Vrf{}
}
