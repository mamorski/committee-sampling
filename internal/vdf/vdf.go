package vdf

import (
	"crypto/sha256"
	"time"
)

type Vdf struct{}

func New() *Vdf {
	return &Vdf{}
}

func (v *Vdf) Eval(x, vk []byte, delta int) ([]byte, []byte, error) {
	time.Sleep(time.Duration(delta) * time.Second)
	hash := sha256.Sum256(append(x, vk...))
	phi := hash[:]

	proof := sha256.Sum256(phi)

	return phi, proof[:], nil
}

func (v *Vdf) Verify(x, phi, pi, vk []byte) (bool, error) {
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
