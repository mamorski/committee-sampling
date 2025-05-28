package vdf

import (
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"time"
)

type Vdf struct {
	lambda int
	delta  int
	vk     []byte
}

func New() *Vdf {
	return &Vdf{}
}

func (v *Vdf) Setup(lambda, delta int) ([]byte, error) {
	v.lambda = lambda
	v.delta = delta

	vk := make([]byte, lambda/8)
	_, err := rand.Read(vk)
	if err != nil {
		return nil, fmt.Errorf("failed to generate verification key: %w", err)
	}

	v.vk = vk
	return vk, nil
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
