package resource_bound

import (
	"errors"
	"reflect"
	"testing"

	"github.com/mamorski/committee-sampling/internal/common"
)

// fakeResourceProof returns errors based on the vk value.
type fakeResourceProof struct{}

func (f *fakeResourceProof) Setup(vk []byte) ([]byte, error) {
	if string(vk) == "fail_setup" {
		return nil, errors.New("setup error")
	}
	return []byte("auxRP"), nil
}

func (f *fakeResourceProof) Prove(vk []byte, _ float64, _ []byte, _ []byte) ([]byte, error) {
	if string(vk) == "fail_rp_prove" {
		return nil, errors.New("prove error")
	}
	return []byte("piRP"), nil
}

func (f *fakeResourceProof) Ver(vk []byte, _ float64, _ []byte, _ []byte) bool {
	return string(vk) != "fail_ver"
}

type fakeExPost struct{}

func (f *fakeExPost) Generate(session string, _ []byte) ([][][]byte, []byte, error) {
	if session == "fail_exp_generate" {
		return nil, nil, errors.New("exp generate error")
	}
	return [][][]byte{{[]byte("sigmaExp")}}, []byte("challenge"), nil
}

func (f *fakeExPost) Verify(
	session string,
	vk []byte,
	fSigmaExp *common.FSigmaExp,
	auxTag *common.AuxTag,
	_ float64,
	filter filterTagFunc) (map[common.Key]common.O, error) {

	if session == "fail_exp_verify" {
		return nil, errors.New("exp verify error")
	}
	// If the provided filter returns false, simulate no output.
	if !filter(session, vk, fSigmaExp.Challenge, auxTag) {
		return map[common.Key]common.O{}, nil
	}
	if session == "nomatch" {
		return map[common.Key]common.O{}, nil
	}
	key := common.Key{VK: "key1", Ch: string(fSigmaExp.Challenge)}
	return map[common.Key]common.O{
		key: {
			VK:        vk,
			Challenge: fSigmaExp.Challenge,
			Aux:       auxTag.AuxKey,
			Grade:     8,
		},
	}, nil
}

type fakeExAnte struct{}

func (f *fakeExAnte) Generate(session string, _ []byte, _ []byte, _ []byte) ([][][]byte, error) {
	if session == "fail_exa_generate" {
		return nil, errors.New("exa generate error")
	}
	return [][][]byte{{[]byte("sigmaExa")}}, nil
}

func (f *fakeExAnte) Verify(
	session string,
	vk []byte,
	_ [][][]byte,
	auxTag *common.AuxTag,
	_ float64,
	filter filterTagFunc) (map[common.Key]common.O, error) {

	if session == "fail_exa_verify" {
		return nil, errors.New("exante verify error")
	}

	if !filter(session, vk, []byte("challenge"), auxTag) {
		return map[common.Key]common.O{}, nil
	}

	if session == "nomatch" {
		key := common.Key{VK: "other", Ch: "challenge"}
		return map[common.Key]common.O{
			key: {
				VK:        vk,
				Challenge: []byte("challenge"),
				Aux:       auxTag.AuxKey,
				Grade:     5,
			},
		}, nil
	}

	key := common.Key{VK: "key1", Ch: "challenge"}
	return map[common.Key]common.O{
		key: {
			VK:        vk,
			Challenge: []byte("challenge"),
			Aux:       auxTag.AuxKey,
			Grade:     5,
		},
	}, nil
}

func alwaysTrueFilter(_ string, _ []byte, _ []byte, _ *common.AuxKey) bool {
	return true
}

func alwaysFalseFilter(_ string, _ []byte, _ []byte, _ *common.AuxKey) bool {
	return false
}

func TestGen_Success(t *testing.T) {
	rp := &fakeResourceProof{}
	exp := &fakeExPost{}
	exa := &fakeExAnte{}
	weight := 1.0
	rbexp := New(rp, exp, exa, alwaysTrueFilter, weight)
	sid := "session1"
	vk := []byte("vk")

	challenge, proof, err := rbexp.Gen(sid, vk)
	if err != nil {
		t.Fatalf("Gen returned error: %v", err)
	}

	if string(challenge) != "challenge" {
		t.Errorf("Expected challenge 'challenge', got %s", challenge)
	}

	if string(proof.PiRP) != "piRP" {
		t.Errorf("Expected proof.PiRP 'piRP', got %s", proof.PiRP)
	}

	if len(proof.SigmaExp) != 1 || len(proof.SigmaExp[0]) != 1 || string(proof.SigmaExp[0][0]) != "sigmaExp" {
		t.Errorf("Unexpected SigmaExp: %v", proof.SigmaExp)
	}

	if len(proof.SigmaExa) != 1 || len(proof.SigmaExa[0]) != 1 || string(proof.SigmaExa[0][0]) != "sigmaExa" {
		t.Errorf("Unexpected SigmaExa: %v", proof.SigmaExa)
	}
}

func TestGen_ErrorSetup(t *testing.T) {
	rp := &fakeResourceProof{}
	exp := &fakeExPost{}
	exa := &fakeExAnte{}
	rbexp := New(rp, exp, exa, alwaysTrueFilter, 1.0)
	sid := "session1"
	vk := []byte("fail_setup")

	_, _, err := rbexp.Gen(sid, vk)
	if err == nil || err.Error() != "setup error" {
		t.Fatalf("Expected setup error, got %v", err)
	}
}

func TestGen_ErrorExpGenerate(t *testing.T) {
	rp := &fakeResourceProof{}
	exp := &fakeExPost{}
	exa := &fakeExAnte{}
	rbexp := New(rp, exp, exa, alwaysTrueFilter, 1.0)
	sid := "fail_exp_generate"
	vk := []byte("vk")

	_, _, err := rbexp.Gen(sid, vk)
	if err == nil || err.Error() != "exp generate error" {
		t.Fatalf("Expected exp.Generate error, got %v", err)
	}
}

func TestGen_ErrorRPProve(t *testing.T) {
	rp := &fakeResourceProof{}
	exp := &fakeExPost{}
	exa := &fakeExAnte{}
	rbexp := New(rp, exp, exa, alwaysTrueFilter, 1.0)
	sid := "session1"
	vk := []byte("fail_rp_prove")

	_, _, err := rbexp.Gen(sid, vk)
	if err == nil || err.Error() != "prove error" {
		t.Fatalf("Expected rp.Prove error, got %v", err)
	}
}

func TestGen_ErrorExaGenerate(t *testing.T) {
	rp := &fakeResourceProof{}
	exp := &fakeExPost{}
	exa := &fakeExAnte{}
	rbexp := New(rp, exp, exa, alwaysTrueFilter, 1.0)
	sid := "fail_exa_generate"
	vk := []byte("vk")

	_, _, err := rbexp.Gen(sid, vk)
	if err == nil || err.Error() != "exa generate error" {
		t.Fatalf("Expected exa.Generate error, got %v", err)
	}
}

func TestVer_Success(t *testing.T) {
	rp := &fakeResourceProof{}
	exp := &fakeExPost{}
	exa := &fakeExAnte{}
	rbexp := New(rp, exp, exa, alwaysTrueFilter, 1.0)
	sid := "session1"
	vk := []byte("vk")
	challenge := []byte("challenge")
	proof := &common.RBExpProof{
		PiRP:     []byte("piRP"),
		SigmaExp: [][][]byte{{[]byte("sigmaExp")}},
		SigmaExa: [][][]byte{{[]byte("sigmaExa")}},
	}
	auxKey := &common.AuxKey{
		PhiVRF: []byte("phi"),
		PiVRF:  []byte("pivr"),
		PhiVDF: []byte("phivdf"),
		PiVDF:  []byte("pivdf"),
	}

	outputs, err := rbexp.Ver(sid, vk, challenge, proof, auxKey, 0)
	if err != nil {
		t.Fatalf("Ver returned error: %v", err)
	}
	if len(outputs) != 1 {
		t.Fatalf("Expected 1 output, got %d", len(outputs))
	}
	out := outputs[0]
	if out.SID != sid {
		t.Errorf("Expected SID %s, got %s", sid, out.SID)
	}
	if !reflect.DeepEqual(out.VK, vk) {
		t.Errorf("Expected VK %v, got %v", vk, out.VK)
	}
	if string(out.Challenge) != "challenge" {
		t.Errorf("Expected Challenge 'challenge', got %s", out.Challenge)
	}
	// fakeExPost returns Grade 8 and fakeExAnte returns Grade 5, so the minimum is 5.
	if out.Grade != 5 {
		t.Errorf("Expected Grade 5, got %d", out.Grade)
	}
}

func TestVer_NoMatchingOutput(t *testing.T) {
	rp := &fakeResourceProof{}
	exp := &fakeExPost{}
	exa := &fakeExAnte{}
	rbexp := New(rp, exp, exa, alwaysTrueFilter, 1.0)
	// Using session "nomatch" forces both Verify methods to return no matching key.
	sid := "nomatch"
	vk := []byte("vk")
	challenge := []byte("challenge")
	proof := &common.RBExpProof{
		PiRP:     []byte("piRP"),
		SigmaExp: [][][]byte{{[]byte("sigmaExp")}},
		SigmaExa: [][][]byte{{[]byte("sigmaExa")}},
	}
	auxKey := &common.AuxKey{}

	_, err := rbexp.Ver(sid, vk, challenge, proof, auxKey, 0)
	if err == nil || err.Error() != "verification failed: no matching output" {
		t.Fatalf("Expected no matching output error, got %v", err)
	}
}

func TestVer_ErrorExPost(t *testing.T) {
	rp := &fakeResourceProof{}
	exp := &fakeExPost{}
	exa := &fakeExAnte{}
	rbexp := New(rp, exp, exa, alwaysTrueFilter, 1.0)
	sid := "fail_exp_verify"
	vk := []byte("vk")
	challenge := []byte("challenge")
	proof := &common.RBExpProof{
		PiRP:     []byte("piRP"),
		SigmaExp: [][][]byte{{[]byte("sigmaExp")}},
		SigmaExa: [][][]byte{{[]byte("sigmaExa")}},
	}
	auxKey := &common.AuxKey{}

	_, err := rbexp.Ver(sid, vk, challenge, proof, auxKey, 0)
	if err == nil || err.Error() != "exp verify error" {
		t.Fatalf("Expected exp.Verify error, got %v", err)
	}
}

func TestVer_ErrorExAnte(t *testing.T) {
	rp := &fakeResourceProof{}
	exp := &fakeExPost{}
	exa := &fakeExAnte{}
	rbexp := New(rp, exp, exa, alwaysTrueFilter, 1.0)
	sid := "fail_exa_verify"
	vk := []byte("vk")
	challenge := []byte("challenge")
	proof := &common.RBExpProof{
		PiRP:     []byte("piRP"),
		SigmaExp: [][][]byte{{[]byte("sigmaExp")}},
		SigmaExa: [][][]byte{{[]byte("sigmaExa")}},
	}
	auxKey := &common.AuxKey{}

	_, err := rbexp.Ver(sid, vk, challenge, proof, auxKey, 0)
	if err == nil || err.Error() != "exante verify error" {
		t.Fatalf("Expected exa.Verify error, got %v", err)
	}
}

func TestVer_FilterFalse(t *testing.T) {
	rp := &fakeResourceProof{}
	exp := &fakeExPost{}
	exa := &fakeExAnte{}
	// Use a filter that always returns false.
	rbexp := New(rp, exp, exa, alwaysFalseFilter, 1.0)
	sid := "session1"
	vk := []byte("vk")
	challenge := []byte("challenge")
	proof := &common.RBExpProof{
		PiRP:     []byte("piRP"),
		SigmaExp: [][][]byte{{[]byte("sigmaExp")}},
		SigmaExa: [][][]byte{{[]byte("sigmaExa")}},
	}
	auxKey := &common.AuxKey{}

	_, err := rbexp.Ver(sid, vk, challenge, proof, auxKey, 0)
	if err == nil || err.Error() != "verification failed: no matching output" {
		t.Fatalf("Expected filter false error, got %v", err)
	}
}
