package boot

import (
	"context"
	"crypto/sha256"
	"fmt"
	"math"
	"math/big"

	"github.com/mamorski/committee-sampling/internal/common"
	"github.com/mamorski/committee-sampling/internal/exante"
	"github.com/mamorski/committee-sampling/internal/expost"
	"github.com/mamorski/committee-sampling/internal/gce"
	"github.com/mamorski/committee-sampling/internal/mdag"
	"github.com/mamorski/committee-sampling/internal/network"
	"github.com/mamorski/committee-sampling/internal/resourcebound"
	"github.com/mamorski/committee-sampling/internal/resourceproof"
	"github.com/mamorski/committee-sampling/internal/vdf"
	"github.com/mamorski/committee-sampling/internal/vrf"
	"github.com/mamorski/committee-sampling/pkg/config"

	"go.uber.org/zap"
)

type GCE interface {
	Initialize(id string, sid string, vrf gce.VRF, rbexp gce.RBExp, vdf gce.VDF, delay int, lambda int) (*gce.LocalState, error)
	CommitteeElection(sid string, state *gce.LocalState, weight float64, vrf gce.VRF, rbexp gce.RBExp) ([]*common.CommitteeOutput, error)
}

type Bootstrap struct {
	id         string // Node ID, used for logging and network communication
	MDagExAnte exante.MDAG
	MDagExPost expost.MDAG
	ExAnte     resourcebound.ExAnte
	ExPost     resourcebound.ExPost
	RP         resourcebound.ResourceProof
	RbExp      gce.RBExp
	VDF        gce.VDF
	VRF        gce.VRF
	GCE        GCE
	Config     *config.Config
	Network    network.Network
	Logger     *zap.Logger
	Context    context.Context
}

func Oracle(data []byte) []byte {
	hash := sha256.Sum256(data)
	return hash[:]
}

//nolint:funlen
func New(ctx context.Context, cfg *config.Config, node network.Network, logger *zap.Logger, sync common.Synchronizer) (*Bootstrap, error) {

	vdFunc := vdf.New(logger)
	vrFunc := vrf.New(logger)

	// (VRF-Sortition-Grading). The Sortition filter function
	// ffilter(sid, id||vk(vrf), ch,(ϕ(vdf), π(vdf), ϕ(vrf), π(vrf))) =
	// VDF.Verify(H(id||vk(vrf) || ch), ϕ(vdf), π(vdf)) ∧ VRF.Verify(H(ϕ(vdf) || sid), ϕ(vrf), π(vrf), vk(vrf))
	// returns true iff both the VRF and VDF verification succeed.
	filterF := func(sid, id string, vk []byte, ch []byte, auxKey *common.AuxKey) bool {
		vdfInput := gce.HashData([]byte(id), vk, ch)
		vrfInput := gce.HashData(auxKey.PhiVDF, []byte(sid))

		logger.Debug("Filter function called, verifying VDF and VRF",
			zap.String("sender_id", id),
			zap.String("sid", sid),
			zap.Binary("vk", vk),
			zap.Binary("challenge", ch),
			zap.Binary("phi_vdf", auxKey.PhiVDF),
			zap.Binary("pi_vdf", auxKey.PiVDF),
			zap.Binary("VDF Input", vdfInput),
		)
		vdfRes, err := vdFunc.Verify(vdfInput, auxKey.PhiVDF, auxKey.PiVDF, vk)
		if err != nil {
			logger.Error("Failed to verify VDF", zap.Error(err))
			return false
		} else if !vdfRes {
			logger.Warn("VDF verification failed", zap.String("node_id", id))
			return false
		}

		vrfRes, err := vrFunc.Verify(vrfInput, auxKey.PhiVRF, auxKey.PiVRF, vk)
		if err != nil {
			logger.Warn("Failed to verify VRF", zap.Error(err))
			return false
		} else if !vrfRes {
			logger.Warn("VRF verification failed", zap.String("node_id", id))
			return false
		}

		return true
	}

	// The key grading function f_grade_∆W (sid, id||vk(vrf) , ch,(ϕ(vdf) , π(vdf) , ϕ(vrf) , π(vrf) ), Wi),
	// parameterized by a "weight disagreement" bound ∆W computes gi ← d + 1 − (Wi − n · 2^λ/ ϕ(vrf)+1) · 1 / ∆W
	// and returns grade min {d + 1, ⌊g⌋}.
	gradeF := func(sid string, vk []byte, ch []byte, auxKey *common.AuxKey, weight float64) int {

		g, err := computeGrade(cfg.Graph.GradingLevels, cfg.RunTime.CommitteeSize, cfg.RunTime.Lambda, weight, cfg.RunTime.DeltaW,
			auxKey.PhiVRF, logger.Named("GradeFunction"))
		if err != nil {
			logger.Warn("Failed to compute grade",
				zap.String("sid", sid),
				zap.Error(err),
			)
			return 0 // Return 0 if there's an error in grade calculation
		}

		logger.Info("Grading function calculated",
			zap.String("sid", sid),
			zap.Int("g", g),
			zap.Int("gradingLevels", cfg.Graph.GradingLevels),
			zap.Binary("Phi^VRF", auxKey.PhiVRF),
			zap.Binary("vk", vk),
		)

		return g
	}

	mdagExAnte := mdag.New(
		cfg.Graph.Diameter*cfg.Graph.GradingLevels,
		cfg.RunTime.SessionID,
		Oracle,
		node,
		sync,
		logger,
		common.ExAnteMDAG,
		"exante",
	)

	mdagExPost := mdag.New(
		cfg.Graph.Diameter*cfg.Graph.GradingLevels,
		cfg.RunTime.SessionID,
		Oracle,
		node,
		sync,
		logger,
		common.ExPostMDAG,
		"expost",
	)

	exAnte := exante.New(
		node,
		mdagExAnte,
		cfg.RunTime.SessionID,
		sync,
		cfg.Graph.GradingLevels,
		cfg.Graph.Diameter,
		gradeF,
		logger,
	)

	exPost := expost.New(
		node,
		mdagExPost,
		cfg.RunTime.SessionID,
		nil,
		sync,
		cfg.Graph.GradingLevels,
		cfg.Graph.Diameter,
		cfg.RunTime.Lambda,
		gradeF,
		logger,
	)

	rp := resourceproof.New(logger)
	rbExp := resourcebound.New(rp, exPost, exAnte, filterF, cfg.RunTime.Weight, logger)

	election := gce.New(logger)

	return &Bootstrap{
		id:         node.GetNodeID(),
		MDagExAnte: mdagExAnte,
		MDagExPost: mdagExPost,
		ExAnte:     exAnte,
		ExPost:     exPost,
		RP:         rp,
		RbExp:      rbExp,
		VDF:        vdFunc,
		VRF:        vrFunc,
		GCE:        election,
		Network:    node,
		Config:     cfg,
		Logger:     logger,
		Context:    ctx,
	}, nil

}

func (b *Bootstrap) Run() error {
	state, err := b.GCE.Initialize(b.id, b.Config.RunTime.SessionID, b.VRF, b.RbExp, b.VDF, b.Config.RunTime.Delay, b.Config.RunTime.Lambda)
	if err != nil {
		return fmt.Errorf("failed to initialize GCE: %w", err)
	}

	committee, err := b.GCE.CommitteeElection(b.Config.RunTime.SessionID, state, b.Config.RunTime.Weight, b.VRF, b.RbExp)
	if err != nil {
		return fmt.Errorf("failed to perform committee election: %w", err)
	}

	b.Logger.Info("Committee elected", zap.Any("committee", committee))
	for _, member := range committee {
		fmt.Printf("ID:    %s\n", member.ID)
		fmt.Printf("VK:    %s\n", member.VK)
		fmt.Printf("Grade: %d\n", member.Grade)
		fmt.Println("--------------------------------")
	}

	return nil
}

// computeGrade parses φ from a VRF‐generated byte slice and computes:
//
//	gᵢ = d + 1 − (Wᵢ − n·2^λ/(φ+1))·(1/ΔW)
//	return min{ d+1, floor(gᵢ) }.
//
// Inputs:
//   - d         : integer "d".
//   - Wi        : float64 (Wᵢ).
//   - n         : integer n.
//   - beta      : []byte (VRF output, interpreted as a big‐endian integer φ).
//   - deltaW    : float64 (ΔW, must be ≠ 0).
//   - lambda    : integer λ (bit‐length for 2^λ).
//
// Returns:
//   - int: ⌊gᵢ⌋ clamped to ≤ (d+1).
//   - error if any input is invalid.
//
// This implementation uses big.Int and big.Float to compute 2^λ/(φ+1) with enough precision
// before converting to float64. Finally, it floors and clamps as specified.
func computeGrade(d, n, lambda int, Wi, deltaW float64, beta []byte, logger *zap.Logger) (int, error) {
	// 1) Validate inputs
	if deltaW == 0 {
		return 0, fmt.Errorf("deltaW must be nonzero")
	}

	// 2) Parse φ from the provided []byte
	phiInt := new(big.Int).SetBytes(beta)
	if phiInt.Sign() <= 0 {
		return 0, fmt.Errorf("phi must be > 0")
	}

	// 3) Compute φ + 1 as big.Int
	phiPlusOne := new(big.Int).Add(phiInt, big.NewInt(1))

	// 4) Compute 2^λ as big.Int
	twoToLambdaInt := new(big.Int).Lsh(big.NewInt(1), uint(lambda)) //nolint:gosec

	// 5) Convert numerator (2^λ) and denominator (φ+1) to big.Float
	numerator := new(big.Float).SetInt(twoToLambdaInt)
	denominator := new(big.Float).SetInt(phiPlusOne)

	// 6) Compute ratio = 2^λ / (φ + 1) as big.Float, then to float64
	ratioF, _ := new(big.Float).Quo(numerator, denominator).Float64()
	//    ratioF ≈ 2^λ/(φ+1)

	// 7) Compute the subterm: (Wᵢ − n·ratioF)
	sub := Wi - float64(n)*ratioF

	// 8) Multiply by (1/ΔW)
	term := sub / deltaW

	// 9) Compute gᵢ = (d + 1) − term
	g := float64(d+1) - term

	// Log the computed values for debugging
	logger.Debug("Computed grade components",
		zap.Int("d", d),
		zap.Int("n", n),
		zap.Float64("Wi", Wi),
		zap.Float64("ratioF", ratioF),
		zap.Float64("sub", sub),
		zap.Float64("term", term),
		zap.Float64("g", g),
		zap.String("phi", phiInt.String()),
		zap.String("phi_plus_one", phiPlusOne.String()),
		zap.String("twoToLambdaInt", twoToLambdaInt.String()),
		zap.Int("lambda", lambda),
	)
	// 10) Take floor(gᵢ)
	floorG := math.Floor(g)

	finalGrade := math.Min(float64(d+1), floorG)

	return int(finalGrade), nil
}
