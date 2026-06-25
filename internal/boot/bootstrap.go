package boot

import (
	"context"
	"fmt"
	"math"
	"math/big"
	stdsync "sync" // aliased: the New() param `sync common.Synchronizer` shadows the package name
	"sync/atomic"

	"github.com/mamorski/committee-sampling/internal/common"
	"github.com/mamorski/committee-sampling/internal/exante"
	"github.com/mamorski/committee-sampling/internal/expost"
	"github.com/mamorski/committee-sampling/internal/gce"
	"github.com/mamorski/committee-sampling/internal/hash"
	"github.com/mamorski/committee-sampling/internal/mdag"
	"github.com/mamorski/committee-sampling/internal/network"
	"github.com/mamorski/committee-sampling/internal/resourcebound"
	"github.com/mamorski/committee-sampling/internal/resourceproof"
	"github.com/mamorski/committee-sampling/internal/vdf"
	"github.com/mamorski/committee-sampling/internal/vrf"
	"github.com/mamorski/committee-sampling/pkg/config"
	pb "github.com/mamorski/committee-sampling/pkg/proto"

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

	gradeCacheHits   *atomic.Int64
	gradeCacheMisses *atomic.Int64
}

//nolint:funlen
func New(ctx context.Context, cfg *config.Config, node network.Network, logger *zap.Logger, sync common.Synchronizer) (*Bootstrap, error) {

	vdFunc := vdf.New(logger)
	vrFunc := vrf.New(logger)

	// (VRF-Sortition-Grading). The Sortition filter function
	// ffilter(sid, id||vk(vrf), ch,(ϕ(vdf), π(vdf), ϕ(vrf), π(vrf))) =
	// VDF.Verify(H(id||vk(vrf) || ch), ϕ(vdf), π(vdf)) ∧ VRF.Verify(H(ϕ(vdf) || sid), ϕ(vrf), π(vrf), vk(vrf))
	// returns true iff both the VRF and VDF verification succeed.
	filterF := func(sid, id string, vk []byte, ch []byte, auxKey *pb.AuxKeyMessage) bool {
		if auxKey == nil {
			logger.Warn("Filter function received nil auxKey")
			return false
		}

		vdfInput := hash.Sum([]byte(id), vk, ch)
		vrfInput := hash.Sum(auxKey.PhiVdf, []byte(sid))

		if logger.Core().Enabled(zap.DebugLevel) {
			logger.Debug(
				"Filter function called, verifying VDF and VRF",
				zap.String("sender_id", id),
				zap.String("sid", sid),
				zap.Binary("vk", vk),
				zap.Binary("challenge", ch),
				zap.Binary("phi_vdf", auxKey.PhiVdf),
				zap.Binary("pi_vdf", auxKey.PiVdf),
				zap.Binary("VDF Input", vdfInput),
			)
		}
		vdfRes, err := vdFunc.Verify(vdfInput, auxKey.PhiVdf, auxKey.PiVdf, vk)
		if err != nil {
			logger.Warn("Failed to verify VDF", zap.Error(err))
			return false
		} else if !vdfRes {
			logger.Warn("VDF verification failed", zap.String("node_id", id))
			return false
		}

		vrfRes, err := vrFunc.Verify(vrfInput, auxKey.PhiVrf, auxKey.PiVrf, vk)
		if err != nil {
			logger.Warn("Failed to verify VRF", zap.Error(err))
			return false
		} else if !vrfRes {
			logger.Warn("VRF verification failed", zap.String("node_id", id))
			return false
		}

		return true
	}

	// gradeCache memoizes computeGrade for the run. The same member's φ(vrf) is
	// gossiped and re-graded hundreds of thousands of times, and grading is pure in
	// φ(vrf) given the run-fixed parameters and weight (big.Int/big.Float math, no
	// crypto but allocation-heavy). Caching by those inputs collapses the redundant
	// work to ~one compute per distinct member. The closure is shared by both ExPost
	// and ExAnte; sync.Map fits the write-once / read-many / stable-key pattern.
	var gradeCache stdsync.Map // map[string]int
	gradeCacheHits := &atomic.Int64{}
	gradeCacheMisses := &atomic.Int64{}

	// The key grading function f_grade_∆W (sid, id||vk(vrf) , ch,(ϕ(vdf) , π(vdf), ϕ(vrf), π(vrf) ), Wi),
	// parameterized by a "weight disagreement" bound ∆W computes gi ← d + 1 − (Wi − n · 2^λ/ ϕ(vrf)+1) · 1 / ∆W
	// and returns grade min {d + 1, ⌊g⌋}.
	gradeF := func(sid string, vk []byte, ch []byte, auxKey *pb.AuxKeyMessage, weight float64) int {
		if auxKey == nil {
			logger.Warn("Grade function received nil auxKey")
			return 0
		}

		// Grade depends only on φ(vrf) among per-message inputs: the grading params
		// are run-fixed config and weight is the run-fixed cfg.Committee.TotalW (this
		// closure is only ever called with that single value), so φ(vrf) alone keys
		// the cache.
		cacheKey := string(auxKey.PhiVrf)
		if v, ok := gradeCache.Load(cacheKey); ok {
			gradeCacheHits.Add(1)
			return v.(int)
		}
		gradeCacheMisses.Add(1)

		g, err := computeGrade(
			cfg.Graph.GradingLevels,
			cfg.Committee.CommitteeSize,
			cfg.Committee.Lambda,
			weight,
			cfg.Committee.DeltaW,
			auxKey.PhiVrf,
			logger.Named("GradeFunction"),
		)
		if err != nil {
			logger.Warn(
				"Failed to compute grade", zap.String("sid", sid), zap.Error(err),
			)
			gradeCache.Store(cacheKey, 0) // memoize the rejection too
			return 0                      // Return 0 if there's an error in grade calculation
		}

		if logger.Core().Enabled(zap.DebugLevel) {
			logger.Debug(
				"Grading function calculated",
				zap.String("sid", sid),
				zap.Int("g", g),
				zap.Int("gradingLevels", cfg.Graph.GradingLevels),
				zap.Binary("Phi^VRF", auxKey.PhiVrf),
				zap.Binary("vk", vk),
			)
		}

		gradeCache.Store(cacheKey, g)
		return g
	}

	mdagExAnte := mdag.New(
		cfg.Graph.Diameter*cfg.Graph.GradingLevels,
		cfg.Committee.SessionID,
		hash.Oracle,
		node,
		sync,
		logger,
		common.ExAnteMDAG,
		"exanteMDAG",
	)

	mdagExPost := mdag.New(
		cfg.Graph.Diameter*cfg.Graph.GradingLevels,
		cfg.Committee.SessionID,
		hash.Oracle,
		node,
		sync,
		logger,
		common.ExPostMDAG,
		"expostMDAG",
	)

	exAnte := exante.New(node, mdagExAnte, cfg.Committee.SessionID, sync, cfg.Graph.GradingLevels, cfg.Graph.Diameter, gradeF, logger)

	exPost := expost.New(
		node,
		mdagExPost,
		cfg.Committee.SessionID,
		nil,
		sync,
		cfg.Graph.GradingLevels,
		cfg.Graph.Diameter,
		cfg.Committee.Lambda,
		gradeF,
		logger,
	)

	rp := resourceproof.New(logger)
	rbExp := resourcebound.New(rp, exPost, exAnte, filterF, cfg.Committee.Weight, cfg.Committee.NoAdversarial, logger)

	election := gce.New(logger)

	return &Bootstrap{
		id:               node.GetNodeID(),
		MDagExAnte:       mdagExAnte,
		MDagExPost:       mdagExPost,
		ExAnte:           exAnte,
		ExPost:           exPost,
		RP:               rp,
		RbExp:            rbExp,
		VDF:              vdFunc,
		VRF:              vrFunc,
		GCE:              election,
		Network:          node,
		gradeCacheHits:   gradeCacheHits,
		gradeCacheMisses: gradeCacheMisses,
		Config:           cfg,
		Logger:           logger,
		Context:          ctx,
	}, nil

}

func (b *Bootstrap) Run() error {
	state, err := b.GCE.Initialize(
		b.id, b.Config.Committee.SessionID, b.VRF, b.RbExp, b.VDF, b.Config.Committee.Delay, b.Config.Committee.Lambda,
	)
	if err != nil {
		return fmt.Errorf("failed to initialize GCE: %w", err)
	}

	committee, err := b.GCE.CommitteeElection(b.Config.Committee.SessionID, state, b.Config.Committee.TotalW, b.VRF, b.RbExp)
	if err != nil {
		return fmt.Errorf("failed to perform committee election: %w", err)
	}

	b.Logger.Warn("Committee elected", zap.Int("size", len(committee)), zap.Any("committee", committee))
	b.Logger.Warn("cache stats", zap.String("cache", "grade"),
		zap.Int64("hits", b.gradeCacheHits.Load()), zap.Int64("misses", b.gradeCacheMisses.Load()))
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
//   - d: integer "d".
//   - Wi: float64 (Wᵢ).
//   - n: integer n.
//   - beta: []byte (VRF output, interpreted as a big‐endian integer φ).
//   - deltaW: float64 (ΔW, must be ≠ 0).
//   - lambda: integer λ (bit‐length for 2^λ).
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

	// 3) Convert phi to float64, add 1.0
	phiFloat, _ := phiInt.Float64()
	phiPlusOneF := phiFloat + 1.0

	// 4) Compute 2^lambda as float64
	twoToLambda := math.Ldexp(1.0, lambda)

	// 5) Compute a ratio = 2^λ / (φ + 1) as float64
	ratioF := twoToLambda / phiPlusOneF

	// 6) Compute the subterm: (Wᵢ − n·ratioF)
	sub := Wi - float64(n)*ratioF

	// 7) Multiply by (1/ΔW)
	term := sub / deltaW

	// 8) Compute gᵢ = (d + 1) − term
	g := float64(d+1) - term

	// Log the computed values for debugging
	if logger.Core().Enabled(zap.DebugLevel) {
		logger.Debug(
			"Computed grade components",
			zap.Int("d", d),
			zap.Int("n", n),
			zap.Float64("Wi", Wi),
			zap.Float64("ratioF", ratioF),
			zap.Float64("sub", sub),
			zap.Float64("term", term),
			zap.Float64("g", g),
			zap.String("phi", phiInt.String()),
			zap.Float64("phi_float", phiFloat),
			zap.Int("lambda", lambda),
		)
	}
	// 9) Take floor(gᵢ)
	floorG := math.Floor(g)

	finalGrade := math.Min(float64(d+1), floorG)

	return int(finalGrade), nil
}
