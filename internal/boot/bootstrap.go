package boot

import (
	"crypto/sha256"
	"math/big"
	"time"

	"github.com/beevik/ntp"
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
	Config     *config.Config
	Network    network.Network
	Logger     *zap.Logger
}

type StartTimes struct {
	ExAnteMDAG   time.Time
	ExPostMDAG   time.Time
	ExPostVerify time.Time
	ExAnteVerify time.Time
}

func Oracle(data []byte) []byte {
	hash := sha256.Sum256(data)
	return hash[:]
}

//nolint:funlen
func New(cfg *config.Config, node network.Network, logger *zap.Logger) (*Bootstrap, error) {

	startTimes := CalculateStartTimes(cfg)
	// Sleep until the ExPost MDAG starts, minus 15 seconds to allow for setup.
	// This ensures that enough neighbors are connected before starting the MDAG.
	time.Sleep(time.Until(startTimes.ExPostMDAG.Add(-15 * time.Second)))

	vdFunc := vdf.New()
	vrFunc := vrf.New()

	filterF := func(sid, id string, vk []byte, ch []byte, auxKey *common.AuxKey) bool {
		vdfInput := gce.HashData([]byte(id), vk, ch)
		vrfInput := gce.HashData(auxKey.PhiVDF, []byte(sid))

		logger.Debug("Filter function called, verifying VDF and VRF",
			zap.String("node_id", id),
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
			logger.Error("Failed to verify VRF", zap.Error(err))
			return false
		} else if !vrfRes {
			logger.Warn("VRF verification failed", zap.String("node_id", id))
			return false
		}

		return true
	}

	gradeF := func(sid string, vk []byte, ch []byte, auxKey *common.AuxKey, weight float64) int {
		phiInt := new(big.Int).SetBytes(auxKey.PhiVRF)
		n := big.NewInt(int64(cfg.RunTime.CommitteeSize))
		d := new(big.Float).SetInt64(int64(cfg.Graph.GradingLevels))
		w := big.NewFloat(weight)

		// 1 / delta_w - currently set to 1.0, can be adjusted based on the protocol requirements
		deltaW := big.NewFloat(1.0)

		// 2^lambda
		base := big.NewInt(2)
		exp := big.NewInt(int64(cfg.RunTime.Lambda))
		twoPowLambda := new(big.Int).Exp(base, exp, nil)
		logger.Debug("Grading function parameters",
			zap.String("sid", sid),
			zap.Binary("vk", vk),
			zap.Any("2^lambda", twoPowLambda),
			zap.Int("lambda", cfg.RunTime.Lambda),
			zap.Any("phi_vrf", phiInt.Int64()),
		)

		// d + 1
		d = d.Add(d, big.NewFloat(1.0))

		// 2^lambda / (phi_vrf + 1)
		twoPowLambda = twoPowLambda.Div(twoPowLambda, phiInt.Add(phiInt, big.NewInt(1)))

		// n * (2^lambda / (phi_vrf + 1))
		n = n.Mul(n, twoPowLambda)

		// w - n * (2^lambda / (phi_vrf + 1))
		w = w.Sub(w, new(big.Float).SetInt(n))

		// r = (w - n * (2^lambda / (phi_vrf + 1))) * 1 / delta_w
		r := w.Mul(w, deltaW)

		// g = floor(d + 1 - (w - n * (2^lambda / (phi_vrf + 1))) * 1 / delta_w)
		g, _ := big.NewFloat(0).Sub(d, r).Int64()
		logger.Debug("Grading function calculated",
			zap.String("sid", sid),
			zap.Int64("g", g),
			zap.Int64("gradingLevels", int64(cfg.Graph.GradingLevels)),
		)

		if int64(cfg.Graph.GradingLevels+1) <= g {
			return cfg.Graph.GradingLevels + 1
		}

		return int(g)
	}

	mdagExAnte := mdag.New(
		cfg.Graph.Diameter*cfg.Graph.GradingLevels,
		cfg.RunTime.SessionID,
		Oracle,
		node,
		cfg.RunTime.MDAGRoundTimeout,
		logger,
		startTimes.ExAnteMDAG,
		"exante",
	)

	mdagExPost := mdag.New(
		cfg.Graph.Diameter*cfg.Graph.GradingLevels,
		cfg.RunTime.SessionID,
		Oracle,
		node,
		cfg.RunTime.MDAGRoundTimeout,
		logger,
		startTimes.ExPostMDAG,
		"expost",
	)

	exAnte := exante.New(
		node,
		mdagExAnte,
		cfg.RunTime.SessionID,
		startTimes.ExAnteVerify,
		cfg.RunTime.ExAnteRoundTimeout,
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
		startTimes.ExPostVerify,
		cfg.RunTime.ExPostRoundTimeout,
		cfg.Graph.GradingLevels,
		cfg.Graph.Diameter,
		cfg.RunTime.Lambda,
		gradeF,
		logger,
	)

	rp := resourceproof.New()
	rbExp := resourcebound.New(rp, exPost, exAnte, filterF, cfg.RunTime.Weight, logger)

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
		Network:    node,
		Config:     cfg,
		Logger:     logger,
	}, nil

}

func CalculateStartTimes(cfg *config.Config) *StartTimes {
	response, err := ntp.Query("time.nist.gov")
	if err != nil {
		panic("Failed to query NTP server: " + err.Error())
	}

	rounds := cfg.Graph.Diameter * cfg.Graph.GradingLevels
	startTime := time.Unix(cfg.RunTime.StartTime, 0).UTC().Add(response.ClockOffset)

	return &StartTimes{
		ExPostMDAG:   startTime,
		ExAnteMDAG:   startTime.Add(cfg.RunTime.MDAGRoundTimeout*time.Duration(rounds) + time.Minute),
		ExPostVerify: startTime.Add(cfg.RunTime.MDAGRoundTimeout*time.Duration(rounds) + (time.Second * 120)),
		ExAnteVerify: startTime.Add(cfg.RunTime.ExPostRoundTimeout*time.Duration(rounds) + (time.Second * 10)),
	}
}
