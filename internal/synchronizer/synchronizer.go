package synchronizer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/beevik/ntp"
	"go.uber.org/zap"

	"github.com/mamorski/committee-sampling/internal/common"
	"github.com/mamorski/committee-sampling/pkg/config"
)

var AllSteps = []common.Step{common.Network, common.ExPostMDAG, common.ExAnteMDAG, common.ExPostVerify, common.ExAnteVerify}

type StartTimes struct {
	StartBuildingNetwork time.Time
	ExPostMDAG           time.Time
	ExAnteMDAG           time.Time
	ExPostVerify         time.Time
	ExAnteVerify         time.Time
}

type Synchronizer struct {
	cfg           *config.Config
	logger        *zap.Logger
	stopChan      chan struct{}
	roundChannels map[common.Step][]chan struct{}
	rounds        int
	mu            sync.Mutex
	stopOnce      sync.Once
	clockSkew     time.Duration
}

func New(_ context.Context, cfg *config.Config, logger *zap.Logger) (*Synchronizer, error) {
	var clockSkew time.Duration
	if cfg.Network.Adversary.Enabled {
		clockSkew = cfg.Network.Adversary.ClockSkew
	}
	s := &Synchronizer{
		cfg:           cfg,
		logger:        logger,
		stopChan:      make(chan struct{}),
		roundChannels: make(map[common.Step][]chan struct{}),
		clockSkew:     clockSkew,
	}

	s.rounds = cfg.Graph.Diameter * cfg.Graph.GradingLevels
	for _, step := range AllSteps {
		if step == common.Network {
			// Network has no rounds, so we create a single channel
			// to signal when the step is triggered.
			s.roundChannels[step] = make([]chan struct{}, 2)
			// The first channel is used to signal the start of the network step,
			// and the second channel is used to signal the end of the network step.
			s.roundChannels[step][0] = make(chan struct{})
			s.roundChannels[step][1] = make(chan struct{})
			continue
		}

		s.roundChannels[step] = make([]chan struct{}, s.rounds+1)
		for i := 0; i < s.rounds+1; i++ {
			s.roundChannels[step][i] = make(chan struct{})
		}
	}
	return s, nil
}

func (s *Synchronizer) Start() {
	go s.startTimeSync()
}

func (s *Synchronizer) Stop() {
	s.stopOnce.Do(func() {
		close(s.stopChan)
	})
}

func (s *Synchronizer) Close() error {
	s.Stop()
	return nil
}

func (s *Synchronizer) startTimeSync() {
	startTimes := CalculateStartTimes(s.cfg, s.logger)
	s.logger.Info("Starting time-based synchronization")

	go s.runTimeSyncForStep(common.Network, startTimes.StartBuildingNetwork, s.cfg.Synchronization.BuildingGraphTimeout)
	go s.runTimeSyncForStep(common.ExPostMDAG, startTimes.ExPostMDAG, s.cfg.Synchronization.MDAGRoundTimeout)
	go s.runTimeSyncForStep(common.ExAnteMDAG, startTimes.ExAnteMDAG, s.cfg.Synchronization.MDAGRoundTimeout)
	go s.runTimeSyncForStep(common.ExPostVerify, startTimes.ExPostVerify, s.cfg.Synchronization.ExPostRoundTimeout)
	go s.runTimeSyncForStep(common.ExAnteVerify, startTimes.ExAnteVerify, s.cfg.Synchronization.ExAnteRoundTimeout)

	<-s.stopChan
	s.logger.Info("Stopping time-based synchronization")
}

func (s *Synchronizer) runTimeSyncForStep(step common.Step, startTime time.Time, roundTimeout time.Duration) {
	s.logger.Info("Scheduling rounds for step", zap.String("step", string(step)), zap.Time("startTime", startTime))
	rounds := s.rounds
	if step == common.Network {
		rounds = 1 // Network has only one round
	}

	for i := 0; i < rounds+1; i++ {
		roundStartTime := startTime.Add(time.Duration(i) * roundTimeout).Add(s.clockSkew)
		timer := time.NewTimer(time.Until(roundStartTime))

		select {
		case <-timer.C:
			s.logger.Debug("Time for round, triggering.", zap.String("step", string(step)), zap.Int("round", i))
			s.triggerRound(step, i)
		case <-s.stopChan:
			timer.Stop()
			s.logger.Debug("Stopping round scheduling for step", zap.String("step", string(step)))
			return
		}
	}
	s.logger.Info("Finished scheduling all rounds for step", zap.String("step", string(step)))
}

func (s *Synchronizer) triggerRound(step common.Step, round int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	stepChannels, ok := s.roundChannels[step]
	if !ok {
		s.logger.Warn("Unknown step in sync message", zap.String("step", string(step)))
		return
	}
	if round >= len(stepChannels) {
		s.logger.Warn("Round number exceeds total rounds", zap.Int("round", round), zap.Int("totalRounds", len(stepChannels)))
		return
	}

	ch := stepChannels[round]
	select {
	case <-ch:
		// already closed
	default:
		close(ch)
		s.logger.Debug("Triggered round for step", zap.String("step", string(step)), zap.Int("round", round))
	}
}

func (s *Synchronizer) WaitForRound(step common.Step, round int) (<-chan struct{}, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	stepChannels, ok := s.roundChannels[step]
	if !ok {
		return nil, fmt.Errorf("unknown step: %s", step)
	}

	if round >= len(stepChannels) {
		return nil, fmt.Errorf("round %d exceeds total rounds %d", round, s.rounds)
	}

	return stepChannels[round], nil
}

func CalculateStartTimes(cfg *config.Config, logger *zap.Logger) *StartTimes {
	response, err := ntp.Query(cfg.Synchronization.TimeServer)
	var clockOffset time.Duration
	if err != nil {
		logger.Warn("Failed to query NTP server, using local time", zap.Error(err))
	} else {
		logger.Info("Calculated clock offset", zap.Int64("clockOffset", response.ClockOffset.Milliseconds()))
		clockOffset = response.ClockOffset
	}

	rounds := cfg.Graph.Diameter * cfg.Graph.GradingLevels
	startTime := time.Unix(cfg.Synchronization.StartTime, 0).UTC().Add(clockOffset)

	exPostMDAGTime := startTime.Add(cfg.Synchronization.BuildingGraphTimeout + 1*time.Minute)
	exAnteMDAGTime := exPostMDAGTime.Add(cfg.Synchronization.MDAGRoundTimeout*time.Duration(rounds) + 1*time.Minute)
	exPostVerifyTime := exAnteMDAGTime.Add(
		cfg.Synchronization.MDAGRoundTimeout*time.Duration(rounds) + time.Duration(cfg.Committee.Delay)*time.Second + 1*time.Minute)

	return &StartTimes{
		StartBuildingNetwork: startTime,
		ExPostMDAG:           exPostMDAGTime,
		ExAnteMDAG:           exAnteMDAGTime,
		ExPostVerify:         exPostVerifyTime,
		ExAnteVerify:         exPostVerifyTime,
	}
}
