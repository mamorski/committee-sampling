package synchronizer

import (
	"crypto/ed25519"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/beevik/ntp"
	"github.com/mamorski/committee-sampling/internal/common"
	"github.com/mamorski/committee-sampling/pkg/config"
	"go.uber.org/zap"
)

var AllSteps = []common.Step{common.ExPostMDAG, common.ExAnteMDAG, common.ExPostVerify, common.ExAnteVerify}

type PubSub interface {
	Subscribe(topic string) (<-chan []byte, error)
	VerifySignature(pubKey, message, signature []byte) (bool, error)
}

type SyncMessage struct {
	Step      common.Step `json:"step"`
	Round     int         `json:"round"`
	Signature []byte      `json:"signature"`
}

type StartTimes struct {
	ExPostMDAG   time.Time
	ExAnteMDAG   time.Time
	ExPostVerify time.Time
	ExAnteVerify time.Time
}

type Synchronizer struct {
	pubSub        PubSub
	cfg           *config.Config
	logger        *zap.Logger
	stopChan      chan struct{}
	roundChannels map[common.Step][]chan struct{}
	rounds        int
	mu            sync.Mutex
	publicKey     ed25519.PublicKey
}

func New(pubSub PubSub, cfg *config.Config, logger *zap.Logger) (*Synchronizer, error) {
	s := &Synchronizer{
		pubSub:        pubSub,
		cfg:           cfg,
		logger:        logger,
		stopChan:      make(chan struct{}),
		roundChannels: make(map[common.Step][]chan struct{}),
	}

	// Only read public key from certificate file if using ChannelSync
	if cfg.Synchronization.Type == config.ChannelSync {
		pubKey, err := readPublicKeyFromCert(cfg.Synchronization.CertificatePath)
		if err != nil {
			return nil, fmt.Errorf("failed to read public key from certificate: %w", err)
		}
		s.publicKey = pubKey
	}

	s.rounds = cfg.Graph.Diameter * cfg.Graph.GradingLevels
	for _, step := range AllSteps {
		s.roundChannels[step] = make([]chan struct{}, s.rounds)
		for i := 0; i < s.rounds; i++ {
			s.roundChannels[step][i] = make(chan struct{})
		}
	}
	return s, nil
}

func readPublicKeyFromCert(certPath string) (ed25519.PublicKey, error) {
	certData, err := os.ReadFile(certPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read certificate file: %w", err)
	}

	block, _ := pem.Decode(certData)
	if block == nil {
		return nil, fmt.Errorf("failed to decode PEM block from certificate")
	}

	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse certificate: %w", err)
	}

	pubKey, ok := cert.PublicKey.(ed25519.PublicKey)
	if !ok {
		return nil, fmt.Errorf("certificate does not contain Ed25519 public key")
	}

	return pubKey, nil
}

func (s *Synchronizer) Start() {
	if s.cfg.Synchronization.Type == config.TimeSync {
		go s.startTimeSync()
	} else {
		go s.startChannelSync()
	}
}

func (s *Synchronizer) Stop() {
	close(s.stopChan)
}

func (s *Synchronizer) startChannelSync() {
	msgChan, err := s.pubSub.Subscribe(s.cfg.Synchronization.Topic)
	if err != nil {
		s.logger.Error("Failed to subscribe to sync topic", zap.Error(err))
		return
	}

	s.logger.Info("Starting channel-based synchronization")
	for {
		select {
		case msgBytes := <-msgChan:
			var msg SyncMessage
			if err := json.Unmarshal(msgBytes, &msg); err != nil {
				s.logger.Warn("Failed to unmarshal sync message", zap.Error(err))
				continue
			}

			dataToVerify := []byte(fmt.Sprintf("%s:%d", msg.Step, msg.Round))
			valid, err := s.pubSub.VerifySignature(s.publicKey, dataToVerify, msg.Signature)
			if err != nil {
				s.logger.Warn("Error verifying signature", zap.Error(err))
				continue
			}

			if !valid {
				s.logger.Warn("Invalid signature for sync message")
				continue
			}

			s.logger.Info("Received and verified sync message", zap.String("step", string(msg.Step)), zap.Int("round", msg.Round))
			s.triggerRound(msg.Step, msg.Round)
		case <-s.stopChan:
			s.logger.Info("Stopping channel-based synchronization")
			return
		}
	}
}

func (s *Synchronizer) startTimeSync() {
	startTimes := CalculateStartTimes(s.cfg, s.logger)
	s.logger.Info("Starting time-based synchronization")

	go s.runTimeSyncForStep(common.ExPostMDAG, startTimes.ExPostMDAG, s.cfg.Synchronization.MDAGRoundTimeout)
	go s.runTimeSyncForStep(common.ExAnteMDAG, startTimes.ExAnteMDAG, s.cfg.Synchronization.MDAGRoundTimeout)
	go s.runTimeSyncForStep(common.ExPostVerify, startTimes.ExPostVerify, s.cfg.Synchronization.ExPostRoundTimeout)
	go s.runTimeSyncForStep(common.ExAnteVerify, startTimes.ExAnteVerify, s.cfg.Synchronization.ExAnteRoundTimeout)

	<-s.stopChan
	s.logger.Info("Stopping time-based synchronization")
}

func (s *Synchronizer) runTimeSyncForStep(step common.Step, startTime time.Time, roundTimeout time.Duration) {
	s.logger.Info("Scheduling rounds for step", zap.String("step", string(step)), zap.Time("startTime", startTime))
	for i := 0; i < s.rounds; i++ {
		roundStartTime := startTime.Add(time.Duration(i) * roundTimeout)
		timer := time.NewTimer(time.Until(roundStartTime))

		select {
		case <-timer.C:
			s.logger.Info("Time for round, triggering.", zap.String("step", string(step)), zap.Int("round", i))
			s.triggerRound(step, i)
		case <-s.stopChan:
			timer.Stop()
			s.logger.Info("Stopping round scheduling for step", zap.String("step", string(step)))
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
		s.logger.Info("Triggered round for step", zap.String("step", string(step)), zap.Int("round", round))
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
		return nil, fmt.Errorf("round %d exceeds total rounds %d", round, len(stepChannels))
	}

	return stepChannels[round], nil
}

func CalculateStartTimes(cfg *config.Config, logger *zap.Logger) *StartTimes {
	response, err := ntp.Query(cfg.Synchronization.TimeServer)
	var clockOffset time.Duration
	if err != nil {
		logger.Warn("Failed to query NTP server, using local time", zap.Error(err))
	} else {
		logger.Info("Calculated start times", zap.Int64("clockOffset", response.ClockOffset.Milliseconds()))
		clockOffset = response.ClockOffset
	}

	rounds := cfg.Graph.Diameter * cfg.Graph.GradingLevels
	startTime := time.Unix(cfg.Synchronization.StartTime, 0).UTC().Add(clockOffset)

	return &StartTimes{
		ExPostMDAG:   startTime,
		ExAnteMDAG:   startTime.Add(cfg.Synchronization.MDAGRoundTimeout*time.Duration(rounds) + time.Minute),
		ExPostVerify: startTime.Add(cfg.Synchronization.MDAGRoundTimeout*time.Duration(rounds) + (time.Second * 120)),
		ExAnteVerify: startTime.Add(cfg.Synchronization.ExPostRoundTimeout*time.Duration(rounds) + (time.Second * 10)),
	}
}
