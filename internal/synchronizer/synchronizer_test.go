package synchronizer

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/mamorski/committee-sampling/internal/common"
	"github.com/mamorski/committee-sampling/pkg/config"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
)

type SynchronizerTestSuite struct {
	suite.Suite
	cfg    *config.Config
	logger *zap.Logger
	s      *Synchronizer
}

func (suite *SynchronizerTestSuite) SetupTest() {
	suite.logger = zap.NewNop()

	suite.cfg = &config.Config{
		Graph: config.Graph{
			Diameter:      2,
			GradingLevels: 5,
		},
		Network: config.Network{
			ListenPort: 9000,
		},
		Synchronization: config.Synchronization{
			Type:                 config.TimeSync,
			StartTime:            time.Now().Add(100 * time.Millisecond).Unix(),
			TimeServer:           "pool.ntp.org",
			BuildingGraphTimeout: time.Second,
			MDAGRoundTimeout:     time.Second,
			ExPostRoundTimeout:   time.Second,
			ExAnteRoundTimeout:   time.Second,
		},
	}
	suite.s = suite.createSynchronizerWithMock()
}

// createSynchronizerWithMock creates a synchronizer instance for testing
func (suite *SynchronizerTestSuite) createSynchronizerWithMock() *Synchronizer {
	s := &Synchronizer{
		cfg:           suite.cfg,
		logger:        suite.logger,
		stopChan:      make(chan struct{}),
		roundChannels: make(map[common.Step][]chan struct{}),
	}

	s.rounds = suite.cfg.Graph.Diameter * suite.cfg.Graph.GradingLevels
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
	return s
}

func (suite *SynchronizerTestSuite) TearDownTest() {
	// Nothing to clean up
}

func TestSynchronizerTestSuite(t *testing.T) {
	suite.Run(t, new(SynchronizerTestSuite))
}

func (suite *SynchronizerTestSuite) TestNewSynchronizer() {
	suite.NotNil(suite.s)
	rounds := suite.cfg.Graph.Diameter * suite.cfg.Graph.GradingLevels
	suite.Len(AllSteps, 5, "There should be 5 steps")

	for _, step := range AllSteps {
		if step == common.Network {
			// Network has only 2 channels: start (0) and end (1)
			for i := 0; i < 2; i++ {
				_, err := suite.s.WaitForRound(step, i)
				suite.NoErrorf(err, "Channel for step %s round %d should be initialized", step, i)
			}
		} else {
			// Other steps have rounds+1 channels
			for i := 0; i <= rounds; i++ {
				_, err := suite.s.WaitForRound(step, i)
				suite.NoErrorf(err, "Channel for step %s round %d should be initialized", step, i)
			}
		}
	}
}

func (suite *SynchronizerTestSuite) TestWaitForRound() {
	s := suite.createSynchronizerWithMock()
	defer s.Stop()

	// Happy path
	ch, err := s.WaitForRound(common.ExPostMDAG, 5)
	suite.NoError(err)
	suite.NotNil(ch)

	// Error: unknown step
	_, err = s.WaitForRound("UnknownStep", 5)
	suite.Error(err)
	suite.EqualError(err, "unknown step: UnknownStep")

	// Error: round out of bounds
	totalRounds := suite.cfg.Graph.Diameter * suite.cfg.Graph.GradingLevels
	_, err = s.WaitForRound(common.ExPostMDAG, totalRounds+1)
	suite.Error(err)
	suite.EqualError(err, fmt.Sprintf("round %d exceeds total rounds %d", totalRounds+1, totalRounds))
}

func (suite *SynchronizerTestSuite) TestTriggerRound() {
	round := 3
	step := common.ExAnteMDAG
	ch, err := suite.s.WaitForRound(step, round)
	suite.NoError(err)

	suite.s.triggerRound(step, round)

	select {
	case <-ch:
		// success
	case <-time.After(10 * time.Millisecond):
		suite.Fail("channel was not closed")
	}

	// Test that triggering a second time does not panic
	suite.NotPanics(func() {
		suite.s.triggerRound(step, round)
	})
}

// Test production New function with actual certificate scenarios
func (suite *SynchronizerTestSuite) TestNew_TimeSync() {
	ctx := context.Background()
	s, err := New(ctx, suite.cfg, suite.logger)
	suite.Require().NoError(err)
	defer s.Stop()
	suite.NotNil(s)
}
