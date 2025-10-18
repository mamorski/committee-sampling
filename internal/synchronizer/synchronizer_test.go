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
			Diameter:       2,
			GradingLevels:  5,
			BuildingRounds: 3,
		},
		Network: config.Network{
			ListenPort: 9000,
		},
		Synchronization: config.Synchronization{
			Type:                      config.TimeSync,
			StartTime:                 time.Now().Add(100 * time.Millisecond).Unix(),
			TimeServer:                "pool.ntp.org",
			GraphDiscoveryTimeout:     time.Second,
			GraphBuildingRoundTimeout: time.Second,
			MDAGRoundTimeout:          time.Second,
			ExPostRoundTimeout:        time.Second,
			ExAnteRoundTimeout:        time.Second,
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
		var numChannels int
		switch step {
		case common.GraphDiscovery:
			numChannels = 1
		case common.Network:
			numChannels = suite.cfg.Graph.BuildingRounds + 1
		default:
			numChannels = s.rounds + 1
		}

		s.roundChannels[step] = make([]chan struct{}, numChannels)
		for i := 0; i < numChannels; i++ {
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
	suite.Len(AllSteps, 6, "There should be 6 steps")

	for _, step := range AllSteps {
		var maxRound int
		switch step {
		case common.GraphDiscovery:
			maxRound = 0
		case common.Network:
			maxRound = suite.cfg.Graph.BuildingRounds
		default:
			maxRound = rounds
		}

		for i := 0; i <= maxRound; i++ {
			_, err := suite.s.WaitForRound(step, i)
			suite.NoErrorf(err, "Channel for step %s round %d should be initialized", step, i)
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
	suite.EqualError(
		err, fmt.Sprintf("round %d exceeds total rounds %d for step %s", totalRounds+1, totalRounds, common.ExPostMDAG),
	)

	// Error: discovery only has round 0
	_, err = s.WaitForRound(common.GraphDiscovery, 1)
	suite.Error(err)
	suite.EqualError(err, "round 1 exceeds total rounds 0 for step GraphDiscovery")
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
