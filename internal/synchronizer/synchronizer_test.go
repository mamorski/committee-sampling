package synchronizer

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"testing"
	"time"

	"github.com/mamorski/committee-sampling/internal/common"
	"github.com/mamorski/committee-sampling/pkg/config"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
)

// MockPubSub is a mock for the PubSub interface
type MockPubSub struct {
	mock.Mock
}

func (m *MockPubSub) Subscribe(topic string) (<-chan []byte, error) {
	args := m.Called(topic)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(<-chan []byte), args.Error(1)
}

func (m *MockPubSub) VerifySignature(pubKey, message, signature []byte) (bool, error) {
	args := m.Called(pubKey, message, signature)
	return args.Bool(0), args.Error(1)
}

type SynchronizerTestSuite struct {
	suite.Suite
	pubSub    *MockPubSub
	cfg       *config.Config
	logger    *zap.Logger
	s         *Synchronizer
	certPath  string
	publicKey ed25519.PublicKey
}

func (suite *SynchronizerTestSuite) SetupTest() {
	suite.pubSub = new(MockPubSub)
	suite.logger = zap.NewNop()

	// Create a temporary certificate file for testing
	suite.certPath = suite.createTestCertFile()

	// Extract the public key for use in mock expectations
	suite.publicKey, _ = readPublicKeyFromCert(suite.certPath)

	suite.cfg = &config.Config{
		Graph: config.Graph{
			Diameter:      2,
			GradingLevels: 5, // results in 10 rounds
		},
		Synchronization: config.Synchronization{
			Type:               config.TimeSync,
			MDAGRoundTimeout:   10 * time.Millisecond,
			ExPostRoundTimeout: 10 * time.Millisecond,
			StartTime:          time.Now().Add(100 * time.Millisecond).Unix(),
			TimeServer:         "pool.ntp.org",
			Topic:              "sync-topic",
			CertificatePath:    suite.certPath,
		},
	}
	var err error
	suite.s, err = New(suite.pubSub, suite.cfg, suite.logger)
	suite.Require().NoError(err)
}

func (suite *SynchronizerTestSuite) TearDownTest() {
	if suite.certPath != "" {
		_ = os.Remove(suite.certPath)
	}
}

func (suite *SynchronizerTestSuite) createTestCertFile() string {
	// Generate Ed25519 key pair
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	suite.Require().NoError(err)

	// Create certificate template
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: "test-certificate",
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}

	// Create certificate
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, publicKey, privateKey)
	suite.Require().NoError(err)

	// Encode certificate to PEM
	certPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: certDER,
	})

	// Write to temporary file
	tmpFile, err := os.CreateTemp("", "test_cert_*.pem")
	suite.Require().NoError(err)

	_, err = tmpFile.Write(certPEM)
	suite.Require().NoError(err)

	err = tmpFile.Close()
	suite.Require().NoError(err)

	return tmpFile.Name()
}

func TestSynchronizerTestSuite(t *testing.T) {
	suite.Run(t, new(SynchronizerTestSuite))
}

func (suite *SynchronizerTestSuite) TestNewSynchronizer() {
	suite.NotNil(suite.s)
	rounds := suite.cfg.Graph.Diameter * suite.cfg.Graph.GradingLevels
	suite.Len(AllSteps, 4, "There should be 4 steps")

	for _, step := range AllSteps {
		for i := 0; i < rounds; i++ {
			_, err := suite.s.WaitForRound(step, i)
			suite.NoErrorf(err, "Channel for step %s round %d should be initialized", step, i)
		}
	}
}

func (suite *SynchronizerTestSuite) TestWaitForRound() {
	// Happy path
	ch, err := suite.s.WaitForRound(common.ExPostMDAG, 5)
	suite.NoError(err)
	suite.NotNil(ch)

	// Error: unknown step
	_, err = suite.s.WaitForRound("UnknownStep", 5)
	suite.Error(err)
	suite.EqualError(err, "unknown step: UnknownStep")

	// Error: round out of bounds
	totalRounds := suite.cfg.Graph.Diameter * suite.cfg.Graph.GradingLevels
	_, err = suite.s.WaitForRound(common.ExPostMDAG, totalRounds)
	suite.Error(err)
	suite.EqualError(err, fmt.Sprintf("round %d exceeds total rounds %d", totalRounds, totalRounds))
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

func (suite *SynchronizerTestSuite) TestTimeSyncMode() {
	suite.cfg.Synchronization.Type = config.TimeSync
	suite.cfg.Synchronization.StartTime = time.Now().Add(50 * time.Millisecond).Unix()
	suite.cfg.Synchronization.MDAGRoundTimeout = 20 * time.Millisecond

	var err error
	suite.s, err = New(suite.pubSub, suite.cfg, suite.logger)
	suite.NoError(err)

	roundToTest := 2
	waitChan, err := suite.s.WaitForRound(common.ExPostMDAG, roundToTest)
	suite.NoError(err)

	suite.s.Start()
	defer suite.s.Stop()

	select {
	case <-waitChan:
		// success
	case <-time.After(500 * time.Millisecond):
		suite.Fail("timed out waiting for time-based sync round to trigger")
	}
}

func (suite *SynchronizerTestSuite) TestChannelSyncMode_HappyPath() {
	suite.cfg.Synchronization.Type = config.ChannelSync
	var err error
	suite.s, err = New(suite.pubSub, suite.cfg, suite.logger)
	suite.NoError(err)

	msgChan := make(chan []byte, 1)
	suite.pubSub.On("Subscribe", suite.cfg.Synchronization.Topic).Return((<-chan []byte)(msgChan), nil)

	step := common.ExPostVerify
	round := 7
	syncMsg := SyncMessage{
		Step:      step,
		Round:     round,
		Signature: []byte("valid-sig"),
	}
	msgBytes, _ := json.Marshal(syncMsg)
	dataToVerify := []byte(fmt.Sprintf("%s:%d", syncMsg.Step, syncMsg.Round))

	suite.pubSub.On("VerifySignature", []byte(suite.publicKey), dataToVerify, syncMsg.Signature).Return(true, nil)

	waitChan, err := suite.s.WaitForRound(step, round)
	suite.NoError(err)

	suite.s.Start()
	defer suite.s.Stop()

	msgChan <- msgBytes

	select {
	case <-waitChan:
		// success
	case <-time.After(100 * time.Millisecond):
		suite.Fail("timed out waiting for channel-based sync round to trigger")
	}

	suite.pubSub.AssertExpectations(suite.T())
}

func (suite *SynchronizerTestSuite) TestChannelSyncMode_MalformedJSON() {
	suite.cfg.Synchronization.Type = config.ChannelSync
	var err error
	suite.s, err = New(suite.pubSub, suite.cfg, suite.logger)
	suite.NoError(err)

	msgChan := make(chan []byte, 1)
	suite.pubSub.On("Subscribe", suite.cfg.Synchronization.Topic).Return((<-chan []byte)(msgChan), nil)

	suite.s.Start()
	defer suite.s.Stop()

	waitChan, err := suite.s.WaitForRound(common.ExPostMDAG, 0)
	suite.NoError(err)

	// Malformed JSON
	msgChan <- []byte("this is not json")
	select {
	case <-waitChan:
		suite.Fail("Round should not have been triggered for malformed JSON")
	case <-time.After(20 * time.Millisecond):
		// Expected
	}

	suite.pubSub.AssertExpectations(suite.T())
}

func (suite *SynchronizerTestSuite) TestChannelSyncMode_SignatureVerificationError() {
	suite.cfg.Synchronization.Type = config.ChannelSync
	var err error
	suite.s, err = New(suite.pubSub, suite.cfg, suite.logger)
	suite.NoError(err)

	msgChan := make(chan []byte, 1)
	suite.pubSub.On("Subscribe", suite.cfg.Synchronization.Topic).Return((<-chan []byte)(msgChan), nil)

	suite.s.Start()
	defer suite.s.Stop()

	waitChan, err := suite.s.WaitForRound(common.ExPostMDAG, 0)
	suite.NoError(err)

	// Signature verification error
	syncMsgVerifyError := SyncMessage{Step: common.ExPostMDAG, Round: 0, Signature: []byte("error-sig")}
	msgBytesVerifyError, _ := json.Marshal(syncMsgVerifyError)
	dataToVerifyError := []byte(fmt.Sprintf("%s:%d", syncMsgVerifyError.Step, syncMsgVerifyError.Round))
	suite.pubSub.On("VerifySignature", []byte(suite.publicKey), dataToVerifyError, syncMsgVerifyError.Signature).Return(false, fmt.Errorf("signature verification failed")).Once() //nolint:lll

	msgChan <- msgBytesVerifyError
	select {
	case <-waitChan:
		suite.Fail("Round should not have been triggered for signature verification error")
	case <-time.After(20 * time.Millisecond):
		// Expected
	}

	suite.pubSub.AssertExpectations(suite.T())
}

func (suite *SynchronizerTestSuite) TestChannelSyncMode_InvalidSignature() {
	suite.cfg.Synchronization.Type = config.ChannelSync
	var err error
	suite.s, err = New(suite.pubSub, suite.cfg, suite.logger)
	suite.NoError(err)

	msgChan := make(chan []byte, 1)
	suite.pubSub.On("Subscribe", suite.cfg.Synchronization.Topic).Return((<-chan []byte)(msgChan), nil)

	suite.s.Start()
	defer suite.s.Stop()

	waitChan, err := suite.s.WaitForRound(common.ExPostMDAG, 0)
	suite.NoError(err)

	// Invalid signature
	syncMsgInvalidSig := SyncMessage{Step: common.ExPostMDAG, Round: 0, Signature: []byte("invalid-sig")}
	msgBytesInvalidSig, _ := json.Marshal(syncMsgInvalidSig)
	dataToVerifyInvalidSig := []byte(fmt.Sprintf("%s:%d", syncMsgInvalidSig.Step, syncMsgInvalidSig.Round))
	suite.pubSub.On("VerifySignature", []byte(suite.publicKey), dataToVerifyInvalidSig, syncMsgInvalidSig.Signature).Return(false, nil).Once()

	msgChan <- msgBytesInvalidSig
	select {
	case <-waitChan:
		suite.Fail("Round should not have been triggered for invalid signature")
	case <-time.After(20 * time.Millisecond):
		// Expected
	}

	suite.pubSub.AssertExpectations(suite.T())
}

func (suite *SynchronizerTestSuite) TestChannelSyncMode_RoundOutOfBounds() {
	suite.cfg.Synchronization.Type = config.ChannelSync
	var err error
	suite.s, err = New(suite.pubSub, suite.cfg, suite.logger)
	suite.NoError(err)

	msgChan := make(chan []byte, 1)
	suite.pubSub.On("Subscribe", suite.cfg.Synchronization.Topic).Return((<-chan []byte)(msgChan), nil)

	suite.s.Start()
	defer suite.s.Stop()

	waitChan, err := suite.s.WaitForRound(common.ExPostMDAG, 0)
	suite.NoError(err)

	// Round out of bounds
	syncMsgOOB := SyncMessage{Step: common.ExPostMDAG, Round: 99, Signature: []byte("valid-sig")}
	msgBytesOOB, _ := json.Marshal(syncMsgOOB)
	dataToVerifyOOB := []byte(fmt.Sprintf("%s:%d", syncMsgOOB.Step, syncMsgOOB.Round))
	suite.pubSub.On("VerifySignature", []byte(suite.publicKey), dataToVerifyOOB, syncMsgOOB.Signature).Return(true, nil).Once()

	msgChan <- msgBytesOOB
	select {
	case <-waitChan:
		suite.Fail("Round should not have been triggered for out-of-bounds round")
	case <-time.After(20 * time.Millisecond):
		// Expected
	}

	suite.pubSub.AssertExpectations(suite.T())
}

func (suite *SynchronizerTestSuite) TestChannelSyncMode_UnknownStep() {
	suite.cfg.Synchronization.Type = config.ChannelSync
	var err error
	suite.s, err = New(suite.pubSub, suite.cfg, suite.logger)
	suite.NoError(err)

	msgChan := make(chan []byte, 1)
	suite.pubSub.On("Subscribe", suite.cfg.Synchronization.Topic).Return((<-chan []byte)(msgChan), nil)

	suite.s.Start()
	defer suite.s.Stop()

	waitChan, err := suite.s.WaitForRound(common.ExPostMDAG, 0)
	suite.NoError(err)

	// Unknown step
	syncMsgUnknownStep := SyncMessage{Step: "UnknownStep", Round: 0, Signature: []byte("valid-sig")}
	msgBytesUnknownStep, _ := json.Marshal(syncMsgUnknownStep)
	dataToVerifyUnknownStep := []byte(fmt.Sprintf("%s:%d", syncMsgUnknownStep.Step, syncMsgUnknownStep.Round))
	suite.pubSub.On("VerifySignature", []byte(suite.publicKey), dataToVerifyUnknownStep, syncMsgUnknownStep.Signature).Return(true, nil).Once()

	msgChan <- msgBytesUnknownStep
	select {
	case <-waitChan:
		suite.Fail("Round should not have been triggered for unknown step")
	case <-time.After(20 * time.Millisecond):
		// Expected
	}

	suite.pubSub.AssertExpectations(suite.T())
}

func (suite *SynchronizerTestSuite) TestChannelSyncMode_SubscribeError() {
	suite.cfg.Synchronization.Type = config.ChannelSync
	var err error
	suite.s, err = New(suite.pubSub, suite.cfg, suite.logger)
	suite.NoError(err)

	// Mock Subscribe to return an error
	suite.pubSub.On("Subscribe", suite.cfg.Synchronization.Topic).Return((<-chan []byte)(nil), fmt.Errorf("failed to subscribe to topic"))

	suite.s.Start()
	defer suite.s.Stop()

	// Wait a bit to ensure the error handling completes
	time.Sleep(50 * time.Millisecond)

	suite.pubSub.AssertExpectations(suite.T())
}

func (suite *SynchronizerTestSuite) TestNewSynchronizer_CertificateError() {
	cfg := &config.Config{
		Graph: config.Graph{
			Diameter:      5,
			GradingLevels: 2,
		},
		Synchronization: config.Synchronization{
			Type:            config.ChannelSync,
			CertificatePath: "/nonexistent/path/to/cert.pem", // Invalid path
			Topic:           "test-topic",
		},
	}

	// This should fail because the certificate file doesn't exist
	_, err := New(suite.pubSub, cfg, suite.logger)
	suite.Error(err)
	suite.Contains(err.Error(), "failed to read public key from certificate")
}

func (suite *SynchronizerTestSuite) TestNewSynchronizer_InvalidCertificateFormat() {
	// Create a file with invalid certificate content
	tmpFile, err := os.CreateTemp("", "invalid_cert_*.pem")
	suite.Require().NoError(err)
	defer func(name string) {
		_ = os.Remove(name)
	}(tmpFile.Name())

	// Write invalid PEM data
	_, err = tmpFile.WriteString("invalid certificate data")
	suite.Require().NoError(err)
	err = tmpFile.Close()
	suite.Require().NoError(err)

	cfg := &config.Config{
		Graph: config.Graph{
			Diameter:      5,
			GradingLevels: 2,
		},
		Synchronization: config.Synchronization{
			Type:            config.ChannelSync,
			CertificatePath: tmpFile.Name(),
			Topic:           "test-topic",
		},
	}

	// This should fail because the certificate format is invalid
	_, err = New(suite.pubSub, cfg, suite.logger)
	suite.Error(err)
	suite.Contains(err.Error(), "failed to read public key from certificate")
}
