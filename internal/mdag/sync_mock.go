package mdag

import (
	"time"

	"github.com/mamorski/committee-sampling/internal/common"
)

type syncMock struct {
	delay time.Duration
}

func (s syncMock) WaitForRound(_ common.Step, _ int) (<-chan struct{}, error) {
	d := s.delay
	if d == 0 {
		d = 50 * time.Millisecond
	}

	ch := make(chan struct{})
	go func() {
		time.Sleep(d)
		close(ch)
	}()
	return ch, nil
}

func (s syncMock) TotalRounds(_ common.Step) (int, error) {
	return 1, nil
}
