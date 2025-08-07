package expost

import (
	"time"

	"github.com/mamorski/committee-sampling/internal/common"
)

type delayedSync struct{ delay time.Duration }

func newDelayedSync(d time.Duration) delayedSync {
	return delayedSync{delay: d}
}

func (s delayedSync) WaitForRound(_ common.Step, _ int) (<-chan struct{}, error) {
	ch := make(chan struct{})
	go func() {
		time.Sleep(s.delay)
		close(ch)
	}()
	return ch, nil
}
