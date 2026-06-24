package stats

import (
	"time"

	"go.uber.org/zap"

	"github.com/mamorski/committee-sampling/internal/common"
)

// watch follows one step's synchronizer ticks and reports each as it fires, so
// the consumer can track the step's current round and the wall-clock start of
// each round. Mirrors the byte watcher's tick-following pattern. ticks is the
// number of scheduled ticks (rounds 0..ticks-1).
func (d *Daemon) watch(step common.Step, ticks int) {
	defer d.wg.Done()
	for r := 0; r < ticks; r++ {
		ch, err := d.sync.WaitForRound(step, r)
		if err != nil {
			d.logger.Warn("stats: WaitForRound failed",
				zap.String("step", string(step)), zap.Int("round", r), zap.Error(err))
			return
		}
		select {
		case <-ch:
			d.recordTick(step, r, time.Now())
		case <-d.ctx.Done():
			return
		}
	}
}
