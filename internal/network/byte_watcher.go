package network

import (
	"strings"

	"go.uber.org/zap"

	"github.com/mamorski/committee-sampling/internal/common"
	"github.com/mamorski/committee-sampling/internal/synchronizer"
)

// appProtocolPrefixes identifies our own protocol traffic vs libp2p internals.
var appProtocolPrefixes = []string{
	"/expost/",
	"/exante/",
	"/mdag/",
	"/graph/",
	"/peer/",
}

func isAppProtocol(pid string) bool {
	for _, prefix := range appProtocolPrefixes {
		if strings.HasPrefix(pid, prefix) {
			return true
		}
	}
	return false
}

// startByteWatcher spawns one goroutine per synchronizer step that has at least
// one fully-bounded round; each goroutine measures the per-protocol byte delta
// accumulated within each round window and logs it. The synchronizer fires N
// ticks at the *start* of rounds 0..N-1, so a step with N ticks has N-1 bounded
// rounds: round r runs in the window [tick(r), tick(r+1)). A step with a single
// tick (GraphDiscovery) has no bounded round and is skipped. A final breakdown
// is logged when the node closes.
func (n *P2PNode) startByteWatcher() {
	if n.bwc == nil {
		return
	}
	for _, step := range synchronizer.AllSteps {
		ticks, err := n.sync.TotalRounds(step)
		if err != nil {
			n.logger.Warn("byte watcher: TotalRounds error",
				zap.String("step", string(step)), zap.Error(err))
			continue
		}
		if ticks <= 1 {
			continue
		}
		n.bwWG.Add(1)
		go n.watchStep(step, ticks)
	}
}

// watchStep attributes traffic to round r by measuring the byte counters at
// tick(r) and tick(r+1): round r's traffic is exactly what accrued between the
// tick that starts it and the tick that starts the next round. The baseline is
// taken at the step's own first tick (not at goroutine spawn) so traffic from
// earlier steps and the idle gaps between steps is not folded into round 0.
func (n *P2PNode) watchStep(step common.Step, ticks int) {
	defer n.bwWG.Done()

	// Wait for the step's first tick, then snapshot the baseline. Anything
	// before this belongs to earlier steps or the inter-step gap.
	if !n.waitForTick(step, 0) {
		return
	}
	prev := n.bytesByProtocol()
	prevApp := n.appBytesByProtocol()

	// Round r spans [tick(r), tick(r+1)); the final tick closes the last round.
	for r := 0; r < ticks-1; r++ {
		if !n.waitForTick(step, r+1) {
			return
		}
		cur := n.bytesByProtocol()
		curApp := n.appBytesByProtocol()
		n.logDelta(step, r, prev, cur, prevApp, curApp)
		prev = cur
		prevApp = curApp
	}
}

// waitForTick blocks until the synchronizer fires the given step/round tick.
// Returns false if the context is cancelled or the tick cannot be obtained.
func (n *P2PNode) waitForTick(step common.Step, round int) bool {
	ch, err := n.sync.WaitForRound(step, round)
	if err != nil {
		n.logger.Warn("byte watcher: WaitForRound error",
			zap.String("step", string(step)), zap.Int("round", round), zap.Error(err))
		return false
	}
	select {
	case <-ch:
		return true
	case <-n.ctx.Done():
		return false
	}
}

func (n *P2PNode) logDelta(step common.Step, round int, prev, cur map[string]ByteStats, prevApp, curApp map[string]AppByteStats) {
	// union of all protocol IDs seen in any snapshot
	seen := make(map[string]struct{}, len(cur))
	for pid := range prev {
		seen[pid] = struct{}{}
	}
	for pid := range cur {
		seen[pid] = struct{}{}
	}
	for pid := range curApp {
		seen[pid] = struct{}{}
	}

	for pid := range seen {
		d := common.RoundByteDelta{
			WireIn:     cur[pid].In - prev[pid].In,
			WireOut:    cur[pid].Out - prev[pid].Out,
			PayloadIn:  curApp[pid].PayloadIn - prevApp[pid].PayloadIn,
			PayloadOut: curApp[pid].PayloadOut - prevApp[pid].PayloadOut,
			EnvIn:      curApp[pid].EnvIn - prevApp[pid].EnvIn,
			EnvOut:     curApp[pid].EnvOut - prevApp[pid].EnvOut,
		}
		if d == (common.RoundByteDelta{}) {
			continue
		}

		n.statsd.RecordBytesPerRound(step, round, pid, d)
	}
}

// recordFinalBytes feeds the end-of-run byte breakdown into the stats daemon
// (called from Close, after the byte watchers have stopped).
func (n *P2PNode) recordFinalBytes() {
	if n.bwc == nil || n.statsd == nil {
		return
	}
	all := n.bytesByProtocol()
	app := n.appBytesByProtocol()
	totalIn, totalOut := n.bytesTotal()

	perProto := make(map[string]common.ProtoByteTotals, len(all))
	var appIn, appOut, overheadIn, overheadOut int64
	var payloadIn, payloadOut, envIn, envOut int64
	for pid, s := range all {
		a := app[pid] // zero value if this protocol carried no app payload
		if isAppProtocol(pid) {
			appIn += s.In
			appOut += s.Out
		} else {
			overheadIn += s.In
			overheadOut += s.Out
		}
		// payload/envelope are zero for overhead protocols (never tracked), so they
		// can be summed unconditionally without the isAppProtocol guard.
		payloadIn += a.PayloadIn
		payloadOut += a.PayloadOut
		envIn += a.EnvIn
		envOut += a.EnvOut
		perProto[pid] = common.ProtoByteTotals{
			WireIn: s.In, WireOut: s.Out,
			PayloadIn: a.PayloadIn, PayloadOut: a.PayloadOut,
			EnvIn: a.EnvIn, EnvOut: a.EnvOut,
		}
	}

	n.statsd.RecordBytesFinal(common.ByteSummary{
		AppIn:       appIn,
		AppOut:      appOut,
		OverheadIn:  overheadIn,
		OverheadOut: overheadOut,
		TotalIn:     totalIn,
		TotalOut:    totalOut,
		PayloadIn:   payloadIn,
		PayloadOut:  payloadOut,
		EnvelopeIn:  envIn,
		EnvelopeOut: envOut,
	}, perProto)
}

