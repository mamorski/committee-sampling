// Package stats implements a per-node statistics daemon. A single goroutine
// owns all aggregation state and consumes events from the protocols, the
// network layer, and the bootstrap; at shutdown it writes one JSON report per
// node. Centralizing here lets the protocols drop their own per-round counter
// arrays and removes the need for the analysis notebook to scan the full log.
package stats

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/mamorski/committee-sampling/internal/common"
	"github.com/mamorski/committee-sampling/internal/metrics"
	"github.com/mamorski/committee-sampling/pkg/config"
)

// stepsToWatch are the message-bearing steps whose current round the daemon
// tracks (via synchronizer ticks) so it can flag late messages and measure
// arrival lag relative to each round's start.
var stepsToWatch = []common.Step{
	common.ExPostMDAG,
	common.ExAnteMDAG,
	common.ExPostVerify,
	common.ExAnteVerify,
}

// roundAgg accumulates per-round counters and arrival-lag samples (ns).
type roundAgg struct {
	total, valid                  int
	lagSumNs, lagMaxNs, lagLastNs int64
	lagCount                      int
}

type protoAgg struct {
	step         common.Step
	rounds       map[int]*roundAgg
	lateMessages int
	maxLateness  int
	lateByRound  map[int]int
}

// Daemon collects all per-node statistics and writes a JSON
// report at shutdown. It implements common.StatsRecorder.
type Daemon struct {
	nodeID string
	sid    string
	outDir string
	logger *zap.Logger
	sync   common.Synchronizer

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu        sync.Mutex
	protocols map[string]*protoAgg
	curRound  map[common.Step]int
	tickTimes map[common.Step]map[int]time.Time
	bytes     ByteReport
	neighbors []string
	committee []CommitteeMember
	peerDrops []PeerDropReport
}

var _ common.StatsRecorder = (*Daemon)(nil)

// New constructs the daemon. The returned daemon is inert until Start is called.
func New(cfg *config.Config, logger *zap.Logger, synchronizer common.Synchronizer, nodeID, sid string) *Daemon {
	ctx, cancel := context.WithCancel(context.Background())
	outDir := cfg.Stats.OutputDir
	if outDir == "" {
		outDir = "."
	}
	d := &Daemon{
		nodeID:    nodeID,
		sid:       sid,
		outDir:    outDir,
		logger:    logger.Named("stats"),
		sync:      synchronizer,
		ctx:       ctx,
		cancel:    cancel,
		protocols: make(map[string]*protoAgg),
		curRound:  make(map[common.Step]int),
		tickTimes: make(map[common.Step]map[int]time.Time),
		bytes:     ByteReport{ByProtocol: make(map[string]ByteCounts)},
	}
	for _, s := range stepsToWatch {
		d.curRound[s] = -1
		d.tickTimes[s] = make(map[int]time.Time)
	}
	return d
}

// Start launches one tick-watcher per watched step.
func (d *Daemon) Start() {
	for _, step := range stepsToWatch {
		ticks, err := d.sync.TotalRounds(step)
		if err != nil || ticks <= 0 {
			continue
		}
		d.wg.Add(1)
		go d.watch(step, ticks)
	}
}

// Close stops the watchers, writes the report, and
// waits for every goroutine to exit. Safe to call once.
func (d *Daemon) Close() error {
	d.cancel()
	d.wg.Wait()
	d.mu.Lock()
	defer d.mu.Unlock()
	d.writeReport()
	return nil
}

func (d *Daemon) recordTick(step common.Step, round int, arrival time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.curRound[step] = round
	if d.tickTimes[step] == nil {
		d.tickTimes[step] = make(map[int]time.Time)
	}
	d.tickTimes[step][round] = arrival
}

func (d *Daemon) RecordReceived(proto string, step common.Step, round int, arrival time.Time) {
	d.mu.Lock()
	p := d.proto(proto, step)
	ra := p.round(round)
	ra.total++

	// Late message: stamped round is below the step's current round.
	if cur, ok := d.curRound[step]; ok && cur >= 0 && round < cur {
		late := cur - round
		p.lateMessages++
		p.lateByRound[round]++
		if late > p.maxLateness {
			p.maxLateness = late
		}
	}

	// Arrival lag: time from the round's start tick to message arrival.
	if tt, ok := d.tickTimes[step][round]; ok && !tt.IsZero() {
		lag := arrival.Sub(tt).Nanoseconds()
		if lag < 0 {
			lag = 0
		}
		ra.lagSumNs += lag
		ra.lagCount++
		ra.lagLastNs = lag
		if lag > ra.lagMaxNs {
			ra.lagMaxNs = lag
		}
	}
	d.mu.Unlock()

	metrics.TotalMessages.WithLabelValues(strconv.Itoa(round), proto, d.nodeID, d.sid).Inc()
}

func (d *Daemon) RecordValid(proto string, step common.Step, round int) {
	d.mu.Lock()
	d.proto(proto, step).round(round).valid++
	d.mu.Unlock()

	metrics.ValidMessages.WithLabelValues(strconv.Itoa(round), proto, d.nodeID, d.sid).Inc()
}

func (d *Daemon) RecordBytesPerRound(step common.Step, round int, protocolID string, delta common.RoundByteDelta) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.bytes.PerRound = append(d.bytes.PerRound, ByteRound{
		Step: string(step), Round: round, ProtocolID: protocolID,
		InDelta: delta.WireIn, OutDelta: delta.WireOut,
		PayloadInDelta: delta.PayloadIn, PayloadOutDelta: delta.PayloadOut,
		EnvelopeInDelta: delta.EnvIn, EnvelopeOutDelta: delta.EnvOut,
	})
}

func (d *Daemon) RecordBytesFinal(summary common.ByteSummary, perProtocol map[string]common.ProtoByteTotals) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.bytes.Summary = summary
	for pid, c := range perProtocol {
		d.bytes.ByProtocol[pid] = ByteCounts{
			In: c.WireIn, Out: c.WireOut,
			PayloadIn: c.PayloadIn, PayloadOut: c.PayloadOut,
			EnvelopeIn: c.EnvIn, EnvelopeOut: c.EnvOut,
		}
	}
}

func (d *Daemon) RecordNeighbors(neighbors []string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.neighbors = append([]string(nil), neighbors...)
}

func (d *Daemon) RecordCommittee(members []*common.CommitteeOutput) {
	d.mu.Lock()
	defer d.mu.Unlock()
	cm := make([]CommitteeMember, 0, len(members))
	for _, m := range members {
		if m != nil {
			cm = append(cm, CommitteeMember{ID: m.ID, Grade: m.Grade})
		}
	}
	d.committee = cm
}

func (d *Daemon) RecordPeerDrop(peerID, phase string, round int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.peerDrops = append(d.peerDrops, PeerDropReport{PeerID: peerID, Phase: phase, Round: round})
}

func (d *Daemon) proto(name string, step common.Step) *protoAgg {
	p, ok := d.protocols[name]
	if !ok {
		p = &protoAgg{step: step, rounds: make(map[int]*roundAgg), lateByRound: make(map[int]int)}
		d.protocols[name] = p
	}
	return p
}

func (p *protoAgg) round(r int) *roundAgg {
	ra, ok := p.rounds[r]
	if !ok {
		ra = &roundAgg{}
		p.rounds[r] = ra
	}
	return ra
}

// buildReport snapshots the aggregation state into the serializable Report.
func (d *Daemon) buildReport() Report {
	rep := Report{
		NodeID:      d.nodeID,
		SID:         d.sid,
		GeneratedAt: time.Now(),
		Protocols:   make(map[string]*ProtocolReport, len(d.protocols)),
		Bytes:       d.bytes,
		Neighbors:   d.neighbors,
		Committee:   d.committee,
		PeerDrops:   d.peerDrops,
	}
	if rep.Neighbors == nil {
		rep.Neighbors = []string{}
	}
	if rep.Committee == nil {
		rep.Committee = []CommitteeMember{}
	}
	if rep.PeerDrops == nil {
		rep.PeerDrops = []PeerDropReport{}
	}
	if rep.Bytes.PerRound == nil {
		rep.Bytes.PerRound = []ByteRound{}
	}

	for name, p := range d.protocols {
		maxR := -1
		for r := range p.rounds {
			if r > maxR {
				maxR = r
			}
		}
		pr := &ProtocolReport{
			Step:          string(p.step),
			TotalMessages: make([]int, maxR+1),
			ValidMessages: make([]int, maxR+1),
			LateMessages:  p.lateMessages,
			MaxLateness:   p.maxLateness,
			LateByRound:   p.lateByRound,
			ArrivalLag:    []RoundLag{},
		}
		for r := 0; r <= maxR; r++ {
			ra, ok := p.rounds[r]
			if !ok {
				continue
			}
			pr.TotalMessages[r] = ra.total
			pr.ValidMessages[r] = ra.valid
			if ra.lagCount > 0 {
				pr.ArrivalLag = append(pr.ArrivalLag, RoundLag{
					Round:  r,
					Count:  ra.lagCount,
					MeanNs: ra.lagSumNs / int64(ra.lagCount),
					MaxNs:  ra.lagMaxNs,
					LastNs: ra.lagLastNs,
				})
			}
		}
		rep.Protocols[name] = pr
	}
	return rep
}

func (d *Daemon) writeReport() {
	rep := d.buildReport()
	data, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		d.logger.Error("stats: marshal report failed", zap.Error(err))
		return
	}
	if err := os.MkdirAll(d.outDir, 0o755); err != nil {
		d.logger.Error("stats: create output dir failed", zap.String("dir", d.outDir), zap.Error(err))
		return
	}
	path := filepath.Join(d.outDir, "stats-"+sanitize(d.nodeID)+".json")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		d.logger.Error("stats: write report failed", zap.String("path", path), zap.Error(err))
		return
	}
	d.logger.Info("stats: wrote report", zap.String("path", path))
}

// sanitize makes a node id safe to use as a filename component.
func sanitize(s string) string {
	return strings.NewReplacer("/", "_", "\\", "_", ":", "_").Replace(s)
}
