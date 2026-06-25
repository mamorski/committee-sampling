// Package stats implements a per-node statistics daemon. Protocols accumulate
// counters locally under their own mutex and flush once per round via
// RecordRoundStats, so the daemon's lock is acquired O(rounds) times rather
// than O(messages). At shutdown the daemon writes one JSON report per node.
package stats

import (
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
	nodeID         string
	sid            string
	outDir         string
	logger         *zap.Logger
	metricsEnabled bool

	mu        sync.Mutex
	protocols map[string]*protoAgg
	bytes     ByteReport
	neighbors []string
	committee []CommitteeMember
	peerDrops []PeerDropReport
	caches    map[string]CacheStat
}

var _ common.StatsRecorder = (*Daemon)(nil)

// New constructs the daemon.
func New(cfg *config.Config, logger *zap.Logger, nodeID, sid string) *Daemon {
	outDir := cfg.Stats.OutputDir
	if outDir == "" {
		outDir = "."
	}
	return &Daemon{
		nodeID:         nodeID,
		sid:            sid,
		outDir:         outDir,
		logger:         logger.Named("stats"),
		metricsEnabled: cfg.Metrics.Enabled,
		protocols:      make(map[string]*protoAgg),
		bytes:          ByteReport{ByProtocol: make(map[string]ByteCounts)},
		caches:         make(map[string]CacheStat),
	}
}

// Start is a no-op; kept for call-site compatibility.
func (d *Daemon) Start() {}

// Close writes the JSON report. Safe to call once.
func (d *Daemon) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.writeReport()
	return nil
}

func (d *Daemon) RecordRoundStats(proto string, step common.Step, round int,
	total, valid, lateCount, maxLateness int,
	lagSumNs, lagMaxNs, lagLastNs int64, lagCount int) {

	d.mu.Lock()
	p := d.proto(proto, step)
	ra := p.round(round)
	ra.total += total
	ra.valid += valid
	if lagCount > 0 {
		ra.lagSumNs += lagSumNs
		ra.lagCount += lagCount
		ra.lagLastNs = lagLastNs
		if lagMaxNs > ra.lagMaxNs {
			ra.lagMaxNs = lagMaxNs
		}
	}
	if lateCount > 0 {
		p.lateMessages += lateCount
		p.lateByRound[round] += lateCount
		if maxLateness > p.maxLateness {
			p.maxLateness = maxLateness
		}
	}
	d.mu.Unlock()

	if d.metricsEnabled && (total > 0 || valid > 0) {
		roundStr := strconv.Itoa(round)
		metrics.TotalMessages.WithLabelValues(roundStr, proto, d.nodeID, d.sid).Add(float64(total))
		metrics.ValidMessages.WithLabelValues(roundStr, proto, d.nodeID, d.sid).Add(float64(valid))
	}
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

func (d *Daemon) RecordCacheStats(name string, hits, misses int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	s := d.caches[name]
	s.Hits += hits
	s.Misses += misses
	d.caches[name] = s
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
	caches := make(map[string]CacheStat, len(d.caches))
	for k, v := range d.caches {
		caches[k] = v
	}
	rep := Report{
		NodeID:      d.nodeID,
		SID:         d.sid,
		GeneratedAt: time.Now(),
		Protocols:   make(map[string]*ProtocolReport, len(d.protocols)),
		Bytes:       d.bytes,
		Neighbors:   d.neighbors,
		Committee:   d.committee,
		PeerDrops:   d.peerDrops,
		Caches:      caches,
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
