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

// eventBuffer sizes the event channel. Producers block (with a ctx escape) when
// it is full, so counts are never silently dropped during a normal run.
const eventBuffer = 1 << 16

type evtKind int

const (
	evtReceived evtKind = iota
	evtValid
	evtTick
	evtBytesRound
	evtBytesFinal
	evtNeighbors
	evtCommittee
	evtPeerDrop
)

type event struct {
	kind     evtKind
	proto    string
	step     common.Step
	round    int
	arrival  time.Time
	pid      string
	delta    common.RoundByteDelta
	summary  common.ByteSummary
	perProto map[string]common.ProtoByteTotals
	strs     []string
	members  []CommitteeMember
	peerID   string
	phase    string
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

// Daemon collects all per-node statistics on one goroutine and writes a JSON
// report at shutdown. It implements common.StatsRecorder.
type Daemon struct {
	nodeID string
	sid    string
	outDir string
	logger *zap.Logger
	sync   common.Synchronizer

	events chan event
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// The fields below are owned exclusively by the consumer goroutine (run);
	// no other goroutine touches them, so no locking is needed.
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
		events:    make(chan event, eventBuffer),
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

// Start launches the consumer goroutine plus one tick-watcher per watched step.
func (d *Daemon) Start() {
	d.wg.Add(1)
	go d.run()
	for _, step := range stepsToWatch {
		ticks, err := d.sync.TotalRounds(step)
		if err != nil || ticks <= 0 {
			continue
		}
		d.wg.Add(1)
		go d.watch(step, ticks)
	}
}

// Close stops the watchers, drains buffered events, writes the report, and
// waits for every goroutine to exit. Safe to call once.
func (d *Daemon) Close() error {
	d.cancel()
	d.wg.Wait()
	return nil
}

// emit hands an event to the consumer. It blocks until the consumer accepts it
// (back-pressure preserves counts) but bails out if the daemon is shutting down,
// so it never deadlocks and never panics on a closed channel.
func (d *Daemon) emit(ev event) {
	select {
	case d.events <- ev:
	case <-d.ctx.Done():
	}
}

func (d *Daemon) RecordReceived(proto string, step common.Step, round int, arrival time.Time) {
	d.emit(event{kind: evtReceived, proto: proto, step: step, round: round, arrival: arrival})
}

func (d *Daemon) RecordValid(proto string, step common.Step, round int) {
	d.emit(event{kind: evtValid, proto: proto, step: step, round: round})
}

func (d *Daemon) RecordBytesPerRound(step common.Step, round int, protocolID string, delta common.RoundByteDelta) {
	d.emit(event{kind: evtBytesRound, step: step, round: round, pid: protocolID, delta: delta})
}

func (d *Daemon) RecordBytesFinal(summary common.ByteSummary, perProtocol map[string]common.ProtoByteTotals) {
	d.emit(event{kind: evtBytesFinal, summary: summary, perProto: perProtocol})
}

func (d *Daemon) RecordNeighbors(neighbors []string) {
	d.emit(event{kind: evtNeighbors, strs: append([]string(nil), neighbors...)})
}

func (d *Daemon) RecordCommittee(members []*common.CommitteeOutput) {
	cm := make([]CommitteeMember, 0, len(members))
	for _, m := range members {
		if m != nil {
			cm = append(cm, CommitteeMember{ID: m.ID, Grade: m.Grade})
		}
	}
	d.emit(event{kind: evtCommittee, members: cm})
}

func (d *Daemon) RecordPeerDrop(peerID, phase string, round int) {
	d.emit(event{kind: evtPeerDrop, peerID: peerID, phase: phase, round: round})
}

// run is the single consumer goroutine. It owns all aggregation state. On ctx
// cancellation it drains whatever is still buffered, writes the report, exits.
func (d *Daemon) run() {
	defer d.wg.Done()
	for {
		select {
		case ev := <-d.events:
			d.handle(ev)
		case <-d.ctx.Done():
			for {
				select {
				case ev := <-d.events:
					d.handle(ev)
				default:
					d.writeReport()
					return
				}
			}
		}
	}
}

func (d *Daemon) handle(ev event) {
	switch ev.kind {
	case evtTick:
		d.curRound[ev.step] = ev.round
		if d.tickTimes[ev.step] == nil {
			d.tickTimes[ev.step] = make(map[int]time.Time)
		}
		d.tickTimes[ev.step][ev.round] = ev.arrival

	case evtReceived:
		p := d.proto(ev.proto, ev.step)
		ra := p.round(ev.round)
		ra.total++
		metrics.TotalMessages.WithLabelValues(strconv.Itoa(ev.round), ev.proto, d.nodeID, d.sid).Inc()

		// Late message: stamped round is below the step's current round.
		if cur, ok := d.curRound[ev.step]; ok && cur >= 0 && ev.round < cur {
			late := cur - ev.round
			p.lateMessages++
			p.lateByRound[ev.round]++
			if late > p.maxLateness {
				p.maxLateness = late
			}
		}

		// Arrival lag: time from the round's start tick to message arrival.
		if tt, ok := d.tickTimes[ev.step][ev.round]; ok && !tt.IsZero() {
			lag := ev.arrival.Sub(tt).Nanoseconds()
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

	case evtValid:
		d.proto(ev.proto, ev.step).round(ev.round).valid++
		metrics.ValidMessages.WithLabelValues(strconv.Itoa(ev.round), ev.proto, d.nodeID, d.sid).Inc()

	case evtBytesRound:
		d.bytes.PerRound = append(d.bytes.PerRound, ByteRound{
			Step: string(ev.step), Round: ev.round, ProtocolID: ev.pid,
			InDelta: ev.delta.WireIn, OutDelta: ev.delta.WireOut,
			PayloadInDelta: ev.delta.PayloadIn, PayloadOutDelta: ev.delta.PayloadOut,
			EnvelopeInDelta: ev.delta.EnvIn, EnvelopeOutDelta: ev.delta.EnvOut,
		})

	case evtBytesFinal:
		d.bytes.Summary = ev.summary
		for pid, c := range ev.perProto {
			d.bytes.ByProtocol[pid] = ByteCounts{
				In: c.WireIn, Out: c.WireOut,
				PayloadIn: c.PayloadIn, PayloadOut: c.PayloadOut,
				EnvelopeIn: c.EnvIn, EnvelopeOut: c.EnvOut,
			}
		}

	case evtNeighbors:
		d.neighbors = ev.strs

	case evtCommittee:
		d.committee = ev.members

	case evtPeerDrop:
		d.peerDrops = append(d.peerDrops, PeerDropReport{PeerID: ev.peerID, Phase: ev.phase, Round: ev.round})
	}
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
