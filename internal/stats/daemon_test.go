package stats

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/mamorski/committee-sampling/internal/common"
	"github.com/mamorski/committee-sampling/pkg/config"
)

// stubSync satisfies common.Synchronizer without firing any ticks, so tests can
// drive the daemon deterministically.
type stubSync struct{}

func (stubSync) WaitForRound(common.Step, int) (<-chan struct{}, error) {
	return make(chan struct{}), nil
}

func (stubSync) TotalRounds(common.Step) (int, error) { return 0, nil }

func newTestDaemon(dir string) *Daemon {
	cfg := &config.Config{}
	cfg.Stats.OutputDir = dir
	return New(cfg, zap.NewNop(), stubSync{}, "node-1", "sid-1")
}

// TestLateAndArrivalDetection drives the consumer directly (white-box) so tick
// and message ordering is deterministic: a message stamped round 2 arriving
// while the current round is 5 must be flagged late by 3, and an on-time
// message must contribute an arrival-lag sample.
func TestLateAndArrivalDetection(t *testing.T) {
	d := newTestDaemon(t.TempDir())
	step := common.ExPostVerify
	base := time.Now()

	d.handle(event{kind: evtTick, step: step, round: 0, arrival: base})
	d.handle(event{kind: evtReceived, proto: "expost", step: step, round: 0, arrival: base.Add(5 * time.Millisecond)})
	d.handle(event{kind: evtValid, proto: "expost", step: step, round: 0})

	d.handle(event{kind: evtTick, step: step, round: 5, arrival: base.Add(50 * time.Millisecond)})
	d.handle(event{kind: evtReceived, proto: "expost", step: step, round: 2, arrival: base.Add(55 * time.Millisecond)})

	rep := d.buildReport()
	p := rep.Protocols["expost"]
	if p == nil {
		t.Fatal("missing expost protocol report")
	}
	if p.LateMessages != 1 {
		t.Fatalf("late_messages = %d, want 1", p.LateMessages)
	}
	if p.MaxLateness != 3 {
		t.Fatalf("max_lateness = %d, want 3", p.MaxLateness)
	}
	if p.LateByRound[2] != 1 {
		t.Fatalf("late_by_round[2] = %d, want 1", p.LateByRound[2])
	}
	if p.TotalMessages[0] != 1 || p.TotalMessages[2] != 1 {
		t.Fatalf("total_messages = %v, want [.. round0=1 round2=1]", p.TotalMessages)
	}
	if p.ValidMessages[0] != 1 {
		t.Fatalf("valid_messages[0] = %d, want 1", p.ValidMessages[0])
	}

	var lag0 *RoundLag
	for i := range p.ArrivalLag {
		if p.ArrivalLag[i].Round == 0 {
			lag0 = &p.ArrivalLag[i]
		}
	}
	if lag0 == nil {
		t.Fatal("no arrival-lag sample for round 0")
	}
	if want := (5 * time.Millisecond).Nanoseconds(); lag0.MaxNs != want {
		t.Fatalf("round 0 max arrival lag = %d ns, want %d ns", lag0.MaxNs, want)
	}
}

// TestWritesReport exercises the full async path: Start, record via the public
// recorder API, Close, then load the JSON report back.
func TestWritesReport(t *testing.T) {
	dir := t.TempDir()
	d := newTestDaemon(dir)
	d.Start()

	d.RecordReceived("exante", common.ExAnteVerify, 0, time.Now())
	d.RecordValid("exante", common.ExAnteVerify, 0)
	d.RecordNeighbors([]string{"peerA", "peerB"})
	d.RecordCommittee([]*common.CommitteeOutput{{ID: "m1", Grade: 2}})
	d.RecordPeerDrop("peerX", "ExAnteVerify", 3)

	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(dir, "stats-node-1.json"))
	if err != nil {
		t.Fatalf("read report: %v", err)
	}
	var rep Report
	if err := json.Unmarshal(data, &rep); err != nil {
		t.Fatalf("unmarshal report: %v", err)
	}

	if rep.NodeID != "node-1" || rep.SID != "sid-1" {
		t.Fatalf("identity = %s/%s, want node-1/sid-1", rep.NodeID, rep.SID)
	}
	if len(rep.Neighbors) != 2 {
		t.Fatalf("neighbors = %v, want 2", rep.Neighbors)
	}
	if len(rep.Committee) != 1 || rep.Committee[0].ID != "m1" {
		t.Fatalf("committee = %v, want [m1]", rep.Committee)
	}
	if len(rep.PeerDrops) != 1 || rep.PeerDrops[0].PeerID != "peerX" {
		t.Fatalf("peer_drops = %v, want [peerX]", rep.PeerDrops)
	}
	if p := rep.Protocols["exante"]; p == nil || p.TotalMessages[0] != 1 || p.ValidMessages[0] != 1 {
		t.Fatalf("exante report = %v, want total/valid round0=1", p)
	}
}
