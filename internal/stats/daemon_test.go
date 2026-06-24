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

func newTestDaemon(dir string) *Daemon {
	cfg := &config.Config{}
	cfg.Stats.OutputDir = dir
	return New(cfg, zap.NewNop(), "node-1", "sid-1")
}

// TestLateAndArrivalDetection drives RecordRoundStats directly so late-message
// and lag semantics are verified end-to-end without protocol machinery.
//
// Round 0: 1 message, 1 valid, lag = 5 ms.
// Round 2 arriving when currentRound=5: 1 message, 0 valid, late by 3.
func TestLateAndArrivalDetection(t *testing.T) {
	d := newTestDaemon(t.TempDir())
	step := common.ExPostVerify

	lag5ms := (5 * time.Millisecond).Nanoseconds()

	// round 0: one valid message, 5 ms lag
	d.RecordRoundStats("expost", step, 0,
		1, 1, 0, 0,
		lag5ms, lag5ms, lag5ms, 1)

	// round 2: one message arriving at currentRound=5, late by 3, no lag data
	d.RecordRoundStats("expost", step, 2,
		1, 0, 1, 3,
		0, 0, 0, 0)

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
		t.Fatalf("total_messages = %v, want round0=1 round2=1", p.TotalMessages)
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
	if lag0.MaxNs != lag5ms {
		t.Fatalf("round 0 max arrival lag = %d ns, want %d ns", lag0.MaxNs, lag5ms)
	}
}

// TestWritesReport exercises the full path: record, Close, then load the JSON.
func TestWritesReport(t *testing.T) {
	dir := t.TempDir()
	d := newTestDaemon(dir)
	d.Start()

	d.RecordRoundStats("exante", common.ExAnteVerify, 0, 1, 1, 0, 0, 0, 0, 0, 0)
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

func BenchmarkRecordRoundStats(b *testing.B) {
	d := newTestDaemon(b.TempDir())
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			d.RecordRoundStats("exante", common.ExAnteVerify, 1, 15, 12, 0, 0, 0, 0, 0, 0)
		}
	})
}
