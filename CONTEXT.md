# Committee Sampling — Domain Glossary

## Monitoring / Observability

**Prometheus counters** (`total_messages`, `valid_messages`) — **removed**. With 1000 nodes per run, scraping/pushing per-node (`node_id`-labelled) metrics was impractical, and the per-node stats daemon that emitted them added load. The sole observability mechanism is now **structured logging** (JSON via zap): per-round message counts, arrival lag/lateness, byte volumes, cache hit/miss, committee, neighbors and peer-drops are emitted at `WARN`, so a run with `logger.level: warn` keeps only that signal. Per-node log files are the authoritative source for post-run analysis. (`internal/metrics` still hosts the optional `/metrics` server / Pushgateway pusher but registers no application counters.)

**Byte instrumentation** — the planned addition of communication-volume tracking in bytes. This is an *addition* to existing message-count logging, not a replacement. Byte volumes are an implementation-dependent contribution; message counts are not. Both will coexist in logs.

Measurement strategy: use go-libp2p's `BandwidthCounter` (`libp2p.BandwidthReporter(bwc)`) to track bytes across all protocols on the host. Reported two ways: (1) **per-round delta** — snapshot the per-protocol counter at the start and end of each verification round in ExPost/ExAnte/MDAG, log the delta; (2) **total at shutdown** — log `bwc.GetBandwidthTotals()` and per-protocol totals for every known protocol ID.

Access pattern: a new `BytesForProtocol(id string) (in, out int64)` method is added to the `common.Network` interface, backed by `BandwidthCounter` in `P2PNode`. Protocol modules call this at round boundaries without knowing about libp2p internals. See [ADR 0001](docs/adr/0001-byte-counting-via-network-interface.md).

**Protocol bytes** — bytes attributed to the core protocol IDs: MDAG (`/mdag/1.0.0/…`), ExPost (`/expost/1.0.0/…`), ExAnte (`/exante/1.0.0/…`). These are the "actual protocol" contribution.

**Network overhead bytes** — bytes on all other protocol IDs: graph building (`/graph/proposal/1.0.0/…`, `/graph/drop/1.0.0/…`), DHT (`/kad/…`), libp2p internals (`/identify/…`, `/ping/…`). Captured by `BandwidthCounter` automatically; distinguished in analysis by protocol ID.

## Scaling Study

**Node-count sweep** — the primary scaling experiment: `num_nodes` varies from 500 to 1000 in steps of 20 (26 distinct values), 5 repetitions per node count (130 total runs). Fixed: `max_outbound_degree = 16` (average degree ≈ 40, np = 40 target), `diameter = 5` (D = 5 → R = 15 MDAG rounds, one round of slack over the w.h.p. bound of 4), `committee_size = 50`. Results are plotted as communication complexity vs. node count and extrapolated to 10 000 nodes.

**Sweep config** — the batch config JSON is extended with a `sweep` key that the harness expands into the full `runs` array before executing. One file captures the complete experimental setup for reproducibility. Schema: `num_nodes` sub-object with `from`/`to`/`step`, `repetitions`, and all fixed template parameters inline. Example:
```json
{
  "sweep": {
    "num_nodes": {"from": 500, "to": 1000, "step": 20},
    "repetitions": 5,
    "max_outbound_degree": 16,
    "diameter": 5,
    "committee_size": 50,
    "verify_timeout": "30s"
  }
}
```
The harness expands this into 26 × 5 = 130 run entries before executing. `verify_timeout = 30s` (reduced from 45s — safe at degree 40 since the bottleneck is computation, not gossip propagation). Baseline-only (no drops) — adversarial experiments are a separate config.

**Adversarial runs** — separate batch config at fixed `num_nodes = 1000`, same parameters as existing `sample_simulation_plan.json`. Each scenario runs 3 times for statistical significance, expressed via a `"repetitions": 3` field on each run entry in the `runs` array (the harness expands this inline). Run names are `{scenario_name}-rep-01`, `{scenario_name}-rep-02`, `{scenario_name}-rep-03` — strip the `-rep-NN` suffix to group repetitions in the analysis notebook.

## Adversarial Model

**Message-drop** — independent per-message packet loss; the TCP connection stays alive. Simulated via `drop_on_send` probability in the network layer.

**Node-drop** — a node exits the protocol entirely (crash/churn). Implemented in the simulation harness (`run_simulations.py`): the process is terminated after the graph phase, so it sends and receives nothing for the rest of the run. Models vertex failure.

**Peer-drop** — one specific connection between two nodes dies permanently from the local node's perspective. The node remains alive and communicates with all other neighbors. Models edge failure. Implemented in `internal/network/host.go` as a one-sided local neighbor-map deletion: the dropping node stops sending to and accepting messages from the victim peer. The victim continues sending (those messages are rejected by the IsNeighbor check), making this a one-sided simulation — accepted as sufficient for the research purpose (conservative: victim wastes some bandwidth, but the protocol's convergence properties are what matter).
