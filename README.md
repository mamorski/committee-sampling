# Committee Sampling Framework

A simulation platform for setup-free committee election protocols. This software implements the algorithms and protocols described in **Setup-Free Committee Sampling with Subquadratic Communication** (Asiacrypt 2025).

## About the Paper

📄 **[View on IACR CryptoDB](https://iacr.org/cryptodb/data/paper.php?pubkey=36134)**

Large-scale cryptographic protocols face a fundamental challenge: communication complexity. Committee sampling addresses this by restricting communication to a small, randomly selected subset of participants, while gossip networks replace fully connected topologies with sparse graphs.

Existing committee-sampling protocols either require a trusted setup (problematic in decentralized settings) or have high communication overhead. The paper by Cohen, Das, and Moran presents a setup-free protocol that achieves subquadratic communication complexity.

Building on Andrychowicz and Dziembowski's work (CRYPTO'15), the protocol samples committees proportionally to resource expenditure. Key contributions include: (1) a formal framework for general resource proofs extending beyond proof-of-work, and (2) a more efficient committee-sampling protocol using VDFs and gossip techniques from Cohen, Loss, and Moran (FC'24).

This implementation provides the key building blocks:

- **Verifiable Random Functions (VRF)** for unpredictable sortition
- **Verifiable Delay Functions (VDF)** for time-based fairness
- **Resource-Bounded Proofs** for Sybil resistance
- **Multi-Digraph Aggregation (MDAG)** for efficient message collection

```bibtex
@inproceedings{asiacrypt-2025-36134,
  title={Setup-Free Committee Sampling with Subquadratic Communication},
  publisher={Springer-Verlag},
  author={Ran Cohen and Poulami Das and Tal Moran},
  year=2025
}
```

---

## Architecture Overview

The framework is organized into four layers:

| Layer           | Components                                         | Purpose                                      |
|-----------------|----------------------------------------------------|----------------------------------------------|
| Application     | `cmd/committee-sampling`, `internal/boot`          | Entry point and protocol orchestration       |
| Protocol        | `mdag`, `exante`, `expost`, `gce`, `resourcebound` | Committee election and timestamp protocols   |
| Infrastructure  | `network`, `synchronizer`, `vrf`, `vdf`, `metrics` | P2P networking, time sync, cryptographic ops |
| Configuration   | `pkg/config`                                       | Runtime parameter loading and validation     |

Protocol execution follows these phases:

1. **Discovery** — DHT-based peer discovery
2. **Graph Building** — Multi-round neighbor selection with bounded degree
3. **Initialization** — VRF key generation, resource proofs, VDF evaluation
4. **Election** — Parallel Ex-Post/Ex-Ante verification with grade computation

See [ARCHITECTURE.md](ARCHITECTURE.md) for a detailed breakdown of each module.

---

## How the System Fits Together

The binary entry point is `cmd/committee-sampling/main.go`. At startup the following stages fire in order:

1. **Configuration** – `pkg/config/config.go` loads a JSON profile (or falls back to `./configs/{ENV}.json`) and decodes it into strongly typed structs.
2. **Logging** – `createLogger` builds a zap logger configured by `logger.level`.
3. **Time Synchronization** – `internal/synchronizer` queries the configured NTP server, computes per-step start times, and exposes `WaitForRound(step, round)` channels that gate protocol progress.
4. **Networking** – `internal/network` spins up a libp2p host, joins the discovery DHT, handles neighbor churn, authenticates protobuf messages, and applies optional simulation behaviors.
5. **Metrics** – `internal/metrics` registers Prometheus counters and optionally launches a `/metrics` HTTP server or Pushgateway pusher.
6. **Protocol Bootstrapping** – `internal/boot` wires together the building blocks:
   - `internal/resourceproof` provides proof-of-work based resource certificates.
   - `internal/resourcebound` runs Ex-Post and Ex-Ante timestamp protocols in parallel and intersects their outputs.
   - `internal/mdag` runs the Multi-Digraph aggregation gadget for message collection.
   - `internal/expost` and `internal/exante` orchestrate their respective timestamp rounds using the synchronizer.
   - `internal/vdf` and `internal/vrf` wrap the VDF/VRF implementations.
   - `internal/gce` drives the two-phase Graded Committee Election.
7. **Execution** – `Bootstrap.Run()` performs the initialization phase (VRF key generation, RB-ExP proofs, VDF evaluation) followed by committee election, printing elected members (ID, verification key, grade) and logging the final neighbor set.

Shutdown is coordinated through OS signal handlers, a background error channel, and graceful teardown of metrics and network components.

---

## Configuration

All runtime parameters live under the top-level keys described below. Examples can be found in `configs/dev.json` and `configs/sample_simulation_plan.json`.

### `network`

```json
{
  "network": {
    "listen_port": 0,
    "max_outbound_degree": 4,
    "degree_slack": 0,
    "discovery_config": {
      "protocol_id": "/committee-sampling/1.0.0",
      "interval": "5s",
      "bootstrap_peers": [
        "/ip4/127.0.0.1/tcp/4001"
      ]
    },
    "connectivity_retries": 3,
    "drop_on_send": false,
    "drop_on_send_probability": 0.0
  }
}
```

* `drop_on_send` adds a coarse random lossy link simulation using crypto-grade randomness per recipient.
* `degree_slack` lets a node accept a limited number of inbound connections beyond `max_outbound_degree`, reducing the chance of creating supernodes while keeping the overlay connected.

### `graph`, `committee`, `synchronization`

These control the protocol dynamics: graph diameter/grades, target committee size, session identifier, VRF security parameter `lambda`, the VDF delay, and timeouts for each round. Synchronization includes the launch timestamp and NTP server (`time.google.com` by default).

### `logger`

Single field `level` (`debug`, `info`, `warn`, `error`).

### `metrics`

```json
{
  "metrics": {
    "enabled": true,
    "push_gateway": {
      "enabled": true,
      "url": "http://localhost:9091",
      "username": "",
      "password": "",
      "delete_on_stop": false
    },
    "http_server": {
      "enabled": false,
      "port": 9090,
      "path": "/metrics"
    },
    "push_interval": "30s"
  }
}
```

The collector always registers Prometheus counters:

| Metric           | Labels                                | Meaning                                                  |
|------------------|---------------------------------------|----------------------------------------------------------|
| `total_messages` | `round`, `protocol`, `node_id`, `sid` | Every inbound MDAG/ExAnte/ExPost packet the node parsed. |
| `valid_messages` | Same as above                         | Subset that passed all validation checks.                |

When `push_gateway.enabled` is true, metrics are pushed on the configured interval and optionally deleted on shutdown. The HTTP server exposes live metrics if `http_server.enabled` is set. Disable the entire block (`enabled: false`) for bare-bones runs.

---

## Building and Running

```bash
# Install dependencies (protobufs, Go modules, lint config)
make deps

# Generate protobuf stubs if definitions changed
make proto

# Run tests
make test

# Build the committee-sampling binary
make build
```

To launch a node:

```bash
./bin/committee-sampling -config ./configs/dev.json
```

If `-config` is omitted the loader reads `ENV` (default `dev`) and uses `./configs/{env}.json`.

Shutdown with `Ctrl+C`. Logs land in the current working directory; peer IDs, elected committee members, and error conditions are emitted via zap.

---

## Batch Simulations

`scripts/run_simulations.py` orchestrates multi-node experiments. It starts a libp2p bootstrap server, spawns committee-sampling binaries, assigns generated configs, and optionally kills random nodes for liveness testing.

```bash
python scripts/run_simulations.py configs/sample_simulation_plan.json
```

Key CLI flags:

| Flag                                                                   | Purpose                                                                            |
|------------------------------------------------------------------------|------------------------------------------------------------------------------------|
| `--drop-on-send-percent`, `--drop-on-send-probability`                 | Default drop-on-send profile for nodes not overriding the value in the batch file. |
| `--kill-random-up-to`, `--kill-random-delay-sec`, `--kill-probability` | Enable random process termination for failure injection.                           |

Each run entry in the batch JSON can redefine the same fields. Run-specific config shards are written to `scripts/configs/`, logs to `scripts/logs/<run>/`. A tar archive is created after each run for post-mortem analysis.

`configs/sample_simulation_plan.json` illustrates two runs:

1. Baseline 1000-node network with no simulation.
2. 3 % of nodes enabling drop-on-send at 1 % probability.

---

## Observability & Troubleshooting

* **Logs** – zap outputs to stdout/stderr. For simulations, per-node logs live under `scripts/logs/<run>/node-*.log`.
* **Metrics** – visit the HTTP endpoint or query the Pushgateway / Prometheus instance you configured.
* **Neighbor Topology** – final neighbor lists are logged at the end of each run for quick sanity checks.
* **Committee Output** – elected members are printed both to stdout and to the structured logs with grade details.

---

## Development Notes

* Run `go test ./...` before submitting changes.
* Lint with `golangci-lint run` (the repository includes configuration).
* Protobufs live under `pkg/proto`; regenerate with `make proto` whenever `.proto` files change.
* Avoid editing generated files manually (`*.pb.go`).
* The Go modules target Go 1.23+. Ensure your toolchain matches the `go.mod` requirement.

---

## Repository Layout

| Path                                                  | Purpose                                                 |
|-------------------------------------------------------|---------------------------------------------------------|
| `cmd/committee-sampling`                              | Main binary.                                            |
| `internal/boot`                                       | Protocol bootstrap wiring.                              |
| `internal/network`                                    | libp2p host, discovery.                                 |
| `internal/mdag`, `internal/exante`, `internal/expost` | Protocol sub-components.                                |
| `internal/resourceproof`, `internal/resourcebound`    | Resource-bounded proofs.                                |
| `internal/synchronizer`                               | Time-based round scheduler.                             |
| `internal/metrics`                                    | Prometheus collector.                                   |
| `pkg/config`                                          | Config loader.                                          |
| `pkg/proto`                                           | Generated protobuf stubs.                               |
| `scripts/`                                            | Automation, simulation runners, helper scripts.         |

---

## License

Distributed under the terms of the `LICENSE` file included in this repository.
