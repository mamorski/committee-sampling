# Report Revision — Action Items

Action items derived from the supervisor (Tal) review exchange on the project
report. Tal's follow-up email refined several of the original points; the
refinements are reflected below.

**Plan:** implement all code changes first, then run the simulations once every
requirement is in place, then do the analysis and write-up. Items are ordered to
follow that sequence — code changes first, the report restructuring last.

---

## Phase 1 — Code changes (do first)

### 1. Adversarial model: add node-drop and peer-drop

- [ ] Keep the existing per-message drop data (useful for round-timing estimates
      — see Phase 3).
- [ ] Add a **node-drop** mode: nodes randomly drop out of the protocol
      completely.
  - Implement node-drop directly in the **simulation framework** (not bolted on
    externally): a dropped node stops participating entirely for the rest of the
    run — it sends no messages and its peers receive nothing from it.
- [ ] Add a **peer-drop** mode: node stays up but a connection drops completely
      (more realistic over TCP than dropping individual messages).
- [ ] Frame all three as a simplified adversarial model (no malicious behavior —
      acceptable for this project).

> **Node-drop vs. peer-drop — the distinction (vertex failure vs. edge failure):**
>
> - **Node-drop = remove a vertex.** A node leaves the protocol entirely: it
>   stops sending *and* receiving on **all** its links and contributes nothing
>   to consensus. All its neighbors lose contact with it at once. Models a
>   **crash / churn** (machine goes offline). The dropped node is guaranteed to
>   fail; the test is whether the rest still reaches consensus with fewer
>   participants (e.g., does the committee keep an honest majority).
> - **Peer-drop = remove an edge.** A node stays fully alive and keeps talking to
>   all its *other* neighbors, but **one specific connection** dies completely
>   (bidirectional, since the TCP connection drops). Both endpoints remain
>   healthy and participating. Models a **partition between a pair**. Usually
>   nobody fails — the test is whether the random graph stays connected with low
>   diameter after random edges are removed.
> - **Relationship / why not redundant:** node-drop is the fully-correlated
>   special case of peer-dropping *every* edge of one node at once. Node-drop
>   stresses robustness to losing **participants**; peer-drop stresses the random
>   graph's **edge-connectivity**. Both differ from message-drop (independent
>   per-message loss, link still alive) by being **persistent, all-or-nothing**
>   failures — closer to how TCP actually fails.
- [ ] Support substantially more repetitions per configuration (current count is
      too low for statistical analysis).

### 2. Instrument communication volume in bytes

- [ ] Replace message *counts* with per-node and total communication *volume in
      bytes*.
- [ ] Capture total communication **per round** (needed for timing estimates).
- [ ] Capture message sizes (needed to refine timing estimates).
- [ ] Rationale: byte volumes are implementation-dependent and therefore an
      actual contribution; raw message counts are not interesting.

### 3. Scaling study harness (node count)

- [ ] Support varying node count from **500 to 1000 in steps of 20**, keeping
      the security parameter / average degree constant.
- [ ] Use the **large-degree** setting: **np = 40**.
  - np = 40 → disconnected-graph probability < 2^−40 (the concrete "negligible"
    standard); diameter ≤ 4 w.h.p.
  - np = 20 → disconnected-graph probability > 2^−15 for 10,000 nodes (too weak).

### 4. Committee-size scaling support

- [ ] Enable measuring communication complexity / round length vs. committee
      size, keeping everything else constant.
- [ ] Verification-phase communication should be linear in committee size — may
      be confirmable from existing data without new runs (report already has
      multiple committee sizes). Add instrumentation only if existing data is
      insufficient.

---

## Phase 2 — Run simulations (after all code changes land)

- [ ] Run all configurations (message-drop, node-drop, peer-drop) with high
      repetition counts.
- [ ] Run the node-count scaling sweep (500→1000, step 20, np=40).
- [ ] Run/collect committee-size scaling data (if not already available).

### Logistics

- [ ] Server is available — run simulations **late at night**, do not kill other
      running processes (CPU usually idle, >300 MB RAM free).
- [ ] Notify Tal before running, in case of updates/backups.

---

## Phase 3 — Analysis & write-up

### 5. Deepen the failed-consensus analysis

- [ ] Investigate cases where full consensus was *not* reached, in more detail.
- [ ] Check whether failed nodes had more dropped messages than other nodes.
- [ ] Check whether any failed node had a round with *all* neighbors dropped.
- [ ] Explain the mechanism that prevents full consensus in those cases.
- [ ] Note: this analysis *is* a contribution — message-drop behavior of the
      random graph was not analyzed in the paper.

### 6. Round / protocol timing estimates (WAN)

- [ ] Do **not** simulate network delays — compute estimates from WAN latency
      and throughput instead (we are not running on a real WAN).
- [ ] Use the message-drop data to translate drop probability into round length.
  - Example: if full consensus holds with prob. 1−ε at 25% message drop, and
    "75% of messages arrive within 160 ms", then a 160 ms round length gives
    1−ε success probability.
  - Assumes drops are uniform across clients — fine for a rough estimate.
- [ ] Factor in message sizes for a better estimate where feasible.
- [ ] Report per-round and full-protocol time. Millisecond accuracy is not
      needed — seconds (not minutes) for the whole protocol is a good result.
- [ ] Source for WAN latency figures: https://arxiv.org/pdf/2303.02514
      (skimmed — find a better source if possible).

### 7. Scaling results & extrapolation

- [ ] Plot the growth rate of communication complexity vs. node count.
- [ ] Extrapolate to 10,000 nodes (instead of running impractical experiments).
- [ ] Goal: show actual communication complexity in practice, including p2p /
      DHT / network overhead, scales well.
- [ ] Plot communication complexity / round length vs. committee size; confirm
      linear verification-phase communication.
- [ ] Extrapolate to 10,000 parties with committees of size **500** (guarantees
      honest committee majority except w.p. 2^−40 when 2/3 of the population is
      honest).

### 8. libp2p connection stability (code DONE — write-up only)

- [ ] Add a subsection to the implementation chapter explaining that connection
      stability between graph-generation and verification phases is enforced at
      the application layer, without modifying libp2p or using non-default
      config.
  - Each node freezes its neighbor set after graph generation.
  - Protocol sends to / accepts from only the committed peer identifiers.
  - Phase flags gate which messages are accepted; messages from peers outside
    the neighbor set are discarded before reaching the protocol.
  - libp2p manages the transport with default behavior; since the honest
    communication graph is defined by peer identity (not open TCP connections),
    libp2p's connection churn does not affect it.

### 9. Restructure the "sanity check" results (do last)

- [ ] Shorten the section on committee sizes, message counts, and node degrees.
- [ ] Reframe it as an implementation sanity check (confirms no major bugs in
      code or proofs), not as an experimental contribution.
- [ ] Move it out of the main evaluation chapter.
- [ ] Rationale: these only verify mathematical guarantees already proven in the
      paper, so they are not a contribution of the project.

---

## Final step

- [ ] Share an updated draft with Tal once all of the above are in place.
