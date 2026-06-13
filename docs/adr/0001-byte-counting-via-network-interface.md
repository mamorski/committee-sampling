# Byte counting exposed through the Network interface

Protocol modules (ExPost, ExAnte, MDAG) need per-round byte deltas for communication-volume analysis. We added `BytesForProtocol(id string) (in, out int64)` to the `common.Network` interface, backed by go-libp2p's `BandwidthCounter` in `P2PNode`. This keeps libp2p types out of protocol structs and keeps the interface mockable for tests.

**Considered options:** (A) inject `BandwidthCounter` directly into each module — couples protocol code to a libp2p type; (C) snapshot in Bootstrap — pushes round-level logic into a module already large. Option B was chosen because the `Network` interface is the existing abstraction boundary between protocol logic and transport.
