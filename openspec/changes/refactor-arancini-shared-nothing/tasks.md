## 0. Phase 0: Baseline Hot-Path Hardening
- [x] 0.1 Remove `.await` while holding curation state locks in all paths (`process_updates`, peer down handling, peer-up recovery handling).
- [x] 0.2 Refactor lock usage to gather emit sets while locked, drop lock, then perform channel sends/network I/O.
- [x] 0.3 Refactor state dump flow so filesystem writes and `fsync` do not run while holding the state lock.
- [x] 0.4 Replace string-based IPv4-to-IPv6 conversion (`format!` + `parse`) with numeric conversion.
- [x] 0.5 Rewrite BMP framing path to reusable buffers (no per-message `Vec` allocation and no peek-loop spin on partial headers).
- [x] 0.6 Remove constant Kafka partition key behavior in legacy path and define explicit partitioning/ordering strategy until JetStream cutover is complete.
- [x] 0.7 Add bounded in-flight publish handling in legacy producer path (no strict one-batch-at-a-time pipeline).
- [x] 0.8 Reduce avoidable update cloning; add shared attribute ownership where one BGP update yields many NLRI records.
- [x] 0.9 Evaluate and implement faster hashing strategy for trusted state hot paths (or document rejection rationale if not adopted).
- [x] 0.10 Reduce high-frequency metrics allocation/cardinality overhead in per-update paths.

## 1. Arancini Runtime and Messaging Implementation
- [x] 1.1 Add monoio thread-per-core BMP listener workers and configure `SO_REUSEPORT` on listener sockets.
- [x] 1.2 Integrate `io_uring`-backed fixed-slot packet ingress and zero-copy parse handoff.
- [x] 1.2.1 Add per-worker fixed-slot ring configuration (slot size/count) and enforce max BMP frame size bounds.
- [x] 1.2.2 Implement non-spinning framed monoio read loop (no peek loop) that recycles slots after parse completion.
- [x] 1.2.3 Verify steady-state ingest avoids per-message allocations in the worker hot path.
- [ ] 1.2.4 Evaluate/implement kernel-registered buffer-select (`ProvideBuffers`/`BUFFER_SELECT`) path once runtime support is available (or via custom ingress driver).
- [x] 1.3 Ensure deterministic session-to-core ownership for BMP connections and prohibit cross-core RIB mutation.
- [x] 1.4 Build bounded lock-free MPSC bridge from monoio workers to a dedicated Tokio NATS sidecar thread.
- [x] 1.5 Implement NATS JetStream async publish path with non-blocking ACK handling and error metrics.
- [x] 1.6 Implement subject routing schema `arancini.updates.<router_ip>.<peer_asn>.<afi_safi>`.
- [x] 1.7 Replace global curation lock path with per-core sharded RIB state and local dedup/synthetic-withdraw processing.
- [x] 1.8 Implement periodic per-core snapshot persistence and startup restore (NATS KV bucket or NVMe store).
- [x] 1.9 Implement Cap'n Proto direct-to-buffer serialization path and remove avoidable hot-path allocations.
- [x] 1.10 Apply required socket options (`SO_REUSEPORT`, `TCP_NODELAY`) in runtime socket setup.
- [x] 1.13 Add runtime-configurable listener backlog and ingest `SO_RCVBUF` controls; align defaults with documented host tuning.
- [x] 1.11 Add observability for queue depth, publish latency, ACK lag, per-core update throughput, and snapshot timing.
- [x] 1.12 Add migration notes for downstream systems moving from Kafka topics to JetStream subjects.

## 2. Platform Tuning and Validation
- [x] 2.1 Add/extend tests for bridge backpressure, publish failure handling, and state ownership invariants.
- [x] 2.2 Add regression tests ensuring no lock is held across `.await` in curation/emit paths.
- [ ] 2.3 Run integration tests with multi-router BMP sessions and verify per-core deterministic ownership behavior.
- [ ] 2.4 Run throughput benchmarks and verify near-linear scaling trend with additional cores.
- [ ] 2.5 Verify hot-path allocation profile improves after framing and direct-to-buffer serialization changes.
- [ ] 2.6 Validate host tuning guidance and runtime socket option enforcement on Linux (`sysctl`, fd limits, backlog, RSS/IRQ guidance).
- [ ] 2.8 Validate backlog and receive-buffer socket tuning under burst BMP table-dump scenarios.
- [x] 2.7 Run `openspec validate refactor-arancini-shared-nothing --strict`.
