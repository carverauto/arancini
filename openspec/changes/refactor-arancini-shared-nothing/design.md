## Context
Project Arancini is a major runtime and messaging refactor of Risotto focused on carrier-grade BMP telemetry throughput and deterministic curation behavior. The current Tokio + Kafka path is replaced with a shared-nothing `monoio` ingest runtime and NATS JetStream egress.

Hot-path profiling and code review found several pre-existing failure modes in the baseline implementation that can independently cause throughput collapse under load. These are treated as first-class scope in this change, not optional polish.

## Goals / Non-Goals
- Goals:
  - Remove known baseline choke points before and during the runtime pivot.
  - Minimize ingest overhead with `io_uring` and fixed registered buffers.
  - Enforce deterministic per-session core ownership for lock-free local state mutation.
  - Decouple hot-path parsing from NATS client runtime via a high-performance bridge.
  - Support horizontal scaling with CPU cores and reduce global contention.
  - Keep update schema contract (Cap'n Proto payload) stable unless explicitly versioned.
- Non-Goals:
  - Preserve Kafka transport compatibility in the new default architecture.
  - Introduce distributed multi-host RIB consistency in this change.
  - Replace the existing BMP decoding logic semantics unless required for runtime integration.

## Architecture Overview
1. **Ingest Workers (monoio, thread-per-core)**
   - One worker per CPU core.
   - Each worker binds listener sockets with `SO_REUSEPORT`.
   - Kernel distributes incoming BMP sessions to workers.
   - Session is handled entirely by owning worker.

2. **Zero-Copy Ingress**
   - Worker uses fixed-size reusable slot rings on the `io_uring`-backed runtime path.
   - Parser reads directly from those buffers before recycle.
   - Kernel-registered buffer-select (`ProvideBuffers`/`BUFFER_SELECT`) is tracked as an advanced follow-up once runtime support is available.

3. **Local Curation State (Sharded RIB)**
   - Each worker maintains its own RIB shard.
   - Deduplication and synthetic withdraw generation happen in worker-local memory.
   - No global mutex around all router/peer state.

4. **NATS Bridge and Sidecar**
   - Worker serializes update and pushes bytes into bounded lock-free MPSC channel.
   - Dedicated Tokio sidecar owns NATS JetStream client and publish pipeline.
   - Sidecar handles server ACK futures asynchronously to keep worker hot path non-blocking.
   - Bridge channel crate is selected by bakeoff (`kanal` vs `crossfire`) under Arancini load.

## Baseline Hot-Path Remediation (Phase 0)
Before or in parallel with full Arancini cutover, the implementation must remove current-path bottlenecks:

1. **Lock Scope Safety**
   - Never hold global/shard state locks across `.await`.
   - Gather emit sets while locked, drop lock, then perform channel/network I/O.
   - Apply this rule to normal update flow, peer-down synthetic withdraw flow, and peer-up recovery flow.

2. **State Persistence Lock Decoupling**
   - Snapshot/serialize state without holding locks across filesystem writes and `fsync`.
   - Do not stall update processing during durable write phase.

3. **BMP Framing and Buffer Reuse**
   - Replace peek-plus-allocate-per-message flow with framed parsing using reusable buffers (`BytesMut`-style framing).
   - Eliminate partial-header busy-loop behavior.

4. **Per-Update CPU/Allocation Reductions**
   - Replace string-based IPv4-to-IPv6 mapping with numeric conversion.
   - Remove avoidable `Update` cloning in emit path.
   - Introduce shared attribute ownership (`Arc`-backed structure) where many prefixes share attributes.

5. **Producer Throughput and Ordering Policy**
   - Remove constant partition key behavior that collapses all traffic to one partition in legacy Kafka mode.
   - Use explicit sharding/ordering strategy and bounded in-flight publish pipeline.

6. **State Hashing and Metrics Efficiency**
   - Evaluate faster hashers for trusted-input hot paths.
   - Reduce per-update label-string allocations and control cardinality in high-frequency metrics.

## Data Flow
1. BMP session accepted by worker core.
2. Packet read via `io_uring` fixed buffers.
3. Update decoded and curated against local shard.
4. Update serialized (Cap'n Proto direct-to-buffer path).
5. Serialized payload forwarded to sidecar via channel.
6. Sidecar publishes to JetStream subject:
   - `arancini.updates.<router_ip>.<peer_asn>.<afi_safi>`
7. ACK handled off hot path; failures produce retries/metrics.

## Backpressure Strategy
- Worker-to-sidecar channels are bounded.
- On channel pressure, worker records queue-depth and drop/backoff metrics according to configured policy.
- Sidecar publish queue and ACK lag metrics are exported for operational tuning.
- Legacy-path producer backpressure and in-flight limits remain instrumented until full JetStream migration is complete.

## State Persistence
- Each worker periodically snapshots its shard independently.
- Allowed targets:
  - NATS KV bucket (for simple control-plane integration)
  - Local NVMe-backed store (for lower latency persistence)
- Restore process reconstructs per-core shard ownership at startup.

## System and Socket Tuning
- Linux host sysctl profile is required for target throughput class:
  - `fs.file-max = 2097152`
  - `net.core.rmem_max = 33554432`
  - `net.core.wmem_max = 33554432`
  - `net.ipv4.tcp_rmem = 4096 87380 33554432`
  - `net.ipv4.tcp_wmem = 4096 65536 33554432`
  - `net.ipv4.tcp_fin_timeout = 15`
  - `net.core.netdev_max_backlog = 10000`
  - `net.core.somaxconn` and `net.ipv4.tcp_max_syn_backlog` are documented/tuned for listener burst handling.
- Required socket options in runtime:
  - `SO_REUSEPORT` on ingest listeners.
  - `TCP_NODELAY` on accepted sockets and publish-side sockets as applicable.
- Host readiness guidance also includes:
  - file descriptor limits (`ulimit -n`) aligned with collector connection scale
  - NIC queue/RSS/IRQ tuning guidance for high-packet-rate deployments

## Success Metrics by Phase
- Phase 0: hot-path remediations eliminate lock-across-await behavior and reduce avoidable alloc/CPU overhead in baseline flow.
- Phase 1: monoio listener with `SO_REUSEPORT`, target of zero avoidable context switches per packet on hot path.
- Phase 2: monoio-to-Tokio bridge can deliver updates reliably to JetStream.
- Phase 3: throughput scales approximately linearly as worker core count increases.
- Phase 4: direct-to-buffer serialization removes avoidable hot-path allocations.

## Risks and Mitigations
- Bridge throughput bottleneck:
  - Mitigate with bounded channels, queue-depth telemetry, and sidecar throughput profiling.
- ACK backlog under broker stress:
  - Mitigate with asynchronous ACK processing and retry/error policy.
- Ownership drift during reconnect storms:
  - Mitigate with strict session-to-worker invariants and ownership-focused tests.
