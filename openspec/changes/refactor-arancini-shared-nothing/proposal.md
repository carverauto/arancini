# Change: Refactor Risotto into Project Arancini Shared-Nothing Engine

## Why
Risotto's current architecture (Tokio work-stealing runtime, global state lock points, Kafka producer path) is not optimized for carrier-grade BMP ingestion and curation at 1M+ updates/second. The target system needs deterministic CPU ownership, lower syscall overhead, and a messaging path that does not block parsing.

Recent hot-path analysis also identified immediate throughput collapse risks in the current implementation (awaits while holding global state lock, expensive IP conversion in decode path, per-message BMP allocations/peek loops, and producer partitioning/ack pipeline limits). These issues must be explicitly addressed as part of the refactor plan so we do not carry bottlenecks into Arancini.

## What Changes
- **BREAKING** Replace BMP ingest runtime from Tokio task scheduling to `monoio` thread-per-core workers backed by `io_uring`.
- **BREAKING** Replace Kafka producer integration with NATS JetStream publishing.
- Add a lock-free bridge from monoio worker threads to a dedicated Tokio-based NATS sidecar thread using bounded `kanal` MPSC channels.
- Implement zero-copy BMP ingress using fixed registered buffers (`FixedSizeSlot`) and parse handoff without userspace copy.
- Enforce session ownership by core so each router session's RIB mutations are local to one worker.
- Replace global curation lock model with sharded per-core RIB state and local dedup/synthetic-withdraw logic.
- Add periodic per-core snapshot persistence (NATS KV or local NVMe backing store).
- Add Cap'n Proto direct-to-buffer serialization targets for allocation-free hot-path operation.
- Add host/kernel tuning requirements and required socket options for high-throughput operation.
- Add transitional hot-path remediations in current code paths while the Arancini runtime/messaging migration is in progress.

## Impact
- Affected specs:
  - `bmp-ingest-runtime`
  - `nats-jetstream-bridge`
  - `sharded-curation-store`
  - `deployment-performance-tuning`
  - `hot-path-hardening`
- Affected code (expected):
  - `risotto/src/main.rs`
  - `risotto/src/bmp.rs`
  - `risotto/src/producer.rs` (replacement with NATS bridge/publisher path)
  - `risotto/src/config.rs`
  - `risotto/src/serializer.rs`
  - `risotto-lib/src/state.rs`
  - `risotto-lib/src/state_store/*` (sharding changes)
  - new runtime/bridge modules for monoio workers and Tokio sidecar
- Operational impact:
  - Linux host tuning is now mandatory for target performance profile.
  - Deployment must include reachable NATS JetStream and updated subject routing policies.

## Rollout Strategy
1. Execute hot-path safety fixes first (lock-scope, framing/allocation, conversion and producer strategy).
2. Implement monoio listener and worker ownership model with `SO_REUSEPORT`.
3. Deliver monoio-to-Tokio bridge and JetStream async publish path.
4. Introduce sharded state store and remove global lock bottlenecks.
5. Optimize serialization to direct-to-buffer path and verify allocation reduction.
6. Validate linear throughput scaling by core count and promote to default runtime.
