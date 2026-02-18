## ADDED Requirements

### Requirement: Shared-Nothing Sharded RIB
The system SHALL maintain curation state as worker-local shards instead of a single global mutex-protected store.

#### Scenario: Local shard mutation
- **WHEN** a worker processes an update from an owned session
- **THEN** deduplication and state mutation are applied to that worker's shard only

### Requirement: Per-Shard Deduplication Semantics
The system SHALL preserve deduplication and synthetic-withdraw semantics within each shard for owned sessions.

#### Scenario: Duplicate announcement in owned shard
- **WHEN** an already-present prefix announcement is received for a worker-owned session
- **THEN** the update is suppressed according to curation rules

#### Scenario: Missing withdraw synthesis
- **WHEN** a worker detects peer/session-down for an owned session
- **THEN** synthetic withdraw updates are generated from that shard's active prefixes

### Requirement: Per-Shard Snapshot and Restore
The system SHALL persist and restore each shard independently to support restart recovery.

#### Scenario: Periodic persistence
- **WHEN** the snapshot interval elapses
- **THEN** each worker writes a snapshot of its local shard to configured backing storage (NATS KV or NVMe store)

#### Scenario: Startup recovery
- **WHEN** Arancini starts with existing snapshots
- **THEN** worker shards are restored from persisted state before steady-state processing begins
