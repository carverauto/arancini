## ADDED Requirements

### Requirement: No Await While Holding Curation State Locks
The system SHALL NOT perform asynchronous waits while holding mutable curation state locks in update, peer-down, peer-up recovery, or snapshot preparation paths.

#### Scenario: Stateful update emission
- **WHEN** deduplicated updates are selected for downstream emission
- **THEN** lock-protected state mutation completes before any downstream send await occurs

#### Scenario: Synthetic withdraw emission
- **WHEN** synthetic withdraw updates are generated from state
- **THEN** state lock scope is exited before updates are sent to downstream channels

### Requirement: Snapshot I/O Outside Lock Scope
The system SHALL avoid holding state locks during filesystem durability operations.

#### Scenario: Periodic state dump
- **WHEN** a periodic snapshot is persisted
- **THEN** serialization snapshot creation may occur under lock
- **AND** file write and `fsync` are executed after lock release

### Requirement: Efficient BMP Framing
The system SHALL use reusable framed buffering for BMP input rather than per-message allocation and header peek loops.

#### Scenario: High-rate message ingestion
- **WHEN** BMP packets are read from a TCP stream
- **THEN** framing reuses a growable buffer
- **AND** partial-header conditions do not trigger busy-spin loops

### Requirement: Efficient IP Mapping in Decode Path
The system SHALL map IPv4 addresses to IPv6-mapped addresses using numeric conversion without string formatting/parsing in hot decode paths.

#### Scenario: Address normalization
- **WHEN** router, peer, prefix, or next-hop addresses are normalized
- **THEN** conversion uses direct numeric address operations

### Requirement: Producer Throughput and Partition Strategy
Until full JetStream migration is complete, the legacy producer path SHALL avoid single-partition collapse and support bounded in-flight publishing.

#### Scenario: Legacy transport batching
- **WHEN** legacy producer transport is active
- **THEN** partition selection does not force all records to one partition via a constant key
- **AND** producer path allows more than one in-flight batch with bounded backpressure controls

### Requirement: Allocation and Cardinality Control in Hot Path
The system SHALL reduce avoidable clone/allocation overhead and constrain metrics overhead in high-frequency paths.

#### Scenario: Multi-prefix update decoding
- **WHEN** many NLRI share the same attributes in one BGP update
- **THEN** shared attribute ownership is used where feasible to reduce repeated deep copies

#### Scenario: High-frequency metrics
- **WHEN** per-update metrics are recorded
- **THEN** implementation avoids avoidable per-event string allocations and documents label-cardinality limits
