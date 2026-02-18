## ADDED Requirements

### Requirement: Monoio Thread-Per-Core Runtime
The system SHALL run BMP ingestion on a monoio thread-per-core runtime rather than a shared Tokio work-stealing ingest executor.

#### Scenario: Worker startup model
- **WHEN** Arancini starts BMP ingestion
- **THEN** it creates one ingest worker per configured CPU core
- **AND** each worker owns its local ingest loop and session handling

### Requirement: io_uring Fixed-Buffer Ingestion
The system SHALL use `io_uring`-backed fixed-size reusable slots for BMP packet reads to minimize copy and allocation overhead.

#### Scenario: BMP packet read path
- **WHEN** a worker reads BMP packet data
- **THEN** bytes are read into reusable fixed slots
- **AND** parsing consumes those bytes before the slot is recycled

#### Scenario: Partial frame handling under load
- **WHEN** a BMP frame arrives in multiple TCP segments
- **THEN** the worker read loop buffers partial data without busy-spin behavior
- **AND** the frame is parsed once complete without allocating a fresh packet buffer per message

### Requirement: Deterministic Session Ownership
The system SHALL enforce deterministic ownership of each BMP session by a single worker core for the lifetime of that session.

#### Scenario: Session state mutation
- **WHEN** updates from a router session are processed
- **THEN** all curation mutations for that session are applied by the owning worker only
- **AND** no cross-worker lock is required for that session's RIB state

### Requirement: Required Socket Options for Ingest
The system SHALL set required high-performance socket options on ingest sockets.

#### Scenario: Listener configuration
- **WHEN** BMP listener sockets are created
- **THEN** `SO_REUSEPORT` is enabled so multiple worker listeners can share port 4000

#### Scenario: Session socket configuration
- **WHEN** a BMP TCP session is accepted
- **THEN** `TCP_NODELAY` is enabled for that socket
