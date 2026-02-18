## ADDED Requirements

### Requirement: Linux Host Tuning Profile
The system SHALL define and document the required Linux kernel/network tuning profile for carrier-grade deployment.

#### Scenario: Required sysctl profile
- **WHEN** operators prepare an Arancini host
- **THEN** the documented sysctl parameters include:
- **AND** `fs.file-max = 2097152`
- **AND** `net.core.somaxconn` is tuned for collector listener load
- **AND** `net.ipv4.tcp_max_syn_backlog` is tuned for connection burst handling
- **AND** `net.core.rmem_max = 33554432`
- **AND** `net.core.wmem_max = 33554432`
- **AND** `net.ipv4.tcp_rmem = 4096 87380 33554432`
- **AND** `net.ipv4.tcp_wmem = 4096 65536 33554432`
- **AND** `net.ipv4.tcp_fin_timeout = 15`
- **AND** `net.core.netdev_max_backlog = 10000`

### Requirement: Host Runtime Limits and NIC Guidance
The system SHALL document non-sysctl host readiness requirements for high-throughput operation.

#### Scenario: Host readiness checklist
- **WHEN** operators provision an Arancini node
- **THEN** the checklist includes file descriptor limits (`ulimit -n`) for expected session count
- **AND** includes NIC RSS/IRQ/queue tuning guidance for high packet rates

### Requirement: Runtime Socket Tuning Enforcement
The system SHALL configure runtime socket options required by the architecture.

#### Scenario: Ingest port sharing
- **WHEN** worker listeners bind to BMP port 4000
- **THEN** listeners use `SO_REUSEPORT`

#### Scenario: Low-latency TCP sessions
- **WHEN** BMP and publisher TCP sockets are established
- **THEN** `TCP_NODELAY` is enabled

#### Scenario: Receive buffer sizing
- **WHEN** operators configure high-burst BMP ingestion
- **THEN** ingest socket receive buffer sizing (`SO_RCVBUF`) is configurable and documented

#### Scenario: Listener backlog enforcement
- **WHEN** BMP listeners are initialized
- **THEN** runtime listener backlog is configurable and aligned with host backlog tuning

### Requirement: Performance Validation Gates
The system SHALL define and track phased performance goals for rollout readiness.

#### Scenario: Phase gate tracking
- **WHEN** implementation phases are executed
- **THEN** success metrics are recorded for:
- **AND** Phase 1 monoio listener behavior
- **AND** Phase 2 bridge-to-JetStream delivery
- **AND** Phase 3 multi-core scaling trend
- **AND** Phase 4 hot-path allocation reduction
