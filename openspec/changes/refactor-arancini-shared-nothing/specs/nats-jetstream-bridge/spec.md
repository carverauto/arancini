## ADDED Requirements

### Requirement: Monoio-to-Tokio Bridge
The system SHALL bridge worker output from monoio ingest threads to a dedicated Tokio NATS sidecar using a bounded lock-free `crossfire` MPSC channel.

#### Scenario: Update handoff to sidecar
- **WHEN** a worker emits a curated update
- **THEN** the serialized update is enqueued to the sidecar channel without awaiting broker I/O

### Requirement: JetStream Async Publish
The system SHALL publish updates to NATS JetStream using async publish operations and process server ACKs outside the worker hot path.

#### Scenario: Non-blocking publish behavior
- **WHEN** the sidecar publishes a message
- **THEN** the worker that produced the message is not blocked waiting for JetStream ACK completion
- **AND** ACK success/failure is handled asynchronously by the sidecar

### Requirement: Subject Routing Schema
The system SHALL publish updates using the subject pattern `arancini.updates.<router_ip>.<peer_asn>.<afi_safi>`.

#### Scenario: Subject derivation
- **WHEN** an update is published
- **THEN** its router, peer ASN, and AFI/SAFI fields are encoded into the subject path components

### Requirement: Bridge Backpressure Visibility
The system SHALL expose observability for channel pressure and publish latency.

#### Scenario: Channel pressure
- **WHEN** the bridge queue approaches capacity
- **THEN** queue depth and pressure metrics are emitted for operators
