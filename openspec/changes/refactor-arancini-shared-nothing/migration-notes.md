# Kafka To JetStream Migration Notes

## Scope
These notes cover downstream systems that currently consume Risotto Kafka topic payloads and will move to Arancini JetStream subjects.

## Payload Compatibility
- Cap'n Proto `Update` payload format is unchanged.
- Existing payload decoders can be reused without schema changes.

## Routing Model Change
- Legacy routing:
  - Single Kafka topic (for example `risotto-updates`) with partition-based ordering.
- Arancini routing:
  - Subject hierarchy:
    - `arancini.updates.<router_ip>.<peer_asn>.<afi_safi>`

## Ordering Semantics
- Legacy Kafka ordering was bounded by partition assignment.
- Arancini ordering is naturally scoped by subject stream sequence.
- Consumers that require strict ordering should process per subject key (or an explicit grouped wildcard) rather than across all updates globally.

## Subscription Translation
- Kafka topic subscription: `risotto-updates`
- JetStream subject options:
  - All updates: `arancini.updates.>`
  - Single router: `arancini.updates.<router_ip>.>`
  - Single router+peer: `arancini.updates.<router_ip>.<peer_asn>.>`
  - AFI/SAFI-specific: `arancini.updates.*.*.<afi_safi>`

## Consumer Acknowledgement
- Kafka consumer offsets map to JetStream consumer ACK state.
- Durable JetStream consumers should be used for at-least-once processing and restart continuity.

## Replay And Backfill
- Kafka offset replay maps to JetStream stream replay by consumer configuration.
- For targeted replay, use subject-filtered durable consumers.

## Operational Cutover Plan
1. Deploy JetStream stream and durable consumers alongside existing Kafka consumers.
2. Dual-read in validation environments and compare decoded update counts by router/peer windows.
3. Switch production readers to JetStream subjects.
4. Keep Kafka pipeline as temporary rollback path until JetStream SLOs are met.
5. Remove Kafka dependencies after rollback window expires.

## Metrics To Compare During Cutover
- Message ingest rate vs. publish enqueue rate.
- ACK lag and failure counters.
- Per-worker update throughput counters.
- End-to-end consumer lag in downstream processors.
