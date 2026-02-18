# Project Context

## Purpose
Risotto is a high-performance BMP collector for BGP telemetry.

Primary goals:
- Accept BMP streams from routers (default `0.0.0.0:4000`)
- Decode BMP/BGP updates and normalize attributes
- Optionally curate stream state (deduplication + synthetic withdraw generation)
- Publish updates to Kafka-compatible brokers (Kafka/Redpanda)
- Expose Prometheus metrics (default `0.0.0.0:8080`)

This repository contains:
- `risotto`: collector binary application
- `risotto-lib`: reusable BMP processing and state-machine library

## Tech Stack
- Language: Rust (`edition = "2021"`)
- Runtime: Tokio async runtime
- CLI/config: `clap` + `clap-verbosity-flag`
- Parsing: `bgpkit-parser` (BMP/BGP decode)
- Streaming: `rdkafka` (Kafka/Redpanda producer)
- Serialization:
  - State snapshots: `postcard`
  - Update payload schema: Cap'n Proto (`arancini/schemas/update.capnp`)
- Observability:
  - Logging: `tracing` + `tracing-subscriber`
  - Metrics: `metrics` + `metrics-exporter-prometheus`
- Build/tooling:
  - Cargo workspace with crates `risotto` and `risotto-lib`
  - GitHub Actions CI (`cargo check --locked`, `cargo test --locked`)
  - Docker multi-stage builds using `cargo-chef`
  - Renovate for dependency update automation

## Project Conventions

### Code Style
- Use idiomatic Rust module boundaries (`mod ...`) with focused files by concern (`config`, `bmp`, `producer`, `state`, `serializer`).
- Prefer explicit typed config structs (`AppConfig`, `BMPConfig`, `KafkaConfig`, `CurationConfig`) over unstructured maps.
- Naming:
  - `snake_case` for functions/modules/fields
  - `PascalCase` for types/traits/enums
  - metric names prefixed with `risotto_`
- Error handling:
  - `anyhow::Result` for application/library boundaries
  - fail fast on invalid BMP framing/version/type and invalid producer auth mode
- Logging/metrics are first-class: meaningful `debug`/`trace` statements around message handling, state updates, and producer delivery.

### Architecture Patterns
- Pipeline shape:
  1. TCP BMP listener accepts router sessions
  2. BMP messages are parsed and converted into `Update` records
  3. Optional stateful curation deduplicates and synthesizes withdraws
  4. Kafka producer batches and emits serialized updates
- Concurrency model:
  - Tokio tasks per connection and per subsystem
  - MPSC channel between parser side and producer side
  - Graceful lifecycle via `tokio-graceful::Shutdown`
- State model:
  - `StateStore` trait abstraction in `risotto-lib`
  - Default in-memory implementation: `MemoryStore`
  - Periodic snapshot/restore via postcard file (`state.bin` by default)
- Serialization model:
  - Internal update representation in Rust structs (`risotto-lib::update::Update`)
  - Wire payloads encoded with Cap'n Proto schema from `arancini/schemas/update.capnp`

### Testing Strategy
- CI baseline:
  - `cargo check --locked`
  - `cargo test --locked --verbose`
- Library tests:
  - Async unit/integration-style tests in `arancini-lib/tests/`
  - Cover BMP processor paths such as peer up/down and route monitoring
- System/integration environment:
  - `integration/compose.yml` stands up GoBGP, BIRD, Risotto, Redpanda, and ClickHouse
  - Scenario scripts under `integration/tests/` mutate routing state and verify downstream effects

### Git Workflow
- CI runs on both `push` and `pull_request`.
- Container images are built/pushed in CI after tests succeed (amd64 + arm64 manifests).
- Dependency maintenance is Renovate-driven (`renovate.json`) with automerge for minor/patch/digest updates.
- No additional repository-enforced commit message convention is defined in-tree; keep commit messages clear and imperative.

## Domain Context
- Input protocol is BMP (BGP Monitoring Protocol) from routers.
- Output events represent parsed BGP updates plus metadata (`router`, `peer`, policy flags, attributes).
- Curation mode solves operational BMP realities:
  - suppress duplicate announcements (session resets/replays)
  - synthesize withdraws when peers disappear without explicit withdrawals
- On restart, loaded curation state helps reconstruct missing withdraw behavior; some announcement replay after last snapshot is expected.

## Important Constraints
- Preserve high-throughput async behavior; avoid blocking work in hot paths.
- Keep BMP parsing strict (version/type/length validation in stream handler).
- Maintain backward compatibility of serialized update payloads unless intentionally changing Cap'n Proto schema.
- Stateful curation must remain optional (`--curation-disable` path keeps stateless forwarding behavior).
- Operational defaults are significant:
  - BMP listener default: `0.0.0.0:4000`
  - Metrics endpoint default: `0.0.0.0:8080`
  - Default topic: `risotto-updates`

## External Dependencies
- Kafka-compatible broker for production usage (`Kafka` or `Redpanda`)
- Router BMP sources (e.g., GoBGP, BIRD, vendor routers)
- Prometheus scraper for `/metrics` endpoint
- Optional downstream consumers (integration setup uses ClickHouse Kafka engine)
- Container registry publishing via GitHub Container Registry (`ghcr.io`)
