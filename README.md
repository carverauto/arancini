<p align="center">
  <img src="https://nxthdr.dev/risotto/logo.png" height="256" width="256" alt="Project Logo" />
</p>

# Risotto

Risotto 😋 is a high-performance collector that processes BMP messages from routers and publishes updates to Kafka/Redpanda. This repository includes both the Risotto collector application and the Risotto library.

The collector application streams BGP updates to a Kafka topic, enabling downstream components to consume them. The library offers essential components for decoding BMP messages and generating BGP updates.

## Quick Start

The easiest way to use Risotto with Docker. This command will output the help message and exit:

```bash
docker run ghcr.io/nxthdr/risotto:main --help
```

To run Risotto with Docker with the default parameters, you can use the following command:

```bash
docker run \
  -p 4000:4000 \
  -p 8080:8080 \
  ghcr.io/nxthdr/risotto:main
```

By default, Risotto listens on port `4000` for BMP messages.
Additionally, a Prometheus HTTP endpoint is available at `http://localhost:8080/metrics` to monitor the collector's performance and statistics.

## Data Curation

Risotto maintains a state representing connected routers and their associated BGP peers and announced prefixes. This state is dumped to a file at specified intervals.
This state addresses two challenges when handling BMP data:
- **Duplicate announcements** from BMP session resets, where the router resends all active prefixes to the collector after a restart or connectivity issue.
- **Missing withdraws** that occur when a BGP session goes down and the router is implemented not to send the withdraws messages, or when the collector experiences downtime. These scenarios can result in stale or inaccurate BGP state in downstream systems.

Risotto checks each incoming update against its state. If the prefix is already present, the update is not sent downstream. Missing withdraws are generated synthetically when receiving Peer Down notifications, if the withdraws have not been sent by the router, by using the prefixes stored in the state for this router and downed peer.

When the collector restarts, Risotto infers any missing withdraws from the initial Peer Up sequence, ensuring the database remains accurate despite downtime. However, any announcements received after the last saved state may be replayed. In short, Risotto guarantees a consistent database state, though it may contain some duplicate announcements following a restart.

Conversely, Risotto can be configured to stream updates to the event pipeline as is by disabling the state usage.

## Decoupling Collection from Curation

The Risotto application couples collection (parsing BMP messages) and curation (deduplication, synthetic withdraws) together for simplicity. Of course, you can disable curation to use the app as a stateless collector.

Moreover, the library is designed to facilitate decoupling these two concerns, allowing you to design your data processing pipeline as needed. This enables you to build separate applications, a collector and a curator, using your own serialization format, state store implementation, and streaming platform.

## Contributing

Refer to the Docker Compose [integration](./integration/) tests to try Risotto locally. The setup includes BIRD and GoBGP routers announcing BGP updates between them, and transmitting BMP messages to Risotto.

## Linux Performance Validation

For Arancini deployments, run the tuning/socket validation checks:

```bash
bash integration/bench/run_2_6_linux_tuning_validation.sh
```

This validates:
- Runtime socket option enforcement in code (`SO_REUSEPORT`, `TCP_NODELAY`, backlog and `SO_RCVBUF` wiring).
- Linux host tuning guidance (`sysctl`, file descriptor limits, and RSS/IRQ readiness checks).

## Runtime Benchmark (Risotto vs Arancini)

Use this script to benchmark Arancini runtime throughput with producer paths disabled:

```bash
ROUTE_COUNT_PER_BIRD=2000 TARGET_RX_DELTA=2000 TIMEOUT_SECONDS=180 ARANCINI_WORKERS=4 \
  integration/bench/run_bird_saturating_runtime_benchmark.sh
```

The script writes the report to:

```bash
integration/bench/risotto-vs-arancini-bird-saturating-report.md
```

Latest run (`2026-02-18T21:47:19Z`) on `Linux 5.15.0-312.187.5.3.el9uek.x86_64`:

| Runtime | rx_update delta | elapsed sec | rx throughput (/s) | bmp_message delta | error count |
|---|---:|---:|---:|---:|---:|
| risotto | 4000 | 3.731 | 1072.10 | 4013 | 0 |
| arancini | 4000 | 2.213 | 1807.50 | 4013 | 0 |

Observed result for this saturating replay profile: `1.69x` higher `rx_update` throughput for Arancini.

## NATS JetStream mTLS

Arancini supports TLS and mutual TLS for the NATS JetStream sidecar path.

Example:

```bash
./target/release/risotto \
  --nats-enable \
  --nats-server nats://nats.example.net:4222 \
  --nats-tls-required \
  --nats-tls-ca-cert-path /etc/risotto/nats/ca.pem \
  --nats-tls-client-cert-path /etc/risotto/nats/client.pem \
  --nats-tls-client-key-path /etc/risotto/nats/client-key.pem
```

Optional:
- `--nats-tls-first` enables handshake-first mode when the NATS server is configured for it.
