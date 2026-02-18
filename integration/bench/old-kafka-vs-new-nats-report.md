# Old Kafka vs New NATS/JetStream Report

- Generated: 2026-02-18T20:56:30Z
- Host: Linux oracle-test 5.15.0-312.187.5.3.el9uek.x86_64 #2 SMP Sun Sep 21 08:53:25 PDT 2025 x86_64 x86_64 x86_64 GNU/Linux
- Workload: 100 routes per router preload + 2 reconnect cycles with 50 fresh routes per router/cycle (2s sleep) + 5s settle.

## Results

| Profile | signal metric | signal delta | signal rate (/s) | rx_update delta | bmp_message delta | kafka_messages delta | nats_publish_enqueued delta | nats_ack_success delta | error count |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| old-kafka | kafka_messages | 0 | 0.00 | 0 | 10 | 0 | 0 | 0 | 0 |
| new-nats | nats_publish_enqueued | 0 | 0.00 | 0 | 10 | 0 | 0 | 0 | 0 |

## Shared Comparator

| Profile | bmp_message rate (/s) |
|---|---:|
| old-kafka | 0.22 |
| new-nats | 0.22 |

## Relative Throughput

- signal-rate (transport specific): **n/a**
- bmp-rate (shared fallback): **1.00x**
- effective comparator used: **bmp-rate-fallback** => **1.00x**
