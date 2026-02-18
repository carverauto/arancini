# Risotto vs Arancini Runtime Benchmark Report

- Generated: 2026-02-18T21:14:39Z
- Host: Linux oracle-test 5.15.0-312.187.5.3.el9uek.x86_64 #2 SMP Sun Sep 21 08:53:25 PDT 2025 x86_64 x86_64 x86_64 GNU/Linux
- Workload: 100 routes/router preload + 2 reconnect cycles with 50 fresh routes/router/cycle (2s sleep) + 5s settle.
- Producer paths: disabled (--kafka-disable, NATS disabled)
- Curation: disabled (--curation-disable)

## Results

| Runtime | bmp_message delta | bmp_message rate (/s) | rx_update delta | tx_update delta | process_cpu_seconds delta | cpu sec / 1k bmp | error count |
|---|---:|---:|---:|---:|---:|---:|---:|
| risotto | 10 | 0.22 | 0 | 0 | 0.000000 | 0.0000 | 0 |
| arancini | 10 | 0.22 | 0 | 0 | 0.000000 | 0.0000 | 0 |

## Relative

- Arancini vs Risotto BMP throughput: **1.00x**
- Arancini vs Risotto CPU efficiency (lower is better): **n/a**
