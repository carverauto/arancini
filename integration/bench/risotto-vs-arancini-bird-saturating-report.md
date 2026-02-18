# Risotto vs Arancini BIRD Saturating Runtime Report

- Generated: 2026-02-18T21:47:19Z
- Host: Linux oracle-test 5.15.0-312.187.5.3.el9uek.x86_64 #2 SMP Sun Sep 21 08:53:25 PDT 2025 x86_64 x86_64 x86_64 GNU/Linux
- Workload: 2000 static routes per BIRD peer, target rx delta 2000, timeout 180s
- Producer paths: disabled (--kafka-disable, NATS disabled)
- Curation: disabled (--curation-disable)

## Results

| Runtime | rx_update delta | elapsed sec | rx throughput (/s) | bmp_message delta | process_cpu_seconds delta | error count |
|---|---:|---:|---:|---:|---:|---:|
| risotto | 4000 | 3.731 | 1072.10 | 4013 | 0.000000 | 0 |
| arancini | 4000 | 2.213 | 1807.50 | 4013 | 0.000000 | 0 |

## Relative

- Arancini vs Risotto rx throughput: **1.69x**
