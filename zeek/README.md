# Zeek Network Traffic Analysis with Kafka Streaming

This setup captures network traffic with Zeek, extracts custom UNSW-NB15 features, and streams logs to both local files and Kafka.

## Features

- **Real-time packet capture** from network interface
- **Custom UNSW-NB15 features extraction**: TCP RTT, packet timing, TTL values, packet sizes
- **Dual output**: Local log files + Kafka streaming
- **Docker-based**: Isolated, reproducible environment

## Workflow
```
Network Traffic (wlo1)
        |
        v
Zeek (Docker Container)
├── unsw-extra.zeek (custom features)
├── writekafka.zeek (Kafka streaming)
        |
        v
Outputs:
├── Local Files: zeek_logs/*.log
└── Kafka: localhost:9092/zeek-logs
```

## Custom Features (unsw-extra.zeek)

The `unsw-extra.log` captures UNSW-NB15 dataset features:

| Field | Description |
|-------|-------------|
| `tcp_rtt` | TCP round-trip time estimate |
| `src_pkt_times` | Vector of source packet timestamps |
| `dst_pkt_times` | Vector of destination packet timestamps |
| `src_ttl` | Source TTL value |
| `dst_ttl` | Destination TTL value |
| `src_pkt_sizes` | Vector of source packet sizes |
| `dst_pkt_sizes` | Vector of destination packet sizes |
