# Logs Processing ETL Pipeline

ETL pipeline for processing Zeek network logs and transforming them to UNSW-NB15 compatible features for Graph Neural Network based Network Intrusion Detection System.

## Overview

This pipeline ingests real-time network traffic data from Zeek (via  Kafka streams), transforms it to match UNSW-NB15 dataset schema, and prepares graph-structured data for GNN model training and inference.

### Key features

- **Real-time Processing**: Consume Zeek logs from Kafka streams or local files
- **UNSW-NB15 Compatible**: Transform Zeek features to match UNSW-NB15 schema
-  **Efficent Processing**: DuckDB + Pyarrow for high-performance data transformations ( on single node setup :-) )


## Data Flow

### Input: Zeek Logs

The pipeline consumes two primary Zeek logs:

1. **conn.log**: Standard Zeek connection records
    - Duration, bytes, packets, protocol info
    - Connection states, history flags, etc.

2. **unsw-extra.log**: Custom UNSW-NB15 features
    - TCP RTT (Round-Trip Time)
    - Packet timestamps (for inter-packet time)
    - TTL values (source/destination Time-To-Live)
    - Packet sizes (for mean calculations)

### Transformation: UNSW-NB15 Feature Mapping

| UNSW-NB15 Feature | Source | Calculation |
|-------------------|--------|-------------|
| `dur` | conn.log | duration |
| `sbytes` | conn.log | orig_bytes |
| `dbytes` | conn.log | resp_bytes |
| `sttl` | unsw-extra.log | src_ttl |
| `dttl` | unsw-extra.log | dst_ttl |
| `sintpkt` | unsw-extra.log | mean(diff(src_pkt_times)) |
| `dintpkt` | unsw-extra.log | mean(diff(dst_pkt_times)) |
| `smean` | unsw-extra.log | mean(src_pkt_sizes) |
| `dmean` | unsw-extra.log | mean(dst_pkt_sizes) |
| `tcprtt` | unsw-extra.log | tcp_rtt |

### Kafka Integration

**Topic structure**: Mixed conn + unsw-extra records in JSON format

Example message:
```
{
  "conn": {
    "ts": 1763799918.631471,
    "uid": "CkMusy3Q1tKlesiki7",
    "id.orig_h": "192.168.1.12",
    "id.orig_p": 33298,
    "id.resp_h": "34.36.57.103",
    "id.resp_p": 443,
    "proto": "tcp",
    "duration": 0.033,
    "orig_bytes": 24,
    "resp_bytes": 1197
  }
  "unsw-extra": {...}
}
```

Perform transformations to get UNSW-NB15 compatible record:
```
┌────────────┬───────┬───────────────┬────────┬─────────┬─────────┬───┬────────────┬────────────┐
│   srcip    │ sport │     dstip     │ dsport │  proto  │  state  │ … │ ct_dst_ltm │ ct_src_ltm │
│  varchar   │ int64 │    varchar    │ int64  │ varchar │ varchar │   │   int64    │   int64    │
├────────────┼───────┼───────────────┼────────┼─────────┼─────────┼───┼────────────┼────────────┤
│ 59.166.0.0 │ 1390  │ 149.171.126.6 │   53   │ udp     │ con     │ … │     1      │     3      │
└────────────┴───────┴───────────────┴────────┴─────────┴─────────┴───┴────────────┴────────────┘
```

