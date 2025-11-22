# Zeek Network Traffic Analysis with Kafka Streaming

This setup captures network traffic with Zeek, extracts custom UNSW-NB15 features, and streams logs to both local files and Kafka.

## Features

- **Real-time packet capture** from network interface
- **Custom UNSW-NB15 features extraction**: TCP RTT, packet timing, TTL values, packet sizes
- **Dual output**: Local log files + Kafka streaming
- **Docker-based**: Isolated, reproducible environment

## Quick Start

### 1. Start Kafka Infrastructure

```bash
cd ../data_pipeline
docker compose up -d
```

Verify Kafka is running:
```bash
docker ps | grep -E "(kafka|zookeeper)"
```

### 2. Build Zeek Docker Image

```bash
docker build -t zeek_streaming:latest .
```

### 3. Start Zeek

```bash
bash run.sh
```

Zeek will start capturing traffic from interface `wlo1` and writing logs to:
- **Local**: `./zeek_logs/`
- **Kafka**: topic `zeek-logs` at `localhost:9092`

## Verify Everything is Working

### Check Local Logs
```bash
ls -lah zeek_logs/
```

Expected files:
- `conn.log` - Connection records
- `unsw-extra.log` - Custom UNSW features (tcp_rtt, pkt_times, ttl, pkt_sizes)
- `dns.log`, `ssl.log`, `http.log`, etc.

### Check Kafka Messages
```bash
# Count messages
docker exec broker kafka-run-class kafka.tools.GetOffsetShell \
    --broker-list localhost:9092 --topic zeek-logs

# View sample messages
docker exec broker kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic zeek-logs \
    --from-beginning \
    --max-messages 5
```

## Architecture

```
Network Traffic (wlo1)
    ↓
Zeek (Docker Container)
    ├── unsw-extra.zeek (custom features)
    ├── writekafka.zeek (Kafka streaming)
    ↓
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

## File Overview

- **Dockerfile**: Builds Zeek image with Kafka plugin
- **run.sh**: Starts Zeek container with proper configuration
- **unsw-extra.zeek**: Custom script for UNSW-NB15 feature extraction
- **writekafka.zeek**: Kafka streaming configuration
- **zeek_logs/**: Directory for local log output

## Stopping

```bash
# Stop Zeek
docker stop zeek-kafka-stream

# Stop Kafka (optional)
cd ../data_pipeline && docker compose down
```

## Troubleshooting

See [TROUBLESHOOTING.md](./TROUBLESHOOTING.md) for detailed issue resolution.

### Common Issues

**No logs appearing?**
- Check container is running: `docker ps | grep zeek`
- Check container logs: `docker logs zeek-kafka-stream`

**No unsw-extra.log?**
- Verify scripts are loaded explicitly on command line (see `run.sh`)
- Check for errors: `cat zeek_logs/reporter.log`

**Kafka not receiving messages?**
- Ensure Kafka is running: `docker ps | grep broker`
- Check connectivity: `docker exec zeek-kafka-stream bash -c "timeout 2 bash -c '</dev/tcp/localhost/9092'"`
- Enable debug mode in `writekafka.zeek`: `redef Kafka::debug = "all";`

## Configuration

### Change Network Interface

Edit `run.sh` and change `-i wlo1` to your interface:
```bash
zeek -C -i eth0 ...  # Example for eth0
```

Find your interfaces: `ip addr show`

### Change Kafka Broker

Edit `writekafka.zeek`:
```zeek
redef Kafka::kafka_conf = table(
    ["metadata.broker.list"] = "your-broker:9092"
);
```

### Change Kafka Topic

Edit `writekafka.zeek`:
```zeek
redef Kafka::topic_name = "your-topic-name";
```

## Performance Considerations

- **Packet drops**: Monitor `packet_filter.log` for dropped packets
- **Disk space**: Local logs can grow large on high-traffic networks
- **Memory**: Zeek tracks connection state; monitor container memory usage
- **Kafka throughput**: High traffic may require Kafka tuning

## Development

### Test Script Changes

After modifying `.zeek` files:
```bash
# Rebuild image
docker build -t zeek_streaming:latest .

# Restart container
docker stop zeek-kafka-stream
bash run.sh
```

### Debug Mode

Add debug output to scripts:
```zeek
event zeek_init() {
    print "Script loaded!";
}

event new_connection(c: connection) {
    print fmt("New connection: %s", c$uid);
}
```

View output:
```bash
docker logs -f zeek-kafka-stream
```

## References

- [Zeek Documentation](https://docs.zeek.org/)
- [Zeek Kafka Plugin](https://github.com/SeisoLLC/zeek-kafka)
- [UNSW-NB15 Dataset](https://research.unsw.edu.au/projects/unsw-nb15-dataset)
