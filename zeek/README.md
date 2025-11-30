# Zeek UNSW-NB15 Feature Extraction + Kafka Streaming

Extract NF-UNSW-NB15-v2 compatible features from network traffic and stream to Kafka.

## Quick Test

```bash
cd /home/v/works/gnn_4_nids

# 1. Start Kafka
cd infrastructure && make up-base

# 2. Test Zeek → Kafka with PCAP
cd ../zeek
docker run --rm --network ml_platform \
  -v $(pwd):/scripts:ro \
  -v $(pwd)/../data_pipeline/data/pcaps:/pcaps:ro \
  -e ZEEK_KAFKA_BROKER=broker:29092 \
  -e ZEEK_KAFKA_TOPIC=zeek.logs \
  zeek_streaming:latest \
  zeek -C -r /pcaps/quickstart.pcap \
    /scripts/unsw-nb15-features.zeek \
    /scripts/unsw-nb15-kafka.zeek

# 3. Verify output in Kafka
cd ../infrastructure
docker exec -it broker kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic zeek.logs \
  --from-beginning --max-messages 5
```

## Live Capture

```bash
docker run -d --name zeek-producer \
  --network host --privileged \
  -v $(pwd):/scripts:ro \
  -e ZEEK_KAFKA_BROKER=localhost:9092 \
  -e ZEEK_KAFKA_TOPIC=zeek.logs \
  zeek_streaming:latest \
  zeek -C -i wlan0 /scripts/unsw-nb15-kafka.zeek
```

## Features: 34 fields including TCP flags, TTL, packet times/sizes, DNS, FTP, ICMP

See unsw-nb15-features.zeek for full list.
