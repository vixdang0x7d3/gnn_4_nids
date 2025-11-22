# Zeek Setup Troubleshooting Guide

This document details the issues encountered during Zeek setup and their solutions.

---

## Issue 1: Zeek Container Exiting Immediately

### Problem
Zeek container started but immediately exited with only version output:
```
zeek version 8.0.3
```

### Root Cause
The `-v` flag in the zeek command was being interpreted as "print version and exit" instead of "verbose mode".

### Solution
Remove the `-v` flag from the zeek command in `run.sh`:

**Before:**
```bash
zeek -v -C -i wlo1
```

**After:**
```bash
zeek -C -i wlo1
```

---

## Issue 2: Logs Written to Wrong Directory

### Problem
Zeek was running but no logs appeared in the mounted `zeek_logs/` directory. Logs were being written to the container's root directory (`/`) instead.

### Root Cause
The Docker container's working directory was `/` by default, and Zeek writes logs to its current working directory.

### Solution
Added `-w /zeek_logs` flag to set the working directory in the Docker run command:

**Updated run.sh:**
```bash
docker run \
    --rm \
    --name zeek-kafka-stream \
    --network host \
    --cap-add=NET_ADMIN \
    --cap-add=NET_RAW \
    -v $(pwd)/zeek_logs:/zeek_logs \
    -w /zeek_logs \
    zeek_streaming:latest \
    zeek -C -i wlo1
```

---

## Issue 3: No unsw-extra.log Being Created

### Problem
Standard Zeek logs (conn.log, dns.log, ssl.log) were being created, but the custom `unsw-extra.log` was not appearing.

### Root Cause
The `unsw-extra.zeek` script was not being loaded properly when sourced through `local.zeek`. The script loading chain was:
- `local.zeek` → `writekafka.zeek` → `unsw-extra.zeek`

This indirect loading caused the script events (`new_connection`, `new_packet`, `connection_state_remove`) to not fire.

### Solution
Explicitly load both scripts on the Zeek command line instead of relying on `local.zeek`:

**Updated run.sh:**
```bash
docker run \
    --rm \
    --name zeek-kafka-stream \
    --network host \
    --cap-add=NET_ADMIN \
    --cap-add=NET_RAW \
    -v $(pwd)/zeek_logs:/zeek_logs \
    -w /zeek_logs \
    zeek_streaming:latest \
    zeek -C -i wlo1 \
        /usr/local/zeek/share/zeek/site/unsw-extra.zeek \
        /usr/local/zeek/share/zeek/site/writekafka.zeek
```

**Verification:**
Added debug print statements to `unsw-extra.zeek` to confirm loading:
```zeek
event zeek_init() {
    Log::create_stream(UNSW::LOG, [$columns=Info, $ev=unsw_extra, $path="unsw-extra"]);
    print "UNSW module loaded";  # Debug output
}

event new_connection(c: connection) {
    print fmt("new_connection fired for %s", c$uid);  # Debug output
    # ... rest of event handler
}
```

---

## Issue 4: Kafka Not Receiving Messages

### Problem
- Kafka broker was running at `localhost:9092`
- The `zeek-logs` topic was created
- But topic had 0 messages despite Zeek running and capturing traffic

### Root Cause 1: Kafka Local Logging Disabled
The Zeek Kafka plugin by default disables local ASCII log writing when enabled. We needed both local logs AND Kafka streaming.

**Solution:**
Added `redef Kafka::disable_default_logs = F;` to `writekafka.zeek` to keep local logging enabled.

### Root Cause 2: Incorrect Kafka Writer Configuration
The initial configuration used `Kafka::logs_to_send` which is deprecated or doesn't work with zeek-kafka plugin v1.2.0. The correct approach is to explicitly add Kafka writers to log streams.

**Solution:**
Updated `writekafka.zeek` to use `Log::add_filter` with `Log::WRITER_KAFKAWRITER`:

```zeek
@load packages/zeek-kafka
@load ./unsw-extra.zeek

# Kafka configuration
redef Kafka::tag_json = T;
redef Kafka::kafka_conf = table(
    ["metadata.broker.list"] = "localhost:9092"
);

# Default topic
redef Kafka::topic_name = "zeek-logs";

# Enable debug mode (optional, for troubleshooting)
redef Kafka::debug = "all";

# Add Kafka writer to conn.log
event zeek_init() &priority=-10 {
    local conn_filter: Log::Filter = [
        $name = "kafka-conn",
        $writer = Log::WRITER_KAFKAWRITER,
        $config = table(["topic_name"] = "zeek-logs")
    ];
    Log::add_filter(Conn::LOG, conn_filter);

    local unsw_filter: Log::Filter = [
        $name = "kafka-unsw",
        $writer = Log::WRITER_KAFKAWRITER,
        $config = table(["topic_name"] = "zeek-logs")
    ];
    Log::add_filter(UNSW::LOG, unsw_filter);
}
```

### Root Cause 3: Scripts Not Being Loaded
This was the same issue as Issue #3 - the scripts weren't being loaded properly through `local.zeek`, so the Kafka writer configuration never executed.

**Solution:**
Same as Issue #3 - explicitly load scripts on command line.

---

## Issue 5: Kafka Infrastructure Not Running

### Problem
Zeek was configured to send logs to Kafka at `localhost:9092`, but Kafka wasn't running.

### Solution
Start Kafka infrastructure using docker-compose:

```bash
cd data_pipeline
docker compose up -d
```

**Verification:**
```bash
# Check Kafka is running
docker ps | grep -E "(kafka|zookeeper)"

# Check Kafka topics
docker exec broker kafka-topics --list --bootstrap-server localhost:9092

# Check message count
docker exec broker kafka-run-class kafka.tools.GetOffsetShell \
    --broker-list localhost:9092 --topic zeek-logs
```

---

## Final Working Configuration

### File Structure
```
zeek/
├── Dockerfile
├── run.sh
├── unsw-extra.zeek
├── writekafka.zeek
└── zeek_logs/
    ├── conn.log
    ├── unsw-extra.log
    ├── dns.log
    └── ...
```

### run.sh (Final Version)
```bash
docker run \
    --rm \
    --name zeek-kafka-stream \
    --network host \
    --cap-add=NET_ADMIN \
    --cap-add=NET_RAW \
    -v $(pwd)/zeek_logs:/zeek_logs \
    -w /zeek_logs \
    zeek_streaming:latest \
    zeek -C -i wlo1 \
        /usr/local/zeek/share/zeek/site/unsw-extra.zeek \
        /usr/local/zeek/share/zeek/site/writekafka.zeek
```

### writekafka.zeek (Final Version)
```zeek
@load packages/zeek-kafka
@load ./unsw-extra.zeek

# Kafka configuration
redef Kafka::tag_json = T;
redef Kafka::kafka_conf = table(
    ["metadata.broker.list"] = "localhost:9092"
);

# Default topic
redef Kafka::topic_name = "zeek-logs";

# Enable debug mode (optional)
redef Kafka::debug = "all";

# Add Kafka writer to conn.log and unsw-extra.log
event zeek_init() &priority=-10 {
    local conn_filter: Log::Filter = [
        $name = "kafka-conn",
        $writer = Log::WRITER_KAFKAWRITER,
        $config = table(["topic_name"] = "zeek-logs")
    ];
    Log::add_filter(Conn::LOG, conn_filter);

    local unsw_filter: Log::Filter = [
        $name = "kafka-unsw",
        $writer = Log::WRITER_KAFKAWRITER,
        $config = table(["topic_name"] = "zeek-logs")
    ];
    Log::add_filter(UNSW::LOG, unsw_filter);
}
```

### unsw-extra.zeek
No changes needed - the script was correct from the start.

---

## Verification Steps

### 1. Check Zeek Container is Running
```bash
docker ps | grep zeek
```

### 2. Check Local Logs Are Being Written
```bash
ls -lah zeek/zeek_logs/
# Should see: conn.log, unsw-extra.log, dns.log, ssl.log, etc.
```

### 3. Verify unsw-extra.log Contents
```bash
head -20 zeek/zeek_logs/unsw-extra.log
# Should show fields: ts, uid, id.orig_h, tcp_rtt, src_pkt_times,
# dst_pkt_times, src_ttl, dst_ttl, src_pkt_sizes, dst_pkt_sizes
```

### 4. Check Kafka Message Count
```bash
docker exec broker kafka-run-class kafka.tools.GetOffsetShell \
    --broker-list localhost:9092 --topic zeek-logs
# Should show: zeek-logs:0:N (where N > 0)
```

### 5. Sample Kafka Messages
```bash
docker exec broker kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic zeek-logs \
    --from-beginning \
    --max-messages 5
```

Expected output includes both conn and unsw-extra messages in JSON format:
```json
{"conn": {"ts":...,"uid":"...","id.orig_h":"192.168.1.12",...}}
{"unsw-extra": {"ts":...,"uid":"...","src_pkt_times":[...],"dst_pkt_times":[...],...}}
```

---

## Key Takeaways

1. **Always explicitly load Zeek scripts** on the command line for custom scripts that rely on packet-level events (`new_packet`, `new_connection`)

2. **Set working directory** when running Zeek in Docker to control where logs are written

3. **Use Log::add_filter with WRITER_KAFKAWRITER** for zeek-kafka plugin v1.2.0, not the deprecated `Kafka::logs_to_send`

4. **Test scripts incrementally** - add debug print statements to verify events are firing

5. **Check Kafka connectivity** from within the Zeek container using `--network host` mode

---

## Common Commands

### Start Everything
```bash
# Start Kafka infrastructure
cd data_pipeline && docker compose up -d

# Build Zeek image (if modified)
docker build -t zeek_streaming:latest zeek/

# Start Zeek
cd zeek && bash run.sh
```

### Monitor Logs
```bash
# Watch local logs grow
watch -n 2 'ls -lh zeek/zeek_logs/*.log'

# Tail unsw-extra.log
tail -f zeek/zeek_logs/unsw-extra.log

# Check Zeek container logs
docker logs -f zeek-kafka-stream
```

### Stop Everything
```bash
# Stop Zeek
docker stop zeek-kafka-stream

# Stop Kafka
cd data_pipeline && docker compose down
```

### Debug
```bash
# Check if Kafka is reachable from Zeek container
docker exec zeek-kafka-stream bash -c "timeout 2 bash -c '</dev/tcp/localhost/9092' && echo 'Connected'"

# Test Zeek script syntax
docker exec zeek-kafka-stream zeek -C /usr/local/zeek/share/zeek/site/unsw-extra.zeek

# Check loaded plugins
docker exec zeek-kafka-stream zeek -NN | grep -i kafka
```
