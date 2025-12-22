# kafka_streaming.zeek
# Multi-log Kafka streaming integration
# Streams pkt-stats, conn, dns, ftp, http, and ssl logs to Kafka

@load packages/zeek-kafka
@load ./pkt_stats.zeek

# Kafka configuration from environment variables
global kafka_broker = getenv("KAFKA_BROKER") == "" ?
    "localhost:9092" : getenv("KAFKA_BROKER");
global kafka_topic = getenv("KAFKA_TOPIC") == "" ?
    "zeek.dpi" : getenv("KAFKA_TOPIC");

# Kafka client configuration
redef Kafka::tag_json = T;
redef Kafka::kafka_conf = table(
    ["metadata.broker.list"] = kafka_broker,
    ["client.id"] = "zeek-dpi-producer",
    # Performance tuning
    ["compression.type"] = "snappy",
    ["batch.num.messages"] = "100",
    ["queue.buffering.max.messages"] = "100000",
    ["queue.buffering.max.ms"] = "1000",
    # Increase max message size to 10MB
    ["message.max.bytes"] = "10485760"
);

redef Kafka::topic_name = kafka_topic;

# Initialize Kafka streaming for multiple log types
event zeek_init() &priority=-10
{
    print fmt("Initializing DPI Kafka producer...");
    print fmt("Broker: %s", kafka_broker);
    print fmt("Topic: %s", kafka_topic);

    # Custom pkt_stats log
    local pkt_stats_filter: Log::Filter = [
        $name = "kafka-pkt-stats",
        $writer = Log::WRITER_KAFKAWRITER,
        $config = table(["topic_name"] = kafka_topic)
    ];
    Log::add_filter(PKT_STATS::LOG, pkt_stats_filter);

    # Standard conn.log (main Zeek connection log)
    local conn_filter: Log::Filter = [
        $name = "kafka-conn",
        $writer = Log::WRITER_KAFKAWRITER,
        $config = table(["topic_name"] = kafka_topic)
    ];
    Log::add_filter(Conn::LOG, conn_filter);

    # Standard protocol logs
    local dns_filter: Log::Filter = [
        $name = "kafka-dns",
        $writer = Log::WRITER_KAFKAWRITER,
        $config = table(["topic_name"] = kafka_topic)
    ];
    Log::add_filter(DNS::LOG, dns_filter);

    local ftp_filter: Log::Filter = [
        $name = "kafka-ftp",
        $writer = Log::WRITER_KAFKAWRITER,
        $config = table(["topic_name"] = kafka_topic)
    ];
    Log::add_filter(FTP::LOG, ftp_filter);

    local http_filter: Log::Filter = [
        $name = "kafka-http",
        $writer = Log::WRITER_KAFKAWRITER,
        $config = table(["topic_name"] = kafka_topic)
    ];
    Log::add_filter(HTTP::LOG, http_filter);

    local ssl_filter: Log::Filter = [
        $name = "kafka-ssl",
        $writer = Log::WRITER_KAFKAWRITER,
        $config = table(["topic_name"] = kafka_topic)
    ];
    Log::add_filter(SSL::LOG, ssl_filter);

    print "DPI Kafka producer ready!";
    print "Streaming: pkt-stats, conn, dns, ftp, http, ssl";
}
