##! UNSW-NB15 Feature Extraction with Kafka Output
##!
##! Combines feature extraction with streaming output to Kafka
##!
##! Usage:
##!   zeek -i wlan0 unsw-nb15-kafka.zeek
##!   zeek -r traffic.pcap unsw-nb15-kafka.zeek

@load packages/zeek-kafka
@load ./unsw-nb15-features.zeek

# Kafka configuration
# Override with: zeek -C kafka_broker=broker:29092 unsw-nb15-kafka.zeek
global kafka_broker = getenv("ZEEK_KAFKA_BROKER") == "" ? "localhost:9092" : getenv("ZEEK_KAFKA_BROKER");
global kafka_topic = getenv("ZEEK_KAFKA_TOPIC") == "" ? "zeek.logs" : getenv("ZEEK_KAFKA_TOPIC");

redef Kafka::tag_json = T;
redef Kafka::kafka_conf = table(
    ["metadata.broker.list"] = kafka_broker,
    ["client.id"] = "zeek-unsw-nb15-producer",
    # Performance tuning
    ["compression.type"] = "snappy",
    ["batch.num.messages"] = "100",
    ["queue.buffering.max.messages"] = "100000"
);

# Topic for UNSW-NB15 features
redef Kafka::topic_name = kafka_topic;

# Enable debug mode (comment out for production)
# redef Kafka::debug = "broker,topic,msg";

# Add Kafka writer to UNSW-NB15 feature log
event zeek_init() &priority=-10 {
    print fmt("Initializing UNSW-NB15 Kafka producer...");
    print fmt("Broker: %s", kafka_broker);
    print fmt("Topic: %s", kafka_topic);

    local unsw_nb15_filter: Log::Filter = [
        $name = "kafka-unsw-nb15",
        $writer = Log::WRITER_KAFKAWRITER,
        $config = table(
            ["topic_name"] = kafka_topic,
            ["tag_json"] = "T"
        )
    ];

    Log::add_filter(UNSW_NB15::LOG, unsw_nb15_filter);

    print " UNSW-NB15 Kafka producer ready!";
}

## Optional: Also write standard conn.log to Kafka for comparison
## event zeek_init() &priority=-11 {
##     local conn_filter: Log::Filter = [
##         $name = "kafka-conn",
##         $writer = Log::WRITER_KAFKAWRITER,
##         $config = table(
##             ["topic_name"] = "zeek.conn",
##             ["tag_json"] = "T"
##         )
##     ];
##
##     Log::add_filter(Conn::LOG, conn_filter);
## }
