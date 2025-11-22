@load packages/zeek-kafka
@load ./unsw-extra.zeek

# Kafka configuration
redef Kafka::tag_json = T;
redef Kafka::kafka_conf = table(
    ["metadata.broker.list"] = "localhost:9092"
);

# Default topic
redef Kafka::topic_name = "zeek-logs";

# Enable debug mode (optional - comment out for production)
# redef Kafka::debug = "all";

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

