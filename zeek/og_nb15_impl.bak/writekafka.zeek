@load packages/zeek-kafka
@load ./unsw-extra.zeek

# Kafka configuration (support environment variables)
global kafka_broker = getenv("KAFKA_BROKER") == "" ? "localhost:9092" : getenv("KAFKA_BROKER");
global kafka_topic = getenv("KAFKA_TOPIC") == "" ? "zeek-logs" : getenv("KAFKA_TOPIC");

redef Kafka::tag_json = T;
redef Kafka::kafka_conf = table(
    ["metadata.broker.list"] = kafka_broker,
    ["client.id"] = "zeek-original-producer"
);

# Default topic
redef Kafka::topic_name = kafka_topic;

# Enable debug mode (optional - comment out for production)
# redef Kafka::debug = "all";

# Add Kafka writer to conn.log and unsw-extra.log
event zeek_init() &priority=-10 {
    print fmt("Initializing Zeek Kafka producer (original implementation)...");
    print fmt("Broker: %s", kafka_broker);
    print fmt("Topic: %s", kafka_topic);

    local conn_filter: Log::Filter = [
        $name = "kafka-conn",
        $writer = Log::WRITER_KAFKAWRITER,
        $config = table(["topic_name"] = kafka_topic)
    ];
    Log::add_filter(Conn::LOG, conn_filter);

    local unsw_filter: Log::Filter = [
        $name = "kafka-unsw",
        $writer = Log::WRITER_KAFKAWRITER,
        $config = table(["topic_name"] = kafka_topic)
    ];
    Log::add_filter(UNSW::LOG, unsw_filter);

    print "Kafka producer ready!";
}

