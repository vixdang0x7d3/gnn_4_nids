docker run \
    --rm \
    --name zeek-kafka-stream \
    --network host \
    --cap-add=NET_ADMIN \
    --cap-add=NET_RAW \
    -v $(pwd)/zeek_logs:/zeek_logs \
    -w /zeek_logs \
    zeek_streaming:latest \
    zeek -C -i wlo1 /usr/local/zeek/share/zeek/site/unsw-extra.zeek /usr/local/zeek/share/zeek/site/writekafka.zeek


