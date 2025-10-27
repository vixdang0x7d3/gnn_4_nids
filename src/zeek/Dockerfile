FROM zeek/zeek:latest

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    g++ \
    cmake \
    make \
    libpcap-dev \
    curl \
    ca-certificates \
    libsasl2-dev \
    libssl-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Install librdkafka
WORKDIR /tmp
RUN curl -L https://github.com/edenhill/librdkafka/archive/v1.4.4.tar.gz | tar xvz && \
    cd librdkafka-1.4.4/ && \
    ./configure --enable-sasl && \
    make && \
    make install && \
    ldconfig && \
    cd / && \
    rm -rf /tmp/librdkafka-1.4.4

# Install Zeek Kafka plugin using zkg
RUN zkg install seisollc/zeek-kafka --version v1.2.0 --force

# Verify plugin installation
RUN zeek -N Seiso:Kafka

# Set cwd back to default
WORKDIR /

# set default command
CMD ["zeek"]
