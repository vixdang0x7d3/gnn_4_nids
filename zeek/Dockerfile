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
 RUN zeek -N Seiso::Kafka

 COPY unsw-extra.zeek /usr/local/zeek/share/zeek/site/
 COPY writekafka.zeek /usr/local/zeek/share/zeek/site/

# append to local.zeek
RUN echo '' >> /usr/local/zeek/share/zeek/site/local.zeek && \
    echo '# Custom UNSW feature extraction and Kafka logging' >> /usr/local/zeek/share/zeek/site/local.zeek && \
    echo '@load ./unsw-extra.zeek' >> /usr/local/zeek/share/zeek/site/local.zeek && \
    echo '@load ./writekafka.zeek' >> /usr/local/zeek/share/zeek/site/local.zeek

# Set cwd back to default
WORKDIR /

# set default command
CMD ["zeek"]
