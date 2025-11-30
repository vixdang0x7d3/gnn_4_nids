#!/bin/bash
set -e

# Configuration
CONTAINER_NAME="zeek-kafka-producer"
IMAGE="zeek_streaming:latest"
NETWORK="ml_platform"
KAFKA_BROKER="broker:29092"
KAFKA_TOPIC="zeek.logs"
SCRIPTS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

print_usage() {
    cat << EOF
Usage: $0 <command> [options]

Commands:
    live <interface>     Start live packet capture on interface
    pcap <file>          Process PCAP file
    stop                 Stop running container
    logs                 Show container logs
    status               Show container status

Examples:
    $0 live wlan0                          # Capture from wlan0
    $0 pcap quickstart.pcap                # Process PCAP file
    $0 pcap ../data_pipeline/data/pcaps/capture.pcap
    $0 stop                                # Stop container
    $0 logs                                # View logs
    $0 logs -f                             # Follow logs

Environment variables:
    KAFKA_BROKER    Kafka broker address (default: localhost:9092)
    KAFKA_TOPIC     Kafka topic name (default: zeek-logs)
EOF
}

check_kafka() {
    echo -e "${YELLOW}Checking Kafka...${NC}"
    if ! docker ps | grep -q broker; then
        echo -e "${RED}Kafka broker not running!${NC}"
        echo "Start it with: cd ../infrastructure && make up-base"
        exit 1
    fi
    echo -e "${GREEN}Kafka broker running${NC}"
}

stop_container() {
    if docker ps -a | grep -q "$CONTAINER_NAME"; then
        echo -e "${YELLOW}Stopping $CONTAINER_NAME...${NC}"
        docker stop "$CONTAINER_NAME" 2>/dev/null || true
        docker rm "$CONTAINER_NAME" 2>/dev/null || true
        echo -e "${GREEN}Stopped${NC}"
    else
        echo -e "${YELLOW}Container not running${NC}"
    fi
}

show_logs() {
    if docker ps -a | grep -q "$CONTAINER_NAME"; then
        docker logs "$@" "$CONTAINER_NAME"
    else
        echo -e "${RED}Container not running${NC}"
        exit 1
    fi
}

show_status() {
    if docker ps | grep -q "$CONTAINER_NAME"; then
        echo -e "${GREEN}Running${NC}"
        docker ps --filter "name=$CONTAINER_NAME" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
    elif docker ps -a | grep -q "$CONTAINER_NAME"; then
        echo -e "${RED}Stopped${NC}"
        docker ps -a --filter "name=$CONTAINER_NAME" --format "table {{.Names}}\t{{.Status}}"
    else
        echo -e "${YELLOW}Not found${NC}"
    fi
}

run_live() {
    local interface=$1

    if [ -z "$interface" ]; then
        echo -e "${RED}Error: Interface required${NC}"
        echo "Usage: $0 live <interface>"
        exit 1
    fi

    check_kafka
    stop_container

    echo -e "${GREEN}Starting Zeek live capture on $interface...${NC}"
    echo "Broker: $KAFKA_BROKER"
    echo "Topic: $KAFKA_TOPIC"
    echo "Container: $CONTAINER_NAME"
    echo ""

    docker run -d \
        --name "$CONTAINER_NAME" \
        --network host \
        --privileged \
        -v "$SCRIPTS_DIR:/scripts:ro" \
        -e ZEEK_KAFKA_BROKER="$KAFKA_BROKER" \
        -e ZEEK_KAFKA_TOPIC="$KAFKA_TOPIC" \
        "$IMAGE" \
        zeek -C -i "$interface" \
            /scripts/unsw-nb15-features.zeek \
            /scripts/unsw-nb15-kafka.zeek

    echo -e "${GREEN}Started!${NC}"
    echo ""
    echo "View logs: $0 logs -f"
    echo "Stop: $0 stop"
}

run_pcap() {
    local pcap_file=$1

    if [ -z "$pcap_file" ]; then
        echo -e "${RED}Error: PCAP file required${NC}"
        echo "Usage: $0 pcap <file>"
        exit 1
    fi

    # Resolve absolute path
    if [[ ! "$pcap_file" = /* ]]; then
        pcap_file="$(pwd)/$pcap_file"
    fi

    if [ ! -f "$pcap_file" ]; then
        echo -e "${RED}Error: File not found: $pcap_file${NC}"
        exit 1
    fi

    local pcap_dir=$(dirname "$pcap_file")
    local pcap_name=$(basename "$pcap_file")

    check_kafka

    echo -e "${GREEN}Processing PCAP: $pcap_name${NC}"
    echo "Broker: $KAFKA_BROKER"
    echo "Topic: $KAFKA_TOPIC"
    echo ""

    docker run --rm \
        --name "${CONTAINER_NAME}-pcap" \
        --network "$NETWORK" \
        -v "$SCRIPTS_DIR:/scripts:ro" \
        -v "$pcap_dir:/pcaps:ro" \
        -e ZEEK_KAFKA_BROKER="$KAFKA_BROKER" \
        -e ZEEK_KAFKA_TOPIC="$KAFKA_TOPIC" \
        "$IMAGE" \
        zeek -C -r "/pcaps/$pcap_name" \
            /scripts/unsw-nb15-features.zeek \
            /scripts/unsw-nb15-kafka.zeek

    echo -e "${GREEN}Done!${NC}"
}

# Main
case "${1:-}" in
    live)
        run_live "$2"
        ;;
    pcap)
        run_pcap "$2"
        ;;
    stop)
        stop_container
        ;;
    logs)
        shift
        show_logs "$@"
        ;;
    status)
        show_status
        ;;
    -h|--help|help|"")
        print_usage
        ;;
    *)
        echo -e "${RED}Unknown command: $1${NC}"
        echo ""
        print_usage
        exit 1
        ;;
esac
