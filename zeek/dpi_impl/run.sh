#!/bin/bash
set -e

# Configuration
CONTAINER_NAME="zeek-dpi-stream"
IMAGE="zeek_streaming:latest"
NETWORK="ml_platform"
KAFKA_BROKER="${KAFKA_BROKER:-broker:29092}"
KAFKA_TOPIC="${KAFKA_TOPIC:-zeek.dpi}"
SCRIPTS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Print usage
print_usage() {
    echo "Zeek DPI Implementation - Docker Wrapper"
    echo ""
    echo "Usage: $0 <command> [options]"
    echo ""
    echo "Commands:"
    echo "  live <interface>     Start live network capture"
    echo "  pcap <file>          Process PCAP file"
    echo "  stop                 Stop running container"
    echo "  logs [-f]            View container logs"
    echo "  status               Show container status"
    echo "  help                 Show this help message"
    echo ""
    echo "Environment Variables:"
    echo "  KAFKA_BROKER         Kafka broker address (default: broker:29092)"
    echo "  KAFKA_TOPIC          Kafka topic name (default: zeek.dpi)"
    echo ""
    echo "Examples:"
    echo "  $0 live eth0"
    echo "  $0 pcap /path/to/capture.pcap"
    echo "  $0 logs -f"
    echo "  KAFKA_BROKER=localhost:9092 $0 pcap test.pcap"
}

# Check if Kafka is running
check_kafka() {
    echo -e "${YELLOW}Checking Kafka...${NC}"
    if ! docker ps | grep -q broker; then
        echo -e "${RED}Kafka broker not running!${NC}"
        echo "Start it with: cd ../../infrastructure && make up-base"
        exit 1
    fi
    echo -e "${GREEN}Kafka broker running${NC}"
}

# Stop and remove existing container
stop_container() {
    if docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
        echo -e "${YELLOW}Stopping existing container...${NC}"
        docker stop "$CONTAINER_NAME" >/dev/null 2>&1 || true
        docker rm "$CONTAINER_NAME" >/dev/null 2>&1 || true
        echo -e "${GREEN}Stopped${NC}"
    fi
}

# Show container logs
show_logs() {
    if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
        echo -e "${RED}Container not running${NC}"
        exit 1
    fi
    docker logs "$@" "$CONTAINER_NAME"
}

# Show container status
show_status() {
    echo "Container Status:"
    echo "----------------"
    if docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
        echo -e "Status: ${GREEN}RUNNING${NC}"
        echo ""
        docker ps --filter "name=${CONTAINER_NAME}" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
    else
        echo -e "Status: ${RED}NOT RUNNING${NC}"
    fi
}

# Run live capture
run_live() {
    local interface=$1

    if [ -z "$interface" ]; then
        echo -e "${RED}Error: Interface required${NC}"
        echo "Usage: $0 live <interface>"
        exit 1
    fi

    check_kafka
    stop_container

    echo -e "${GREEN}Starting Zeek DPI capture on $interface...${NC}"
    echo "Implementation: DPI (pkt-stats + protocol logs)"
    echo "Broker: $KAFKA_BROKER"
    echo "Topic: $KAFKA_TOPIC"
    echo "Container: $CONTAINER_NAME"
    echo ""

    docker run -d \
        --name "$CONTAINER_NAME" \
        --network host \
        --cap-add=NET_ADMIN \
        --cap-add=NET_RAW \
        -v "$SCRIPTS_DIR:/scripts:ro" \
        -v "$SCRIPTS_DIR/zeek_logs:/zeek_logs" \
        -w /zeek_logs \
        -e KAFKA_BROKER="$KAFKA_BROKER" \
        -e KAFKA_TOPIC="$KAFKA_TOPIC" \
        "$IMAGE" \
        zeek -C -i "$interface" \
            /scripts/pkt_stats.zeek \
            /scripts/kafka_streaming.zeek

    echo -e "${GREEN}Started!${NC}"
    echo ""
    echo "View logs: $0 logs -f"
    echo "Stop: $0 stop"
}

# Process PCAP file
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
    echo "Implementation: DPI (pkt-stats + protocol logs)"
    echo "Broker: $KAFKA_BROKER"
    echo "Topic: $KAFKA_TOPIC"
    echo ""

    docker run --rm \
        --name "${CONTAINER_NAME}-pcap" \
        --network "$NETWORK" \
        -v "$SCRIPTS_DIR:/scripts:ro" \
        -v "$pcap_dir:/pcaps:ro" \
        -e KAFKA_BROKER="$KAFKA_BROKER" \
        -e KAFKA_TOPIC="$KAFKA_TOPIC" \
        "$IMAGE" \
        zeek -C -r "/pcaps/$pcap_name" \
            /scripts/pkt_stats.zeek \
            /scripts/kafka_streaming.zeek

    echo -e "${GREEN}Done!${NC}"
}

# Main command router
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
