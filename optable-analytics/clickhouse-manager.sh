#!/bin/bash

# ClickHouse Manager Script

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Functions
print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

start_clickhouse() {
    print_status "Starting ClickHouse..."
    docker-compose -f docker-compose.clickhouse.yml up -d
    
    print_status "Waiting for ClickHouse to be ready..."
    sleep 5
    
    # Check if ClickHouse is running
    if docker ps | grep -q prebid-clickhouse; then
        print_status "ClickHouse is running!"
        print_status "HTTP interface: http://localhost:8123"
        print_status "Native interface: localhost:9000"
    else
        print_error "Failed to start ClickHouse"
        exit 1
    fi
}

stop_clickhouse() {
    print_status "Stopping ClickHouse..."
    docker-compose -f docker-compose.clickhouse.yml down
}


ingest_data() {
    print_status "Ingesting analytics data..."
    python3 clickhouse-ingest.py --file analytics-events.jsonl
}


cleanup() {
    print_status "Cleaning up ClickHouse data..."
    docker-compose -f docker-compose.clickhouse.yml down -v
    rm -rf clickhouse-data clickhouse-logs
    print_status "Cleanup complete"
}

# Main script
case "$1" in
    start)
        start_clickhouse
        ;;
    stop)
        stop_clickhouse
        ;;
    restart)
        stop_clickhouse
        start_clickhouse
        ;;
    ingest)
        ingest_data
        ;;
    cleanup)
        cleanup
        ;;
    all)
        start_clickhouse
        ingest_data
        ;;
    *)
        echo "Usage: $0 {start|stop|restart|ingest|cleanup|all}"
        echo ""
        echo "Commands:"
        echo "  start    - Start ClickHouse container"
        echo "  stop     - Stop ClickHouse container"
        echo "  restart  - Restart ClickHouse container"
        echo "  ingest   - Ingest analytics data from JSONL file"
        echo "  cleanup  - Remove all ClickHouse data and containers"
        echo "  all      - Start ClickHouse and ingest data"
        exit 1
        ;;
esac