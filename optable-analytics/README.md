# Optable Analytics

ClickHouse analytics system for measuring Optable's impact on Prebid performance.

## Current Status
✅ **ClickHouse database running** on localhost:8123 (HTTP) and localhost:9000 (native)  
✅ **Real analytics data ingested** - Events from analytics-events.jsonl  
✅ **Schema with Optable enrichment detection** - bidId joins working correctly  

## File Structure

```
optable-analytics/
├── analytics-events.jsonl        # Your tracked analytics data
├── analytics-server.js           # Analytics collection server
├── clickhouse-ingest.py          # Data ingestion script  
├── clickhouse-init.sql           # Database schema definition
├── clickhouse-manager.sh         # Database management script
├── docker-compose.clickhouse.yml # ClickHouse container config
├── optable-queries.sql           # Complete analytical query library
├── requirements.txt              # Python dependencies
└── start-analytics-server.sh     # Analytics server startup script
```

## Database Schema

### Tables
- **`analytics_events`** - Raw JSONL events (bidRequested, bidResponse, bidWon, auctionInit)
- **`bid_requests_mv`** - Materialized view with `optable_enriched` boolean field
- **`bid_responses_mv`** - Materialized view with response data  
- **`bid_wins_mv`** - Materialized view with win data

### Key Join Logic
```sql
-- Correct bid ID mapping:
bid_requests_mv.bidId = bid_responses_mv.requestId = bid_wins_mv.requestId
```

### Optable Detection
The `optable_enriched` field is `1` when either:
- `ortb2.user.eids` contains `"optable.co"`
- `ortb2.user.data` contains `"optable.co"`

## Usage

### 1. Start/Stop ClickHouse
```bash
./clickhouse-manager.sh start
./clickhouse-manager.sh stop
./clickhouse-manager.sh status
```

### 2. Initialize Database
```bash
# Install Python dependencies first (if needed)
pip install -r requirements.txt

# Start ClickHouse and ingest data
./clickhouse-manager.sh start
./clickhouse-manager.sh ingest

# Or do both at once
./clickhouse-manager.sh all
```

### 3. Run Analytics Queries

#### ClickHouse SQL Client
```bash
# Interactive SQL client (direct docker command)
docker exec -it prebid-clickhouse clickhouse-client \
    --user prebid \
    --password prebid123 \
    --database prebid_analytics
```

#### HTTP Interface
```bash
# Query via HTTP
curl "http://localhost:8123" -d "SELECT count() FROM prebid_analytics.analytics_events"

# Web interface (browser)
# Open http://localhost:8123 in your browser
```

#### Using Query Library
All queries in `optable-queries.sql` are ready to use - copy and paste them into:
- The web interface at http://localhost:8123
- The SQL client above


## Key Analytical Queries

See `optable-queries.sql` for complete query library including:

1. **Response Rate Analysis** - Enriched vs non-enriched response rates
2. **Win Rate Analysis** - Enriched vs non-enriched win rates  
3. **CPM Analysis** - Average bid/win CPMs by enrichment status
4. **Latency Analysis** - Response time comparisons
5. **Time Series Analysis** - Performance over time
6. **Data Validation** - Check data quality and join integrity

## Analytics Server

Start the analytics collection server to receive new events:

```bash
./start-analytics-server.sh
```

This starts a server on `http://localhost:3030/analytics` that appends new events to `analytics-events.jsonl`.

## Access Methods

1. **SQL Client**: `./clickhouse-manager.sh client`
2. **HTTP API**: `http://localhost:8123`  
3. **Python**: Use `clickhouse-driver` library
4. **Management Script**: `./clickhouse-manager.sh [command]`

## Commands Reference

```bash
# Management
./clickhouse-manager.sh start     # Start ClickHouse
./clickhouse-manager.sh stop      # Stop ClickHouse
./clickhouse-manager.sh restart   # Restart ClickHouse

# Data Operations
./clickhouse-manager.sh ingest    # Ingest analytics data
./clickhouse-manager.sh all       # Start + ingest

# Cleanup
./clickhouse-manager.sh cleanup   # Remove all data and containers
```
