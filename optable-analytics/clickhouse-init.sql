-- Create database if not exists
CREATE DATABASE IF NOT EXISTS prebid_analytics;

USE prebid_analytics;

-- Create tables and views if they don't exist (idempotent setup)
-- Note: Using IF NOT EXISTS to preserve existing data

-- Create main analytics events table
CREATE TABLE IF NOT EXISTS analytics_events
(
    -- Server metadata
    serverTimestamp DateTime64(3) DEFAULT now64(3),
    clientIP String,
    
    -- Event metadata
    eventType String,
    timestamp DateTime64(3),
    
    -- Event data (stored as JSON for flexibility)
    data String,
    
    -- Materialized columns for common queries
    auctionId String MATERIALIZED JSONExtractString(data, 'auctionId'),
    timeout UInt32 MATERIALIZED JSONExtractUInt(data, 'timeout'),
    
    -- Date partition for efficient queries
    date Date DEFAULT toDate(serverTimestamp)
) 
ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (eventType, serverTimestamp)
SETTINGS index_granularity = 8192;

-- Create view for bid requests with Optable enrichment detection
CREATE MATERIALIZED VIEW IF NOT EXISTS bid_requests_mv
ENGINE = MergeTree()
ORDER BY (serverTimestamp, bidderCode, bidId)
AS SELECT
    serverTimestamp,
    clientIP,
    timestamp,
    JSONExtractString(data, 'bidderCode') as bidderCode,
    JSONExtractString(data, 'auctionId') as auctionId,
    JSONExtractString(data, 'bidderRequestId') as bidderRequestId,
    -- Extract bid IDs from the bids array
    arrayJoin(JSONExtractArrayRaw(data, 'bids')) as bidJson,
    JSONExtractString(bidJson, 'bidId') as bidId,
    JSONExtractString(bidJson, 'adUnitCode') as adUnitCode,
    JSONExtractString(bidJson, 'transactionId') as transactionId,
    JSONExtractString(bidJson, 'adUnitId') as adUnitId,
    -- Check for Optable enrichment in user.eids or user.data
    (
        (JSONHas(data, 'ortb2', 'user', 'eids') AND 
         JSONExtractRaw(data, 'ortb2', 'user', 'eids') LIKE '%optable.co%')
        OR 
        (JSONHas(data, 'ortb2', 'user', 'data') AND 
         JSONExtractRaw(data, 'ortb2', 'user', 'data') LIKE '%optable.co%')
    ) as optable_enriched,
    -- Store raw bid data for additional analysis
    bidJson as bidData,
    data as fullData
FROM analytics_events
WHERE eventType = 'bidRequested';

-- Create view for bid responses
CREATE MATERIALIZED VIEW IF NOT EXISTS bid_responses_mv
ENGINE = MergeTree()
ORDER BY (serverTimestamp, bidderCode, requestId)
AS SELECT
    serverTimestamp,
    clientIP,
    timestamp,
    JSONExtractString(data, 'bidderCode') as bidderCode,
    JSONExtractString(data, 'auctionId') as auctionId,
    JSONExtractString(data, 'adId') as adId,
    JSONExtractString(data, 'adUnitCode') as adUnitCode,
    JSONExtractString(data, 'requestId') as requestId,
    JSONExtractString(data, 'transactionId') as transactionId,
    JSONExtractString(data, 'adUnitId') as adUnitId,
    JSONExtractFloat(data, 'cpm') as cpm,
    JSONExtractString(data, 'currency') as currency,
    JSONExtractInt(data, 'width') as width,
    JSONExtractInt(data, 'height') as height,
    JSONExtractInt(data, 'responseTimestamp') as responseTimestamp,
    JSONExtractInt(data, 'requestTimestamp') as requestTimestamp,
    responseTimestamp - requestTimestamp as latencyMs,
    JSONExtractString(data, 'statusMessage') as statusMessage,
    data as fullData
FROM analytics_events
WHERE eventType = 'bidResponse';

-- Create view for winning bids
CREATE MATERIALIZED VIEW IF NOT EXISTS bid_wins_mv
ENGINE = MergeTree()
ORDER BY (serverTimestamp, bidderCode, requestId)
AS SELECT
    serverTimestamp,
    clientIP,
    timestamp,
    JSONExtractString(data, 'bidderCode') as bidderCode,
    JSONExtractString(data, 'auctionId') as auctionId,
    JSONExtractString(data, 'adId') as adId,
    JSONExtractString(data, 'adUnitCode') as adUnitCode,
    JSONExtractString(data, 'requestId') as requestId,
    JSONExtractString(data, 'transactionId') as transactionId,
    JSONExtractString(data, 'adUnitId') as adUnitId,
    JSONExtractFloat(data, 'cpm') as cpm,
    JSONExtractString(data, 'currency') as currency,
    JSONExtractInt(data, 'width') as width,
    JSONExtractInt(data, 'height') as height,
    JSONExtractString(data, 'statusMessage') as statusMessage,
    data as fullData
FROM analytics_events
WHERE eventType = 'bidWon';

-- Create aggregated view for Optable performance analysis
CREATE MATERIALIZED VIEW IF NOT EXISTS optable_performance_mv
ENGINE = SummingMergeTree()
ORDER BY (date_hour, bidderCode, optable_enriched)
AS SELECT
    toStartOfHour(req.serverTimestamp) as date_hour,
    req.bidderCode as bidderCode,
    req.optable_enriched as optable_enriched,
    -- Bid request metrics
    count(DISTINCT req.bidId) as bid_requests,
    -- Bid response metrics (using LEFT JOIN logic via subquery)
    countIf(resp.requestId != '') as bid_responses,
    -- Win metrics
    countIf(win.requestId != '') as wins,
    -- CPM metrics for responses
    sumIf(resp.cpm, resp.cpm > 0) as sum_response_cpm,
    countIf(resp.cpm > 0) as count_response_cpm,
    -- CPM metrics for wins
    sumIf(win.cpm, win.cpm > 0) as sum_win_cpm,
    countIf(win.cpm > 0) as count_win_cpm,
    -- Latency metrics
    sum(resp.latencyMs) as sum_latency_ms,
    count(resp.latencyMs) as count_latency
FROM bid_requests_mv req
LEFT JOIN bid_responses_mv resp ON 
    req.bidId = resp.requestId AND
    req.bidderCode = resp.bidderCode
LEFT JOIN bid_wins_mv win ON 
    req.bidId = win.requestId AND
    req.bidderCode = win.bidderCode
GROUP BY date_hour, bidderCode, optable_enriched;

-- Create helper functions for analysis
-- Note: ClickHouse doesn't support stored procedures, but we can create views for common queries

-- View for comparing Optable enriched vs non-enriched performance
CREATE VIEW IF NOT EXISTS optable_comparison AS
SELECT
    bidderCode,
    optable_enriched,
    sum(bid_requests) as total_requests,
    sum(bid_responses) as total_responses,
    sum(wins) as total_wins,
    -- Response rate
    (sum(bid_responses) * 100.0 / sum(bid_requests)) as response_rate,
    -- Win rate (from responses)
    (sum(wins) * 100.0 / sum(bid_responses)) as win_rate_from_responses,
    -- Win rate (from requests)
    (sum(wins) * 100.0 / sum(bid_requests)) as win_rate_from_requests,
    -- Average CPMs (calculate from sums)
    (sum(sum_response_cpm) / sum(count_response_cpm)) as avg_response_cpm,
    (sum(sum_win_cpm) / sum(count_win_cpm)) as avg_win_cpm,
    -- CPM uplift for wins
    ((sum(sum_win_cpm) / sum(count_win_cpm)) - (sum(sum_response_cpm) / sum(count_response_cpm))) as cpm_uplift,
    -- Average latency
    (sum(sum_latency_ms) / sum(count_latency)) as avg_latency_ms
FROM optable_performance_mv
GROUP BY bidderCode, optable_enriched
ORDER BY bidderCode, optable_enriched;

-- View for overall Optable impact
CREATE VIEW IF NOT EXISTS optable_impact_summary AS
WITH enriched_stats AS (
    SELECT
        sum(bid_requests) as requests,
        sum(bid_responses) as responses,
        sum(wins) as wins,
        sum(sum_response_cpm) / sum(count_response_cpm) as avg_resp_cpm,
        sum(sum_win_cpm) / sum(count_win_cpm) as avg_win_cpm
    FROM optable_performance_mv
    WHERE optable_enriched = 1
),
non_enriched_stats AS (
    SELECT
        sum(bid_requests) as requests,
        sum(bid_responses) as responses,
        sum(wins) as wins,
        sum(sum_response_cpm) / sum(count_response_cpm) as avg_resp_cpm,
        sum(sum_win_cpm) / sum(count_win_cpm) as avg_win_cpm
    FROM optable_performance_mv
    WHERE optable_enriched = 0
)
SELECT
    'Response Rate Lift' as metric,
    round((enriched_stats.responses * 100.0 / enriched_stats.requests) - 
          (non_enriched_stats.responses * 100.0 / non_enriched_stats.requests), 2) as percentage_lift
FROM enriched_stats, non_enriched_stats
UNION ALL
SELECT
    'Win Rate Lift' as metric,
    round((enriched_stats.wins * 100.0 / enriched_stats.requests) - 
          (non_enriched_stats.wins * 100.0 / non_enriched_stats.requests), 2) as percentage_lift
FROM enriched_stats, non_enriched_stats
UNION ALL
SELECT
    'Response CPM Lift' as metric,
    round(((enriched_stats.avg_resp_cpm - non_enriched_stats.avg_resp_cpm) / 
           non_enriched_stats.avg_resp_cpm) * 100, 2) as percentage_lift
FROM enriched_stats, non_enriched_stats
UNION ALL
SELECT
    'Win CPM Lift' as metric,
    round(((enriched_stats.avg_win_cpm - non_enriched_stats.avg_win_cpm) / 
           non_enriched_stats.avg_win_cpm) * 100, 2) as percentage_lift
FROM enriched_stats, non_enriched_stats;