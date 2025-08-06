-- Optable Analytics Queries for ClickHouse
-- Use these queries to analyze the performance impact of Optable enrichment

-- Set database context for all queries
USE prebid_analytics;

-- ========================================
-- 1. BASIC DATA OVERVIEW
-- ========================================

-- Check Optable enrichment distribution
SELECT 
    optable_enriched,
    count() as request_count,
    round(count() * 100.0 / (SELECT count() FROM prebid_analytics.bid_requests_mv), 1) as percentage
FROM prebid_analytics.bid_requests_mv
GROUP BY optable_enriched;

-- Event counts by type
SELECT 
    eventType,
    count() as count
FROM prebid_analytics.analytics_events
GROUP BY eventType
ORDER BY count DESC;

-- ========================================
-- 2. RESPONSE RATE ANALYSIS
-- ========================================

-- Response rates: enriched vs non-enriched
SELECT
    optable_enriched,
    count(DISTINCT req.bidId) as total_requests,
    countIf(resp.requestId != '') as responses,
    round(countIf(resp.requestId != '') * 100.0 / count(DISTINCT req.bidId), 2) as response_rate_percent
FROM prebid_analytics.bid_requests_mv req
LEFT JOIN prebid_analytics.bid_responses_mv resp ON req.bidId = resp.requestId AND req.bidderCode = resp.bidderCode
GROUP BY optable_enriched
ORDER BY optable_enriched;

-- Response rates by bidder and enrichment status
SELECT
    req.bidderCode,
    req.optable_enriched,
    count(DISTINCT req.bidId) as requests,
    countIf(resp.requestId != '') as responses,
    round(countIf(resp.requestId != '') * 100.0 / count(DISTINCT req.bidId), 2) as response_rate_percent
FROM prebid_analytics.bid_requests_mv req
LEFT JOIN prebid_analytics.bid_responses_mv resp ON req.bidId = resp.requestId AND req.bidderCode = resp.bidderCode
GROUP BY req.bidderCode, req.optable_enriched
ORDER BY req.bidderCode, req.optable_enriched;

-- ========================================
-- 3. WIN RATE ANALYSIS
-- ========================================

-- Win rates: enriched vs non-enriched (from requests)
SELECT
    optable_enriched,
    count(DISTINCT req.bidId) as total_requests,
    countIf(win.requestId != '') as wins,
    round(countIf(win.requestId != '') * 100.0 / count(DISTINCT req.bidId), 2) as win_rate_from_requests_percent
FROM prebid_analytics.bid_requests_mv req
LEFT JOIN prebid_analytics.bid_wins_mv win ON req.bidId = win.requestId AND req.bidderCode = win.bidderCode
GROUP BY optable_enriched
ORDER BY optable_enriched;

-- Win rates: enriched vs non-enriched (from responses)
SELECT
    req.optable_enriched,
    countIf(resp.requestId != '') as total_responses,
    countIf(win.requestId != '') as wins,
    round(countIf(win.requestId != '') * 100.0 / countIf(resp.requestId != ''), 2) as win_rate_from_responses_percent
FROM prebid_analytics.bid_requests_mv req
LEFT JOIN prebid_analytics.bid_responses_mv resp ON req.bidId = resp.requestId AND req.bidderCode = resp.bidderCode
LEFT JOIN prebid_analytics.bid_wins_mv win ON req.bidId = win.requestId AND req.bidderCode = win.bidderCode
GROUP BY req.optable_enriched
ORDER BY req.optable_enriched;

-- Win rates by bidder and enrichment status
SELECT
    req.bidderCode,
    req.optable_enriched,
    count(DISTINCT req.bidId) as requests,
    countIf(resp.requestId != '') as responses,
    countIf(win.requestId != '') as wins,
    round(countIf(resp.requestId != '') * 100.0 / count(DISTINCT req.bidId), 2) as response_rate_percent,
    round(countIf(win.requestId != '') * 100.0 / count(DISTINCT req.bidId), 2) as win_rate_from_requests_percent,
    round(countIf(win.requestId != '') * 100.0 / countIf(resp.requestId != ''), 2) as win_rate_from_responses_percent
FROM prebid_analytics.bid_requests_mv req
LEFT JOIN prebid_analytics.bid_responses_mv resp ON req.bidId = resp.requestId AND req.bidderCode = resp.bidderCode
LEFT JOIN prebid_analytics.bid_wins_mv win ON req.bidId = win.requestId AND req.bidderCode = win.bidderCode
GROUP BY req.bidderCode, req.optable_enriched
ORDER BY req.bidderCode, req.optable_enriched;

-- ========================================
-- 4. CPM ANALYSIS
-- ========================================

-- Average bid response CPMs: enriched vs non-enriched
SELECT
    req.optable_enriched,
    countIf(resp.cpm > 0) as bid_responses_with_cpm,
    round(avg(resp.cpm), 2) as avg_response_cpm,
    round(quantile(0.5)(resp.cpm), 2) as median_response_cpm,
    round(min(resp.cpm), 2) as min_response_cpm,
    round(max(resp.cpm), 2) as max_response_cpm
FROM prebid_analytics.bid_requests_mv req
LEFT JOIN prebid_analytics.bid_responses_mv resp ON req.bidId = resp.requestId AND req.bidderCode = resp.bidderCode
WHERE resp.cpm > 0
GROUP BY req.optable_enriched
ORDER BY req.optable_enriched;

-- Average win CPMs: enriched vs non-enriched
SELECT
    req.optable_enriched,
    countIf(win.cpm > 0) as wins_with_cpm,
    round(avg(win.cpm), 2) as avg_win_cpm,
    round(quantile(0.5)(win.cpm), 2) as median_win_cpm,
    round(min(win.cpm), 2) as min_win_cpm,
    round(max(win.cpm), 2) as max_win_cpm
FROM prebid_analytics.bid_requests_mv req
LEFT JOIN prebid_analytics.bid_wins_mv win ON req.bidId = win.requestId AND req.bidderCode = win.bidderCode
WHERE win.cpm > 0
GROUP BY req.optable_enriched
ORDER BY req.optable_enriched;

-- CPM analysis by bidder and enrichment status
SELECT
    req.bidderCode,
    req.optable_enriched,
    countIf(resp.cpm > 0) as bid_responses,
    round(avg(resp.cpm), 2) as avg_bid_cpm,
    countIf(win.cpm > 0) as wins,
    round(avg(win.cpm), 2) as avg_win_cpm
FROM prebid_analytics.bid_requests_mv req
LEFT JOIN prebid_analytics.bid_responses_mv resp ON req.bidId = resp.requestId AND req.bidderCode = resp.bidderCode
LEFT JOIN prebid_analytics.bid_wins_mv win ON req.bidId = win.requestId AND req.bidderCode = win.bidderCode
WHERE resp.cpm > 0 OR win.cpm > 0
GROUP BY req.bidderCode, req.optable_enriched
ORDER BY req.bidderCode, req.optable_enriched;

-- ========================================
-- 5. LATENCY ANALYSIS
-- ========================================

-- Response latency: enriched vs non-enriched
SELECT
    req.optable_enriched,
    count() as responses_with_latency,
    round(avg(resp.latencyMs), 0) as avg_latency_ms,
    round(quantile(0.5)(resp.latencyMs), 0) as median_latency_ms,
    round(quantile(0.95)(resp.latencyMs), 0) as p95_latency_ms
FROM prebid_analytics.bid_requests_mv req
JOIN prebid_analytics.bid_responses_mv resp ON req.bidId = resp.requestId AND req.bidderCode = resp.bidderCode
WHERE resp.latencyMs > 0
GROUP BY req.optable_enriched
ORDER BY req.optable_enriched;

-- ========================================
-- 6. COMPREHENSIVE COMPARISON (FOR LIFT CALCULATION)
-- ========================================

-- Full comparison table for calculating lifts
WITH enriched_stats AS (
    SELECT
        req.bidderCode,
        count(DISTINCT req.bidId) as requests,
        countIf(resp.requestId != '') as responses,
        countIf(win.requestId != '') as wins,
        round(countIf(resp.requestId != '') * 100.0 / count(DISTINCT req.bidId), 2) as response_rate,
        round(countIf(win.requestId != '') * 100.0 / count(DISTINCT req.bidId), 2) as win_rate,
        round(avg(resp.cpm), 2) as avg_response_cpm,
        round(avg(win.cpm), 2) as avg_win_cpm
    FROM prebid_analytics.bid_requests_mv req
    LEFT JOIN prebid_analytics.bid_responses_mv resp ON req.bidId = resp.requestId AND req.bidderCode = resp.bidderCode
    LEFT JOIN prebid_analytics.bid_wins_mv win ON req.bidId = win.requestId AND req.bidderCode = win.bidderCode
    WHERE req.optable_enriched = 1
    GROUP BY req.bidderCode
),
standard_stats AS (
    SELECT
        req.bidderCode,
        count(DISTINCT req.bidId) as requests,
        countIf(resp.requestId != '') as responses,
        countIf(win.requestId != '') as wins,
        round(countIf(resp.requestId != '') * 100.0 / count(DISTINCT req.bidId), 2) as response_rate,
        round(countIf(win.requestId != '') * 100.0 / count(DISTINCT req.bidId), 2) as win_rate,
        round(avg(resp.cpm), 2) as avg_response_cpm,
        round(avg(win.cpm), 2) as avg_win_cpm
    FROM prebid_analytics.bid_requests_mv req
    LEFT JOIN prebid_analytics.bid_responses_mv resp ON req.bidId = resp.requestId AND req.bidderCode = resp.bidderCode
    LEFT JOIN prebid_analytics.bid_wins_mv win ON req.bidId = win.requestId AND req.bidderCode = win.bidderCode
    WHERE req.optable_enriched = 0
    GROUP BY req.bidderCode
)
SELECT
    COALESCE(e.bidderCode, s.bidderCode) as bidder,
    -- Enriched stats
    COALESCE(e.requests, 0) as enriched_requests,
    COALESCE(e.responses, 0) as enriched_responses,
    COALESCE(e.wins, 0) as enriched_wins,
    COALESCE(e.response_rate, 0) as enriched_response_rate,
    COALESCE(e.win_rate, 0) as enriched_win_rate,
    COALESCE(e.avg_response_cpm, 0) as enriched_avg_response_cpm,
    COALESCE(e.avg_win_cpm, 0) as enriched_avg_win_cpm,
    -- Standard stats
    COALESCE(s.requests, 0) as standard_requests,
    COALESCE(s.responses, 0) as standard_responses,
    COALESCE(s.wins, 0) as standard_wins,
    COALESCE(s.response_rate, 0) as standard_response_rate,
    COALESCE(s.win_rate, 0) as standard_win_rate,
    COALESCE(s.avg_response_cpm, 0) as standard_avg_response_cpm,
    COALESCE(s.avg_win_cpm, 0) as standard_avg_win_cpm,
    -- Lifts (calculate manually or in your application)
    COALESCE(e.response_rate, 0) - COALESCE(s.response_rate, 0) as response_rate_lift_points,
    COALESCE(e.win_rate, 0) - COALESCE(s.win_rate, 0) as win_rate_lift_points
FROM enriched_stats e
FULL OUTER JOIN standard_stats s ON e.bidderCode = s.bidderCode
ORDER BY bidder;

-- ========================================
-- 7. TIME SERIES ANALYSIS
-- ========================================

-- Performance over time (hourly)
SELECT
    toStartOfHour(req.serverTimestamp) as hour,
    req.optable_enriched,
    count(DISTINCT req.bidId) as requests,
    countIf(resp.requestId != '') as responses,
    countIf(win.requestId != '') as wins,
    round(countIf(resp.requestId != '') * 100.0 / count(DISTINCT req.bidId), 1) as response_rate
FROM prebid_analytics.bid_requests_mv req
LEFT JOIN prebid_analytics.bid_responses_mv resp ON req.bidId = resp.requestId AND req.bidderCode = resp.bidderCode
LEFT JOIN prebid_analytics.bid_wins_mv win ON req.bidId = win.requestId AND req.bidderCode = win.bidderCode
GROUP BY hour, req.optable_enriched
ORDER BY hour, req.optable_enriched;

-- ========================================
-- 8. DATA VALIDATION QUERIES
-- ========================================

-- Check for orphaned responses (responses without matching requests)
SELECT count() as orphaned_responses
FROM prebid_analytics.bid_responses_mv resp
LEFT JOIN prebid_analytics.bid_requests_mv req ON resp.requestId = req.bidId AND resp.bidderCode = req.bidderCode
WHERE req.bidId IS NULL;

-- Check for orphaned wins (wins without matching requests)
SELECT count() as orphaned_wins
FROM prebid_analytics.bid_wins_mv win
LEFT JOIN prebid_analytics.bid_requests_mv req ON win.requestId = req.bidId AND win.bidderCode = req.bidderCode
WHERE req.bidId IS NULL;

-- Check join match rates
SELECT
    'bid_requests_to_responses' as join_type,
    count(DISTINCT req.bidId) as left_table_count,
    countIf(resp.requestId != '') as matched_count,
    round(countIf(resp.requestId != '') * 100.0 / count(DISTINCT req.bidId), 1) as match_rate_percent
FROM prebid_analytics.bid_requests_mv req
LEFT JOIN prebid_analytics.bid_responses_mv resp ON req.bidId = resp.requestId AND req.bidderCode = resp.bidderCode
UNION ALL
SELECT
    'bid_requests_to_wins' as join_type,
    count(DISTINCT req.bidId) as left_table_count,
    countIf(win.requestId != '') as matched_count,
    round(countIf(win.requestId != '') * 100.0 / count(DISTINCT req.bidId), 1) as match_rate_percent
FROM prebid_analytics.bid_requests_mv req
LEFT JOIN prebid_analytics.bid_wins_mv win ON req.bidId = win.requestId AND req.bidderCode = win.bidderCode;