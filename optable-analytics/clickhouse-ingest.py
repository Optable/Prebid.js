#!/usr/bin/env python3
"""
Ingest JSONL analytics data into ClickHouse
"""

import json
import sys
import time
from datetime import datetime
from dateutil.parser import parse as parse_date
from clickhouse_driver import Client
import argparse

def connect_to_clickhouse(host='localhost', port=9000, user='prebid', password='prebid123', database='prebid_analytics'):
    """Connect to ClickHouse with retry logic"""
    max_retries = 10
    retry_delay = 2
    
    for attempt in range(max_retries):
        try:
            client = Client(
                host=host,
                port=port,
                user=user,
                password=password,
                database=database
            )
            # Test connection
            client.execute('SELECT 1')
            print(f"Connected to ClickHouse at {host}:{port}")
            return client
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"Connection attempt {attempt + 1} failed: {e}")
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                raise Exception(f"Failed to connect to ClickHouse after {max_retries} attempts: {e}")

def init_database(client):
    """Initialize database and tables"""
    with open('clickhouse-init.sql', 'r') as f:
        sql_commands = f.read().split(';')
        
    for command in sql_commands:
        command = command.strip()
        if command:
            try:
                client.execute(command)
                print(f"Executed: {command[:50]}...")
            except Exception as e:
                print(f"Warning: {e}")

def preview_jsonl_file(filename, num_lines=5):
    """Preview the first few lines of JSONL file to understand the structure"""
    print(f"Previewing first {num_lines} lines of {filename}:")
    print("-" * 50)
    
    try:
        with open(filename, 'r') as f:
            for i, line in enumerate(f):
                if i >= num_lines:
                    break
                line = line.strip()
                if line:
                    try:
                        event = json.loads(line)
                        print(f"Line {i+1}:")
                        print(f"  eventType: {event.get('eventType')}")
                        print(f"  serverTimestamp: {event.get('serverTimestamp')} (type: {type(event.get('serverTimestamp'))})")
                        print(f"  timestamp: {event.get('timestamp')} (type: {type(event.get('timestamp'))})")
                        print(f"  clientIP: {event.get('clientIP')}")
                        print(f"  data keys: {list(event.get('data', {}).keys())}")
                        print()
                    except json.JSONDecodeError as e:
                        print(f"Line {i+1}: Invalid JSON - {e}")
    except FileNotFoundError:
        print(f"File not found: {filename}")
    
    print("-" * 50)

def get_latest_timestamp(client):
    """Get the latest serverTimestamp from the database"""
    try:
        result = client.execute("""
            SELECT max(serverTimestamp) as latest_timestamp
            FROM analytics_events
        """)
        if result and result[0][0]:
            return result[0][0]
        return None
    except Exception as e:
        print(f"Warning: Could not get latest timestamp: {e}")
        return None

def ingest_jsonl_file(client, filename):
    """Ingest JSONL file into ClickHouse (idempotent - only adds new events)"""
    batch_size = 1000
    batch = []
    total_count = 0
    skipped_count = 0
    
    print(f"Reading from {filename}...")
    
    # Get the latest timestamp from the database
    latest_db_timestamp = get_latest_timestamp(client)
    if latest_db_timestamp:
        print(f"Latest timestamp in database: {latest_db_timestamp}")
        print(f"Only ingesting events newer than this timestamp...")
    else:
        print("No existing data found - ingesting all events")
    
    try:
        with open(filename, 'r') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                
                try:
                    event = json.loads(line)
                    
                    # Parse timestamps to datetime objects (make them timezone-naive)
                    server_timestamp = datetime.now().replace(tzinfo=None)
                    client_timestamp = datetime.now().replace(tzinfo=None)
                    
                    if 'serverTimestamp' in event:
                        try:
                            parsed = parse_date(event['serverTimestamp'])
                            # Remove timezone info to make it naive
                            server_timestamp = parsed.replace(tzinfo=None) if parsed.tzinfo else parsed
                        except (ValueError, TypeError):
                            print(f"Warning: Could not parse serverTimestamp: {event.get('serverTimestamp')}")
                    
                    if 'timestamp' in event:
                        try:
                            parsed = parse_date(event['timestamp'])
                            # Remove timezone info to make it naive
                            client_timestamp = parsed.replace(tzinfo=None) if parsed.tzinfo else parsed
                        except (ValueError, TypeError):
                            print(f"Warning: Could not parse timestamp: {event.get('timestamp')}")
                    
                    # Skip if this event is older than or equal to the latest in database
                    if latest_db_timestamp and server_timestamp <= latest_db_timestamp:
                        skipped_count += 1
                        continue
                    
                    # Prepare record for insertion
                    record = (
                        server_timestamp,
                        event.get('clientIP', ''),
                        event.get('eventType', ''),
                        client_timestamp,
                        json.dumps(event.get('data', {}))
                    )
                    
                    batch.append(record)
                    
                    # Insert batch when it reaches batch_size
                    if len(batch) >= batch_size:
                        insert_batch(client, batch)
                        total_count += len(batch)
                        print(f"Inserted {total_count} events...")
                        batch = []
                        
                except json.JSONDecodeError as e:
                    print(f"Error parsing line {line_num}: {e}")
                    continue
            
            # Insert remaining records
            if batch:
                insert_batch(client, batch)
                total_count += len(batch)
                
        print(f"Successfully ingested {total_count} new events from {filename}")
        if skipped_count > 0:
            print(f"Skipped {skipped_count} existing events (already in database)")
        
    except FileNotFoundError:
        print(f"File not found: {filename}")
        return False
    except Exception as e:
        print(f"Error reading file: {e}")
        return False
    
    return True

def insert_batch(client, batch):
    """Insert a batch of records into ClickHouse"""
    try:
        client.execute(
            'INSERT INTO analytics_events (serverTimestamp, clientIP, eventType, timestamp, data) VALUES',
            batch
        )
    except Exception as e:
        print(f"Error inserting batch: {e}")
        raise

def query_stats(client):
    """Query and display basic statistics"""
    print("\n=== Analytics Statistics ===")
    
    # Total events by type
    result = client.execute("""
        SELECT 
            eventType,
            count() as count
        FROM analytics_events
        GROUP BY eventType
        ORDER BY count DESC
    """)
    
    print("\nEvents by Type:")
    for row in result:
        print(f"  {row[0]}: {row[1]}")
    
    # Recent bid responses
    result = client.execute("""
        SELECT 
            bidderCode,
            adUnitCode,
            round(avg(cpm), 2) as avg_cpm,
            count() as bid_count
        FROM bid_responses_mv
        WHERE serverTimestamp > now() - INTERVAL 1 HOUR
        GROUP BY bidderCode, adUnitCode
        ORDER BY avg_cpm DESC
        LIMIT 10
    """)
    
    if result:
        print("\nRecent Bid Performance (Last Hour):")
        print(f"  {'Bidder':<15} {'Ad Unit':<40} {'Avg CPM':<10} {'Count':<10}")
        print("  " + "-" * 75)
        for row in result:
            print(f"  {row[0]:<15} {row[1]:<40} ${row[2]:<9} {row[3]:<10}")
    
    # Win rate by bidder
    result = client.execute("""
        SELECT 
            b.bidderCode,
            countIf(w.bidderCode != '') as wins,
            count() as bids,
            round(countIf(w.bidderCode != '') * 100.0 / count(), 2) as win_rate
        FROM bid_responses_mv b
        LEFT JOIN bid_wins_mv w ON 
            b.bidderCode = w.bidderCode AND 
            b.adUnitCode = w.adUnitCode AND
            abs(b.serverTimestamp - w.serverTimestamp) < 5
        GROUP BY b.bidderCode
        HAVING bids > 0
        ORDER BY win_rate DESC
    """)
    
    if result:
        print("\nWin Rate by Bidder:")
        print(f"  {'Bidder':<15} {'Wins':<10} {'Bids':<10} {'Win Rate %':<10}")
        print("  " + "-" * 45)
        for row in result:
            print(f"  {row[0]:<15} {row[1]:<10} {row[2]:<10} {row[3]:<10}")

def main():
    parser = argparse.ArgumentParser(description='Ingest Prebid analytics data into ClickHouse')
    parser.add_argument('--file', default='analytics-events.jsonl', help='JSONL file to ingest')
    parser.add_argument('--host', default='localhost', help='ClickHouse host')
    parser.add_argument('--port', type=int, default=9000, help='ClickHouse port')
    parser.add_argument('--user', default='prebid', help='ClickHouse user')
    parser.add_argument('--password', default='prebid123', help='ClickHouse password')
    parser.add_argument('--database', default='prebid_analytics', help='ClickHouse database')
    parser.add_argument('--init-only', action='store_true', help='Only initialize database')
    parser.add_argument('--stats-only', action='store_true', help='Only show statistics')
    parser.add_argument('--preview', action='store_true', help='Preview JSONL file structure')
    
    args = parser.parse_args()
    
    # Connect to ClickHouse
    client = connect_to_clickhouse(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        database=args.database
    )
    
    if args.preview:
        preview_jsonl_file(args.file)
        return
    
    if args.init_only:
        init_database(client)
        print("Database initialized successfully")
        return
    
    if args.stats_only:
        query_stats(client)
        return
    
    # Initialize database
    init_database(client)
    
    # Ingest data
    if ingest_jsonl_file(client, args.file):
        # Show statistics
        query_stats(client)

if __name__ == '__main__':
    main()