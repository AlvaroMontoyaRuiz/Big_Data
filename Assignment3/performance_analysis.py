import psycopg2
from datetime import datetime, timedelta
import time
from decimal import Decimal
import sys

def connect_db():
    """Establishes a connection to the PostgreSQL database."""
    return psycopg2.connect(
        host="localhost",
        database="streamflix",
        user="student",
        password="student"
    )

def create_base_table(conn):
    """Creates a base table for viewing events, dropping it if it already exists."""
    cur = conn.cursor()
    cur.execute("""
        DROP TABLE IF EXISTS viewing_events CASCADE;

        CREATE TABLE viewing_events (
            event_id BIGSERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL,
            content_id INTEGER NOT NULL,
            event_timestamp TIMESTAMPTZ NOT NULL,
            event_type VARCHAR(50),
            watch_duration_seconds INTEGER,
            device_type VARCHAR(50),
            country_code VARCHAR(2),
            quality VARCHAR(10),
            bandwidth_mbps DECIMAL(6,2),
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
    """)
    conn.commit()
    cur.close()
    print("✓ Base table created")

def generate_viewing_events_generate_series(conn, num_days=90, events_per_day=100_000):
    """
    Generates realistic viewing events using SQL's generate_series.
    The data includes a normal distribution for peak viewing hours.
    """
    cur = conn.cursor()
    start_date = datetime.now() - timedelta(days=num_days)
    end_date = datetime.now()

    print(f"Generating {num_days * events_per_day} events with SQL generate_series...")

    try:
        cur.execute(f"""
            INSERT INTO viewing_events (
                user_id, content_id, event_timestamp, event_type,
                watch_duration_seconds, device_type, country_code,
                quality, bandwidth_mbps
            )
            SELECT
                (FLOOR(POW(random(), 2) * 100000) + 1)::int AS user_id,
                (FLOOR(POW(random(), 2) * 10000) + 1)::int AS content_id,
                date_trunc('day', gs.day)
                + make_interval(hours := GREATEST(0, LEAST(23, ROUND(20 + (random() - 0.5) * 4)::int)))
                + make_interval(mins := FLOOR(random() * 60)::int)
                + make_interval(secs := FLOOR(random() * 60)::int) AS event_timestamp,
                (ARRAY['start','pause','resume','complete','skip'])
                    [CASE 
                        WHEN random() < 0.4 THEN 1
                        WHEN random() < 0.7 THEN 2
                        WHEN random() < 0.9 THEN 3
                        WHEN random() < 0.95 THEN 4
                        ELSE 5
                    END] AS event_type,
                (FLOOR(random() * 3571) + 30)::int AS watch_duration_seconds,
                (ARRAY['mobile','tv','web','tablet'])
                    [CASE
                        WHEN random() < 0.4 THEN 1
                        WHEN random() < 0.7 THEN 2
                        WHEN random() < 0.9 THEN 3
                        ELSE 4
                    END] AS device_type,
                (ARRAY['US','UK','CA','AU','OT'])
                    [CASE
                        WHEN random() < 0.4 THEN 1
                        WHEN random() < 0.55 THEN 2
                        WHEN random() < 0.65 THEN 3
                        WHEN random() < 0.75 THEN 4
                        ELSE 5
                    END] AS country_code,
                (ARRAY['480p','720p','1080p','4K'])[FLOOR(random()*4 + 1)] AS quality,
                ROUND((random() * 49 + 1)::numeric, 2) AS bandwidth_mbps
            FROM generate_series('{start_date}'::timestamp, '{end_date}'::timestamp, interval '1 day') AS gs(day),
                    generate_series(1, {events_per_day}) AS gs2
        """)
        conn.commit()
        print("✓ All events generated")
    except psycopg2.Error as e:
        print(f"Error generating data: {e}", file=sys.stderr)
        conn.rollback()
    finally:
        cur.close()

def create_indexes(conn):
    """
    Reads and executes index creation statements from a separate SQL file.
    """
    cur = conn.cursor()
    print("Creating indexes from index_strategy.sql...")
    
    try:
        with open('index_strategy.sql', 'r') as f:
            sql_commands = f.read()
        
        # Split the commands by semicolon and execute each one
        for command in sql_commands.split(';'):
            command = command.strip()
            if command:
                cur.execute(command)
        
        conn.commit()
        print("✓ Indexes created successfully")
    except psycopg2.Error as e:
        print(f"Error creating indexes: {e}", file=sys.stderr)
        conn.rollback()
    except FileNotFoundError:
        print("Error: The file 'index_strategy.sql' was not found.", file=sys.stderr)
        conn.rollback()
    finally:
        cur.close()

def get_daily_active_users_query(days=7):
    """Returns the daily active users query for a specified number of days."""
    return f"""
        SELECT event_timestamp::date AS day, COUNT(DISTINCT user_id) AS dau
        FROM viewing_events
        WHERE event_timestamp >= NOW() - INTERVAL '{days} days'
        GROUP BY day
        ORDER BY day;
    """

def get_top_content_query(days=1, limit=10):
    """Returns the top content by views query for a specified time period and limit."""
    return f"""
        SELECT content_id, COUNT(*) AS views
        FROM viewing_events
        WHERE event_timestamp >= NOW() - INTERVAL '{days} days'
        GROUP BY content_id
        ORDER BY views DESC
        LIMIT {limit};
    """

def get_device_usage_query(days=30):
    """Returns the device usage query for a specified number of days."""
    return f"""
        SELECT device_type, COUNT(*) AS views
        FROM viewing_events
        WHERE event_timestamp >= NOW() - INTERVAL '{days} days'
        GROUP BY device_type
        ORDER BY views DESC;
    """

def get_content_performance_query(days=30, limit=10):
    """Returns the content performance query for a specified time period and limit."""
    return f"""
        SELECT
            content_id, 
            COUNT(*) AS views,
            SUM(CASE WHEN event_type = 'complete' THEN 1 ELSE 0 END) AS completions
        FROM viewing_events
        WHERE event_timestamp >= NOW() - INTERVAL '{days} days' AND event_type IN ('start', 'complete')
        GROUP BY content_id
        ORDER BY views DESC
        LIMIT {limit};
    """

def get_regional_analytics_query(days=30):
    """Returns the regional analytics query for a specified number of days."""
    return f"""
        SELECT country_code, COUNT(*) AS total_views
        FROM viewing_events
        WHERE event_timestamp >= NOW() - INTERVAL '{days} days'
        GROUP BY country_code
        ORDER BY total_views DESC;
    """

def get_user_engagement_query(days=7):
    """Returns the user engagement query for a specified number of days."""
    return f"""
        SELECT
            user_id,
            SUM(watch_duration_seconds) AS total_watch_time
        FROM viewing_events
        WHERE event_timestamp >= NOW() - INTERVAL '{days} days'
        GROUP BY user_id
        ORDER BY total_watch_time DESC
        LIMIT 10;
    """

def analyze_current_performance(conn):
    """
    Runs EXPLAIN ANALYZE on critical queries and returns results.

    Tests a comprehensive set of queries based on business analytics needs.
    """
    results = {}
    cur = conn.cursor()

    queries_to_explain = {
        "daily_active_users": get_daily_active_users_query(),
        "top_content_performance": get_content_performance_query(),
        "regional_analytics": get_regional_analytics_query(),
        "device_analytics": get_device_usage_query(),
        "most_engaged_users_last_7_days": get_user_engagement_query()
    }

    print("\n--- Running EXPLAIN ANALYZE on current performance ---")
    for name, query in queries_to_explain.items():
        print(f"Analyzing query: {name}...")
        cur.execute(f"EXPLAIN (ANALYZE, BUFFERS, VERBOSE) {query}")
        results[name] = cur.fetchall()

    cur.close()
    return results

def test_query_performance(conn, query, iterations=5):
    """
    Test query performance with a number of iterations.
    Returns average, min, and max execution times.
    """
    cursor = conn.cursor()
    times = []
    
    # Warm up the cache
    cursor.execute(query)
    cursor.fetchall()

    for _ in range(iterations):
        start = time.perf_counter()
        cursor.execute(query)
        cursor.fetchall()
        times.append(time.perf_counter() - start)
    
    cursor.close()
    
    return {
        'avg_time': sum(times) / len(times),
        'min_time': min(times),
        'max_time': max(times)
    }

def measure_index_performance(conn, queries_dict):
    """
    Measures query performance before and after creating indexes,
    using the quick performance test method.

    Args:
        conn: Database connection
        queries_dict: Dictionary of test queries
        
    Returns:
        Dictionary with before/after timings and improvement percentages
    """
    results = {}
    
    # 1. Run queries and record times BEFORE creating indexes
    print("\n--- Measuring performance BEFORE indexing ---")
    for name, query in queries_dict.items():
        print(f"Executing query: {name}...")
        timings = test_query_performance(conn, query, iterations=5)
        results[name] = {
            "before_index_avg_time": timings["avg_time"],
            "before_index_min_time": timings["min_time"],
            "before_index_max_time": timings["max_time"],
            "after_index_avg_time": None,
            "after_index_min_time": None,
            "after_index_max_time": None,
            "improvement_percent": None
        }
    
    # 2. Create the indexes
    create_indexes(conn)
    
    # 3. Run queries again AFTER creating indexes
    print("\n--- Measuring performance AFTER indexing ---")
    for name, query in queries_dict.items():
        print(f"Executing query: {name}...")
        timings = test_query_performance(conn, query, iterations=5)
        results[name]["after_index_avg_time"] = timings["avg_time"]
        results[name]["after_index_min_time"] = timings["min_time"]
        results[name]["after_index_max_time"] = timings["max_time"]
    
    # 4. Calculate improvement percentages
    for name, data in results.items():
        before_time = data["before_index_avg_time"]
        after_time = data["after_index_avg_time"]
        if before_time > 0 and after_time is not None:
            improvement = (before_time - after_time) / before_time * 100
            results[name]["improvement_percent"] = Decimal(improvement).quantize(Decimal('0.01'))
        else:
            results[name]["improvement_percent"] = 0

    return results

if __name__ == "__main__":
    conn = connect_db()
    create_base_table(conn)
    generate_viewing_events_generate_series(conn, num_days=365, events_per_day=800_000)

    # Dictionary of test queries to analyze
    queries_to_test = {
        "daily_active_users_last_7_days": get_daily_active_users_query(),
        "top_10_content_last_24h": get_top_content_query(days=1, limit=10),
        "device_usage_last_month": get_device_usage_query(days=30),
        "content_performance_last_30_days": get_content_performance_query(days=30, limit=10),
        "regional_analytics_last_30_days": get_regional_analytics_query(days=30),
        "most_engaged_users_last_7_days": get_user_engagement_query(days=7)
    }

    # First, analyze the performance before any index creation
    initial_analysis = analyze_current_performance(conn)
    
    print("\n--- Initial EXPLAIN ANALYZE Results (Before Indexing) ---")
    for query_name, explain_output in initial_analysis.items():
        print(f"\nQuery: {query_name}")
        for line in explain_output:
            print(f"  {line[0]}")

    # Then, run the performance comparison with and without indexes
    performance_results = measure_index_performance(conn, queries_to_test)
    
    print("\n--- Final Performance Report ---")
    for query_name, data in performance_results.items():
        before_avg = f"{data['before_index_avg_time']:.4f}"
        after_avg = f"{data['after_index_avg_time']:.4f}"
        improvement = data['improvement_percent']
        print(f"\nQuery: {query_name}")
        print(f"  - Average Time BEFORE indexes: {before_avg} seconds")
        print(f"  - Average Time AFTER indexes:  {after_avg} seconds")
        print(f"  - Performance improvement: {improvement}%")
        print(f"  - Timing Range Before (Min/Max): {data['before_index_min_time']:.4f} / {data['before_index_max_time']:.4f} seconds")
        print(f"  - Timing Range After (Min/Max):  {data['after_index_min_time']:.4f} / {data['after_index_max_time']:.4f} seconds")
    
    conn.close()