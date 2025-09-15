import psycopg2
from datetime import datetime, timedelta
import time
from decimal import Decimal
import sys

# --- Global Configuration ---
NUM_DAYS = 30
EVENTS_PER_DAY = 100000
CONN_PARAMS = {
    "host": "localhost",
    "database": "streamflix",
    "user": "student",
    "password": "student"
}
# --- Database & Data Generation Functions ---
def connect_db():
    """Establishes a connection to the PostgreSQL database."""
    return psycopg2.connect(**CONN_PARAMS)

def run_sql(conn, sql, params=None):
    """Generic runner for multi-statement SQL (with commit, rollback, cleanup)."""
    with conn.cursor() as cur:
        try:
            cur.execute(sql, params) if params else cur.execute(sql)
            conn.commit()
        except psycopg2.Error as e:
            print(f"SQL error: {e}", file=sys.stderr)
            conn.rollback()
            raise

def create_base_table(conn):
    """Drop and (re)create the main events table."""
    run_sql(conn, """
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
    print("Base table created")

def generate_viewing_events(conn, num_days=90, events_per_day=100_000):
    """Efficiently bulk-inserts synthetic events using generate_series and controlled distributions."""
    start_date = datetime.now() - timedelta(days=num_days)
    end_date = datetime.now()
    print(f"Generating {num_days * events_per_day} events...")
    sql = f"""
        INSERT INTO viewing_events (
            user_id, content_id, event_timestamp, event_type,
            watch_duration_seconds, device_type, country_code,
            quality, bandwidth_mbps
        )
        SELECT
            (FLOOR(POW(random(), 2) * 100000) + 1)::int,
            (FLOOR(POW(random(), 2) * 10000) + 1)::int,
            date_trunc('day', gs.day)
              + make_interval(hours := GREATEST(0, LEAST(23, ROUND(20 + (random() - 0.5) * 4)::int)))
              + make_interval(mins := FLOOR(random() * 60)::int)
              + make_interval(secs := FLOOR(random() * 60)::int),
            (ARRAY['start','pause','resume','complete','skip'])
                [1 + (random() >= 0.4)::int + (random() >= 0.7)::int 
                + (random() >= 0.9)::int + (random() >= 0.95)::int],
            (FLOOR(random() * 3571) + 30)::int,
            (ARRAY['mobile','tv','web','tablet'])
                [1 + (random() >= 0.4)::int + (random() >= 0.7)::int
                + (random() >= 0.9)::int],
            (ARRAY['US','UK','CA','AU','OT'])
                [1 + (random() >= 0.4)::int + (random() >= 0.55)::int
                + (random() >= 0.65)::int + (random() >= 0.75)::int],
            (ARRAY['480p','720p','1080p','4K'])[FLOOR(random()*4 + 1)],
            ROUND((random() * 49 + 1)::numeric, 2)
        FROM generate_series('{start_date}'::timestamp, '{end_date}'::timestamp, interval '1 day') AS gs(day),
             generate_series(1, {events_per_day}) AS gs2
    """
    try:
        run_sql(conn, sql)
        print("Events generated")
    except Exception:
        print("Error generating events", file=sys.stderr)

def create_indexes(conn, index_file='index_strategy.sql'):
    """Reads index definitions from a file and executes them sequentially."""
    print("Building indexes from", index_file)
    try:
        with open(index_file, 'r') as f:
            sql_cmds = [cmd.strip() for cmd in f.read().split(';') if cmd.strip()]
        for stmt in sql_cmds:
            run_sql(conn, stmt)
        print("Indexes created")
    except FileNotFoundError:
        print(f"Index file {index_file} not found.", file=sys.stderr)
    except Exception as e:
        print(f"Error during index creation: {e!r}", file=sys.stderr)

# --- Query templates ---

def get_daily_active_users_query(days=7):
    return f"""
        SELECT event_timestamp::date AS day, COUNT(DISTINCT user_id) AS dau
        FROM viewing_events
        WHERE event_timestamp >= NOW() - INTERVAL '{days} days'
        GROUP BY day
        ORDER BY day;
    """

def get_top_content_query(days=1, limit=10):
    return f"""
        SELECT content_id, COUNT(*) AS views
        FROM viewing_events
        WHERE event_timestamp >= NOW() - INTERVAL '{days} days'
        GROUP BY content_id
        ORDER BY views DESC
        LIMIT {limit};
    """

def get_device_usage_query(days=30):
    return f"""
        SELECT device_type, COUNT(*) AS views
        FROM viewing_events
        WHERE event_timestamp >= NOW() - INTERVAL '{days} days'
        GROUP BY device_type
        ORDER BY views DESC;
    """

def get_content_performance_query(days=30, limit=10):
    return f"""
        SELECT
            content_id, COUNT(*) AS views,
            SUM(CASE WHEN event_type = 'complete' THEN 1 ELSE 0 END) AS completions
        FROM viewing_events
        WHERE event_timestamp >= NOW() - INTERVAL '{days} days'
          AND event_type IN ('start', 'complete')
        GROUP BY content_id
        ORDER BY views DESC
        LIMIT {limit};
    """

def get_regional_analytics_query(days=30):
    return f"""
        SELECT country_code, COUNT(*) AS total_views
        FROM viewing_events
        WHERE event_timestamp >= NOW() - INTERVAL '{days} days'
        GROUP BY country_code
        ORDER BY total_views DESC;
    """

def get_user_engagement_query(days=7):
    return f"""
        SELECT user_id, SUM(watch_duration_seconds) AS total_watch_time
        FROM viewing_events
        WHERE event_timestamp >= NOW() - INTERVAL '{days} days'
        GROUP BY user_id
        ORDER BY total_watch_time DESC
        LIMIT 10;
    """

def analyze_performance(conn, queries):
    """Runs EXPLAIN ANALYZE for given queries."""
    results = {}
    with conn.cursor() as cur:
        for name, q in queries.items():
            cur.execute(f"EXPLAIN (ANALYZE, BUFFERS, VERBOSE) {q}")
            results[name] = cur.fetchall()
    return results

def measure_query_performance(conn, query, times=5):
    "Return (avg, min, max) execution seconds for a query."
    with conn.cursor() as cur:
        cur.execute(query)
        cur.fetchall()  # warm up the query plan
        times_list = []
        for _ in range(times):
            t0 = time.perf_counter()
            cur.execute(query)
            cur.fetchall()
            times_list.append(time.perf_counter() - t0)
        return {
            'avg_time': sum(times_list) / times,
            'min_time': min(times_list),
            'max_time': max(times_list)
        }

def measure_index_performance(conn, queries_dict):
    """Measure query speed before/after indexing and calculate improvements."""
    results = {}
    print("\n--- Measuring performance BEFORE indexing ---")
    for name, query in queries_dict.items():
        pre = measure_query_performance(conn, query)
        results[name] = {f"before_{k}": v for k, v in pre.items()}
    create_indexes(conn)
    print("\n--- Measuring performance AFTER indexing ---")
    for name, query in queries_dict.items():
        post = measure_query_performance(conn, query)
        results[name].update({f"after_{k}": v for k, v in post.items()})
        before, after = results[name]['before_avg_time'], results[name]['after_avg_time']
        results[name]["improvement_percent"] = Decimal(((before - after) / before * 100) if before > 0 else 0).quantize(Decimal('0.01'))
    return results

if __name__ == "__main__":
    with connect_db() as conn:
        create_base_table(conn)
        generate_viewing_events(conn, NUM_DAYS, EVENTS_PER_DAY)
        
        queries = {
            "daily_active_users_last_7_days": get_daily_active_users_query(),
            "top_10_content_last_24h": get_top_content_query(days=1, limit=10),
            "device_usage_last_month": get_device_usage_query(days=30),
            "content_performance_last_30_days": get_content_performance_query(days=30, limit=10),
            "regional_analytics_last_30_days": get_regional_analytics_query(days=30),
            "most_engaged_users_last_7_days": get_user_engagement_query(days=7),
        }
        analysis = analyze_performance(conn, queries)
        print("\n--- Initial EXPLAIN ANALYZE Results (Before Indexing) ---")
        for qn, out in analysis.items():
            print(f"\nQuery: {qn}")
            for line in out:
                print(f"  {line}")
        perf = measure_index_performance(conn, queries)
        print("\n--- Final Performance Report ---")
        for qn, data in perf.items():
            print(f"\nQuery: {qn}")
            print(f"  - Avg BEFORE: {data['before_avg_time']:.4f} sec")
            print(f"  - Avg AFTER:  {data['after_avg_time']:.4f} sec")
            print(f"  - Improvement: {data['improvement_percent']}%")
            print(f"  - Timing Before (Min/Max): {data['before_min_time']:.4f} / {data['before_max_time']:.4f} sec")
            print(f"  - Timing After (Min/Max):  {data['after_min_time']:.4f} / {data['after_max_time']:.4f} sec")