import psycopg2
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import time
import sys
import random


# --- Global Configuration ---
NUM_DAYS = 365
EVENTS_PER_DAY = 500000
CONN_PARAMS = {
    "host": "localhost", "database": "streamflix", "user": "student", "password": "student"
}


# --- Database & Data Generation Functions ---
def connect_db():
    """Establishes a connection to the PostgreSQL database."""
    return psycopg2.connect(**CONN_PARAMS)

def create_table(conn, table_name, partition=False, with_pk=True):
    """Creates a table with optional partitioning and primary key."""
    cur = conn.cursor()
    print(f"Creating table '{table_name}'...")
    pk_clause = "PRIMARY KEY" if with_pk else ""
    partition_clause = "PARTITION BY RANGE (event_timestamp)" if partition else ""
    
    cur.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
    cur.execute(f"""
        CREATE TABLE {table_name} (
            event_id BIGSERIAL {pk_clause},
            user_id INTEGER NOT NULL, content_id INTEGER NOT NULL,
            event_timestamp TIMESTAMPTZ NOT NULL, event_type VARCHAR(50),
            watch_duration_seconds INTEGER, device_type VARCHAR(50),
            country_code VARCHAR(2), quality VARCHAR(10),
            bandwidth_mbps DECIMAL(6,2), created_at TIMESTAMPTZ DEFAULT NOW()
        ) {partition_clause};
    """)
    conn.commit()
    cur.close()
    print(f"✓ Table '{table_name}' created")

def generate_events(conn, table_name, num_days, events_per_day):
    """Generates random viewing events for the given table."""
    cur = conn.cursor()
    start_date = datetime.now() - timedelta(days=num_days)
    end_date = datetime.now()
    print(f"Generating {num_days * events_per_day} events for '{table_name}'...")

    cur.execute(f"""
        INSERT INTO {table_name} (
            user_id, content_id, event_timestamp, event_type,
            watch_duration_seconds, device_type, country_code,
            quality, bandwidth_mbps
        )
        SELECT
            FLOOR(POW(random(), 2) * 100000) + 1, 
            FLOOR(POW(random(), 2) * 10000) + 1, 
            date_trunc('day', gs.day) + 
            make_interval(hours := GREATEST(0, LEAST(23, ROUND(20 + (random() - 0.5) * 4)::int))) + 
            make_interval(mins := FLOOR(random() * 60)) + 
            make_interval(secs := FLOOR(random() * 60)),
            (ARRAY['start','pause','resume','complete','skip'])[FLOOR(random()*5 + 1)],
            FLOOR(random() * 3571) + 30, 
            (ARRAY['mobile','tv','web','tablet'])[FLOOR(random()*4 + 1)],
            (ARRAY['US','UK','CA','AU','OT'])[FLOOR(random()*5 + 1)],
            (ARRAY['480p','720p','1080p','4K'])[FLOOR(random()*4 + 1)],
            ROUND((random() * 49 + 1)::numeric, 2)
        FROM generate_series('{start_date}'::timestamp, '{end_date}'::timestamp, interval '1 day') AS gs(day),
             generate_series(1, {events_per_day}) AS gs2
    """)
    conn.commit()
    cur.close()
    print("✓ Data generation complete")

def create_indexes(conn, table_name):
    """Creates the necessary indexes for the specified table."""
    cur = conn.cursor()
    print("Creating indexes...")
    for field in ['user_id', 'content_id', 'country_code', 'device_type']:
        cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_ts_{field} ON {table_name} (event_timestamp DESC, {field});")
    cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_ts_brin ON {table_name} USING BRIN(event_timestamp);")
    conn.commit()
    cur.close()
    print("✓ Indexes created")


# --- Hybrid Strategy Functions ---
def create_partitioned_table(conn, source_table_name, target_table_name):
    """Creates a partitioned table and partitions it by month."""
    cur = conn.cursor()
    print(f"\nCreating partitioned table '{target_table_name}'...")
    
    # Create partitioned table
    cur.execute(f"DROP TABLE IF EXISTS {target_table_name} CASCADE;")
    cur.execute(f"""
        CREATE TABLE {target_table_name} (
            event_id BIGSERIAL PRIMARY KEY, user_id INTEGER NOT NULL,
            content_id INTEGER NOT NULL, event_timestamp TIMESTAMPTZ NOT NULL,
            event_type VARCHAR(50), watch_duration_seconds INTEGER,
            device_type VARCHAR(50), country_code VARCHAR(2),
            quality VARCHAR(10), bandwidth_mbps DECIMAL(6,2),
            created_at TIMESTAMPTZ DEFAULT NOW()
        ) PARTITION BY RANGE (event_timestamp);
    """)
    
    # Create partitions by month
    cur.execute(f"SELECT MIN(event_timestamp), MAX(event_timestamp) FROM {source_table_name};")
    min_date, max_date = cur.fetchone()
    start_date = min_date.replace(day=1)
    end_date = max_date.replace(day=1) + relativedelta(months=2)  # buffer for future data
    current_date = start_date
    
    while current_date <= end_date:
        partition_name = f"{target_table_name}_{current_date.strftime('%Y_%m')}"
        cur.execute(f"SELECT to_regclass('{partition_name}');")
        if cur.fetchone()[0] is None:
            cur.execute(f"""
                CREATE TABLE {partition_name} PARTITION OF {target_table_name} 
                FOR VALUES FROM ('{current_date.isoformat()}') TO ('{(current_date + relativedelta(months=1)).isoformat()}');
            """)
        current_date += relativedelta(months=1)
    conn.commit()
    cur.close()
    print("✓ Partitioned table created")


# --- Query & Performance Testing ---
def test_query_performance(conn, query, iterations=5):
    """Test the performance of a query."""
    cursor = conn.cursor()
    times = []
    for _ in range(iterations):
        start = time.perf_counter()
        cursor.execute(query)
        cursor.fetchall()
        times.append(time.perf_counter() - start)
    cursor.close()
    return {'avg_time': sum(times) / len(times)}


# --- Main Execution Logic ---
if __name__ == "__main__":
    try:
        conn = connect_db()
        conn.autocommit = True
        
        base_table = "viewing_events_base"
        indexed_table = "viewing_events_indexed"
        hybrid_table = "viewing_events_hybrid"
        
        # --- PHASE 1: BASELINE (NO INDEXES) ---
        create_table(conn, base_table)
        generate_events(conn, base_table, NUM_DAYS, EVENTS_PER_DAY)
        
        # Test base queries
        base_queries = get_queries(base_table)
        base_results = {name: test_query_performance(conn, query) for name, query in base_queries.items()}
        
        # --- PHASE 2: INDEXED MONOLITHIC ---
        create_table(conn, indexed_table)
        generate_events(conn, indexed_table, NUM_DAYS, EVENTS_PER_DAY)
        create_indexes(conn, indexed_table)
        
        indexed_queries = get_queries(indexed_table)
        indexed_results = {name: test_query_performance(conn, query) for name, query in indexed_queries.items()}
        
        # --- PHASE 3: HYBRID OPTIMIZATION ---
        create_partitioned_table(conn, base_table, hybrid_table)
        generate_events(conn, hybrid_table, NUM_DAYS, EVENTS_PER_DAY)
        
        hybrid_queries = get_queries(hybrid_table)
        hybrid_results = {name: test_query_performance(conn, query) for name, query in hybrid_queries.items()}
        
        # --- FINAL PERFORMANCE REPORT ---
        print("\nPerformance Summary:")
        for phase, results in [('Base', base_results), ('Indexed', indexed_results), ('Hybrid', hybrid_results)]:
            print(f"\n{phase} Queries:")
            for query_name, result in results.items():
                print(f"{query_name}: {result['avg_time']:.4f}s")

    except psycopg2.Error as e:
        print(f"Error: {e}", file=sys.stderr)
    finally:
        if conn:
            conn.close()
        print("\nDatabase connection closed.")
