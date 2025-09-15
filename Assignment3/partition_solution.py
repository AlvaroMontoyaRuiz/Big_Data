import psycopg2
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import time
from decimal import Decimal
import sys
import os
import random
import math
import io

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

def create_monolithic_table(conn):
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
    print("Monolithic base table created")

def generate_viewing_events_generate_series(conn, num_days=90, events_per_day=200_000):
    """
    Generates realistic viewing events using SQL's generate_series.
    """
    cur = conn.cursor()
    end_date = datetime.now()
    start_date = end_date - timedelta(days=num_days)

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
                (ARRAY['start','pause','resume','complete','skip'])[FLOOR(random()*5 + 1)] AS event_type,
                (FLOOR(random() * 3571) + 30)::int AS watch_duration_seconds,
                (ARRAY['mobile','tv','web','tablet'])[FLOOR(random()*4 + 1)] AS device_type,
                (ARRAY['US','UK','CA','AU','OT'])[FLOOR(random()*5 + 1)] AS country_code,
                (ARRAY['480p','720p','1080p','4K'])[FLOOR(random()*4 + 1)] AS quality,
                ROUND((random() * 49 + 1)::numeric, 2) AS bandwidth_mbps
            FROM generate_series('{start_date}'::timestamp, '{end_date}'::timestamp, interval '1 day') AS gs(day),
                 generate_series(1, {events_per_day}) AS gs2
        """)
        conn.commit()
        print("All events generated")
    except psycopg2.Error as e:
        print(f"Error generating data: {e}", file=sys.stderr)
        conn.rollback()
    finally:
        cur.close()

# --- SQL for partitioned table and partition creation ---
def create_partitioned_table(conn):
    """
    Creates the main partitioned table with a primary key that includes the partition key.
    """
    cur = conn.cursor()
    cur.execute("""
        DROP TABLE IF EXISTS viewing_events_partitioned CASCADE;

        CREATE TABLE viewing_events_partitioned (
            event_id BIGSERIAL,
            user_id INTEGER NOT NULL,
            content_id INTEGER NOT NULL,
            event_timestamp TIMESTAMPTZ NOT NULL,
            event_type VARCHAR(50),
            watch_duration_seconds INTEGER,
            device_type VARCHAR(50),
            country_code VARCHAR(2),
            quality VARCHAR(10),
            bandwidth_mbps DECIMAL(6,2),
            created_at TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (event_id, event_timestamp)
        ) PARTITION BY RANGE (event_timestamp);
    """)
    conn.commit()
    cur.close()
    print("Partitioned table 'viewing_events_partitioned' created")

# --- Query functions ---
def get_daily_active_users_query(table_name, days=7):
    return f"""
        SELECT event_timestamp::date AS day, COUNT(DISTINCT user_id) AS dau
        FROM {table_name}
        WHERE event_timestamp >= NOW() - INTERVAL '{days} days'
        GROUP BY day
        ORDER BY day;
    """

def get_top_content_query(table_name, days=1, limit=10):
    return f"""
        SELECT content_id, COUNT(*) AS views
        FROM {table_name}
        WHERE event_timestamp >= NOW() - INTERVAL '{days} days'
        GROUP BY content_id
        ORDER BY views DESC
        LIMIT {limit};
    """

def get_device_usage_query(table_name, days=30):
    return f"""
        SELECT device_type, COUNT(*) AS views
        FROM {table_name}
        WHERE event_timestamp >= NOW() - INTERVAL '{days} days'
        GROUP BY device_type
        ORDER BY views DESC;
    """

def get_content_performance_query(table_name, days=30, limit=10):
    return f"""
        SELECT
            content_id, 
            COUNT(*) AS views,
            SUM(CASE WHEN event_type = 'complete' THEN 1 ELSE 0 END) AS completions
        FROM {table_name}
        WHERE event_timestamp >= NOW() - INTERVAL '{days} days' AND event_type IN ('start', 'complete')
        GROUP BY content_id
        ORDER BY views DESC
        LIMIT {limit};
    """

def get_regional_analytics_query(table_name, days=30):
    return f"""
        SELECT country_code, COUNT(*) AS total_views
        FROM {table_name}
        WHERE event_timestamp >= NOW() - INTERVAL '{days} days'
        GROUP BY country_code
        ORDER BY total_views DESC;
    """

def get_user_engagement_query(table_name, days=7):
    return f"""
        SELECT
            user_id,
            SUM(watch_duration_seconds) AS total_watch_time
        FROM {table_name}
        WHERE event_timestamp >= NOW() - INTERVAL '{days} days'
        GROUP BY user_id
        ORDER BY total_watch_time DESC
        LIMIT 10;
    """

def test_query_performance(conn, query, iterations=5):
    """
    Test query performance with a number of iterations.
    """
    cursor = conn.cursor()
    times = []
    
    # Warm up the cache by running the query once
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

# Partition class
class StreamFlixPartitionManager:
    """Automated partition management for StreamFlix viewing events."""

    def __init__(self, conn_params):
        self.conn_params = conn_params
        self.log_file = "migration_progress.log"

    def get_total_rows(self):
        """Helper to get the total number of rows from the source table."""
        conn = psycopg2.connect(**self.conn_params)
        cur = conn.cursor()
        try:
            cur.execute("SELECT COUNT(*) FROM viewing_events;")
            total = cur.fetchone()[0]
            return total
        finally:
            cur.close()
            conn.close()

    def get_last_migrated_id(self):
        """Resumes migration from the last successfully processed record."""
        if os.path.exists(self.log_file):
            with open(self.log_file, 'r') as f:
                last_id_str = f.read().strip()
                return int(last_id_str) if last_id_str and last_id_str.isdigit() else 0
        return 0

    def get_rows_migrated_so_far(self, last_id):
        """Gets the count of rows already migrated up to the last known ID."""
        conn = psycopg2.connect(**self.conn_params)
        cur = conn.cursor()
        try:
            cur.execute("SELECT COUNT(*) FROM viewing_events WHERE event_id <= %s;", (last_id,))
            return cur.fetchone()[0]
        finally:
            cur.close()
            conn.close()

    def log_progress(self, last_id):
        """Saves the last processed ID to a log file."""
        with open(self.log_file, 'w') as f:
            f.write(str(last_id))

    def get_data_date_range(self):
        """Queries the monolithic table to find the min and max dates of the data."""
        conn = psycopg2.connect(**self.conn_params)
        cur = conn.cursor()
        try:
            cur.execute("SELECT MIN(event_timestamp), MAX(event_timestamp) FROM viewing_events;")
            min_ts, max_ts = cur.fetchone()
            return min_ts, max_ts
        finally:
            cur.close()
            conn.close()
            
    def create_monthly_partitions(self, start_date, num_months):
        """
        Creates monthly partitions for the specified range, including CHECK constraints and indexes.
        """
        try:
            conn = psycopg2.connect(**self.conn_params)
            cur = conn.cursor()
            
            print(f"Creating {num_months} monthly partitions starting from {start_date.strftime('%Y-%m')}...")
            
            current_date = start_date.replace(day=1)
            for i in range(num_months):
                start_of_month = current_date
                # Get the first day of the next month
                end_of_month = (current_date + relativedelta(months=1))
                partition_name = f"viewing_events_{start_of_month.strftime('%Y_%m')}"
                
                cur.execute(f"SELECT to_regclass('{partition_name}');")
                if cur.fetchone()[0] is not None:
                    print(f"Partition {partition_name} already exists. Skipping.")
                    current_date = end_of_month
                    continue

                # Create partition with explicit CHECK constraints for partition pruning
                print(f"Creating partition {partition_name}...")
                cur.execute(f"""
                    CREATE TABLE {partition_name} PARTITION OF viewing_events_partitioned
                    FOR VALUES FROM ('{start_of_month.isoformat()}') TO ('{end_of_month.isoformat()}');
                """)
                
                # Add indexes to the new partition
                print(f"Creating indexes on {partition_name}...")
                cur.execute(f"CREATE INDEX idx_{partition_name}_ts_uid ON {partition_name} (event_timestamp DESC, user_id);")
                cur.execute(f"CREATE INDEX idx_{partition_name}_ts_cid ON {partition_name} (event_timestamp DESC, content_id);")
                cur.execute(f"CREATE INDEX idx_{partition_name}_ts_cc ON {partition_name} (event_timestamp DESC, country_code);")
                cur.execute(f"CREATE INDEX idx_{partition_name}_ts_dt ON {partition_name} (event_timestamp DESC, device_type);")

                conn.commit()
                print(f"Partition {partition_name} created successfully.")
                current_date = end_of_month

        except psycopg2.Error as e:
            conn.rollback()
            print(f"Error creating partitions: {e}", file=sys.stderr)
        finally:
            if conn:
                cur.close()
                conn.close()

    def migrate_data_to_partitioned(self, batch_size=50000):
        """
        Migrates data from the monolithic table to the partitioned table in batches
        """
        conn = psycopg2.connect(**self.conn_params)
        cur = conn.cursor()
        
        last_migrated_id = self.get_last_migrated_id()
        total_rows = self.get_total_rows()
        migrated_rows = self.get_rows_migrated_so_far(last_migrated_id) if last_migrated_id > 0 else 0

        print(f"Starting data migration. Total rows: {total_rows}. Already migrated: {migrated_rows}")
        if last_migrated_id > 0:
            print(f"Resuming migration from record with event_id > {last_migrated_id}")

        try:
            while True:
                # Use a server-side cursor for memory efficiency with large result sets
                with conn.cursor('server_cursor') as fetch_cur:
                    fetch_cur.execute("""
                        SELECT 
                            event_id, user_id, content_id, event_timestamp, event_type,
                            watch_duration_seconds, device_type, country_code, quality,
                            bandwidth_mbps, created_at
                        FROM viewing_events
                        WHERE event_id > %s
                        ORDER BY event_id ASC
                        LIMIT %s;
                    """, (last_migrated_id, batch_size))
                    
                    batch_data = fetch_cur.fetchall()
                
                if not batch_data:
                    print("\nAll data has been migrated.")
                    break
                
                # Format data for COPY command
                data_stream = io.StringIO()
                for row in batch_data:
                    # Convert row to a tab-separated string, handling None values for NULL
                    # and converting timestamps to the correct format for COPY.
                    row_str = '\t'.join(
                        str(col) if col is not None else '' 
                        for col in row
                    )
                    data_stream.write(row_str + '\n')
                data_stream.seek(0) # Rewind the stream to the beginning

                # Execute the COPY command to insert the batch
                cur.copy_expert(
                    "COPY viewing_events_partitioned FROM STDIN WITH (FORMAT TEXT)", 
                    data_stream
                )
                conn.commit()

                last_migrated_id = batch_data[-1][0]
                migrated_rows += len(batch_data)
                self.log_progress(last_migrated_id)

                progress_percent = (migrated_rows / total_rows) * 100
                print(f"Migration progress: {progress_percent:.2f}% ({migrated_rows}/{total_rows} rows)")

            # Final verification
            cur.execute("SELECT COUNT(*) FROM viewing_events;")
            original_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM viewing_events_partitioned;")
            partitioned_count = cur.fetchone()[0]

            if partitioned_count == original_count:
                print("\n Data migration complete and integrity verified.")
                if os.path.exists(self.log_file):
                    os.remove(self.log_file)
            else:
                print(f"\n Data count mismatch. Original: {original_count}, Partitioned: {partitioned_count}")

        except psycopg2.Error as e:
            conn.rollback()
            print(f"Error during migration: {e}", file=sys.stderr)
            print("Migration failed. You can resume by re-running the script.")
        finally:
            cur.close()
            conn.close()

    def analyze_partition_performance(self, run_maintenance_test=True):
        """
        Compares performance between monolithic and partitioned tables.
        
        Args:
            run_maintenance_test (bool): If True, performs the destructive DROP TABLE test.
        """
        results = {}
        queries_to_test = {
            "daily_active_users": get_daily_active_users_query("viewing_events", days=7),
            "top_10_content_last_24h": get_top_content_query("viewing_events", days=1),
            "device_usage_last_month": get_device_usage_query("viewing_events", days=30),
            "content_performance_last_30_days": get_content_performance_query("viewing_events", days=30),
            "regional_analytics_last_30_days": get_regional_analytics_query("viewing_events", days=30),
            "most_engaged_users_last_7_days": get_user_engagement_query("viewing_events", days=7)
        }

        try:
            conn = psycopg2.connect(**self.conn_params)
            
            print("\n--- Comparing Query Performance ---")
            for name, query in queries_to_test.items():
                print(f"\nTesting query: {name}")

                # Monolithic performance
                monolithic_timings = test_query_performance(conn, query)
                
                # Partitioned performance
                partitioned_query = query.replace("viewing_events", "viewing_events_partitioned")
                partitioned_timings = test_query_performance(conn, partitioned_query)

                monolithic_time = monolithic_timings['avg_time']
                partitioned_time = partitioned_timings['avg_time']
                
                improvement = (monolithic_time - partitioned_time) / monolithic_time * 100 if monolithic_time > 0 else 0
                
                results[name] = {
                    "monolithic_avg_time": monolithic_time,
                    "monolithic_min_time": monolithic_timings['min_time'],
                    "monolithic_max_time": monolithic_timings['max_time'],
                    "partitioned_avg_time": partitioned_time,
                    "partitioned_min_time": partitioned_timings['min_time'],
                    "partitioned_max_time": partitioned_timings['max_time'],
                    "improvement_percent": Decimal(improvement).quantize(Decimal('0.01'))
                }
            
            # Maintenance Operation Performance (conditional)
            if run_maintenance_test:
                print("\n--- Comparing Maintenance Performance (Data Deletion) ---")
                deletion_date = datetime.now() - relativedelta(months=3)
                monolithic_delete_query = f"DELETE FROM viewing_events WHERE event_timestamp >= '{deletion_date}' AND event_timestamp < '{(deletion_date + relativedelta(months=1))}'"
                partitioned_drop_query = f"DROP TABLE viewing_events_{deletion_date.strftime('%Y_%m')}"

                # Monolithic DELETE test
                conn.rollback() # Ensure transaction is clean
                print("Testing DELETE on monolithic table...")
                start_time = time.perf_counter()
                cur = conn.cursor()
                cur.execute(monolithic_delete_query)
                conn.commit()
                monolithic_delete_time = time.perf_counter() - start_time
                cur.close()
                
                # Partitioned DROP TABLE test
                conn.rollback()
                print("Testing DROP TABLE on partitioned table...")
                start_time = time.perf_counter()
                cur = conn.cursor()
                try:
                    cur.execute(partitioned_drop_query)
                    conn.commit()
                except psycopg2.Error as e:
                    print(f"Error dropping table: {e}. This likely means the partition did not exist.")
                    partitioned_drop_time = float('inf') # Set to infinity to show it's slow/failed
                    conn.rollback()
                else:
                    partitioned_drop_time = time.perf_counter() - start_time
                finally:
                    cur.close()

                drop_improvement = (monolithic_delete_time - partitioned_drop_time) / monolithic_delete_time * 100 if monolithic_delete_time > 0 and partitioned_drop_time != float('inf') else 0
                
                results["data_deletion"] = {
                    "monolithic_time": monolithic_delete_time,
                    "partitioned_time": partitioned_drop_time,
                    "improvement_percent": Decimal(drop_improvement).quantize(Decimal('0.01'))
                }

        except psycopg2.Error as e:
            print(f"Error during performance analysis: {e}", file=sys.stderr)
            conn.rollback()
        finally:
            if conn:
                conn.close()
        
        return results

if __name__ == "__main__":
    # Check for the --clean or -c argument
    if len(sys.argv) > 1 and (sys.argv[1] == '--clean' or sys.argv[1] == '-c'):
        log_file = "migration_progress.log"
        if os.path.exists(log_file):
            print("Argument found. Deleting migration log file to start fresh.")
            os.remove(log_file)
        else:
            print("Argument found, but no log file exists. Starting fresh.")
    else:
        print("No cleanup argument found. Will attempt to resume migration if a log file exists.")

    conn_params = {
        "host": "localhost",
        "database": "streamflix",
        "user": "student",
        "password": "student"
    }

    # Connect and create/populate the original monolithic table
    conn = connect_db()
    create_monolithic_table(conn)
    generate_viewing_events_generate_series(conn, NUM_DAYS, EVENTS_PER_DAY)
    conn.close()

    # Instantiate the partition manager
    manager = StreamFlixPartitionManager(conn_params)
    
    # Create partitioned table structure
    conn = connect_db()
    create_partitioned_table(conn)
    conn.close()

    # Get the date range of the generated data
    min_date, max_date = manager.get_data_date_range()
    
    if min_date and max_date:
        # Calculate the number of months between the two dates
        # Add a small buffer to ensure the last month is included
        num_months_needed = (max_date.year - min_date.year) * 12 + (max_date.month - min_date.month) + 1
        print(f"Data spans from {min_date.strftime('%Y-%m')} to {max_date.strftime('%Y-%m')}, requiring approximately {num_months_needed} partitions.")
        manager.create_monthly_partitions(min_date, num_months_needed)
    else:
        print("No data found in the monolithic table to determine partition range.")

    # Migrate data from the old table to the new one
    manager.migrate_data_to_partitioned()

    # Get user input to decide whether to run the maintenance test
    run_maintenance = input("Do you want to run the destructive data deletion test? (yes/no): ").lower() == 'yes'

    # Run performance analysis with the user's choice
    performance_report = manager.analyze_partition_performance(run_maintenance_test=run_maintenance)

    print("\n--- Performance Comparison Report (Partitioned vs. Monolithic) ---")
    for query_name, data in performance_report.items():
        if query_name == "data_deletion":
            print(f"\nMaintenance Operation: {query_name.replace('_', ' ').title()}")
            monolithic_time_str = f"{data['monolithic_time']:.4f} seconds"
            partitioned_time_str = f"{data['partitioned_time']:.4f} seconds" if data['partitioned_time'] != float('inf') else "N/A (Partition not found)"
            
            print(f"   - Monolithic Table Time (DELETE): {monolithic_time_str}")
            print(f"   - Partitioned Table Time (DROP): {partitioned_time_str}")
            print(f"   - Performance Improvement: {data['improvement_percent']}%")
        else:
            print(f"\nQuery: {query_name.replace('_', ' ').title()}")
            print(f"   - Average Time (Monolithic): {data['monolithic_avg_time']:.4f} seconds")
            print(f"   - Average Time (Partitioned):  {data['partitioned_avg_time']:.4f} seconds")
            print(f"   - Performance Improvement: {data['improvement_percent']}%")
            print(f"   - Timing Range Before (Min/Max): {data['monolithic_min_time']:.4f} / {data['monolithic_max_time']:.4f} seconds")
            print(f"   - Timing Range After (Min/Max):  {data['partitioned_min_time']:.4f} / {data['partitioned_max_time']:.4f} seconds")
    
    print("\nNote: The `DROP TABLE` operation on a partitioned table is nearly instantaneous, showcasing the immense benefit of this strategy for data archival and deletion.")