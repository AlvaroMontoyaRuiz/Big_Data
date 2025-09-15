import psycopg2
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from decimal import Decimal
import os
import sys
import time

def get_conn(params):
    return psycopg2.connect(**params)

def run_query(conn, query, params=None):
    with conn.cursor() as cur:
        cur.execute(query, params or ())
        if cur.description:
            return cur.fetchall()
        return None

def connect_db():
    return get_conn({
        "host": "localhost",
        "database": "streamflix",
        "user": "student",
        "password": "student"
    })

def create_monolithic_table(conn):
    run_query(conn, """
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
    print("✓ Monolithic base table created")

def generate_viewing_events(conn, num_days=90, per_day=50000):
    start = (datetime.now() - timedelta(days=num_days)).date()
    end = datetime.now().date()
    print(f"Inserting {num_days * per_day} events using generate_series...")
    run_query(conn, f"""
        INSERT INTO viewing_events (
            user_id, content_id, event_timestamp, event_type,
            watch_duration_seconds, device_type, country_code,
            quality, bandwidth_mbps
        )
        SELECT
            (FLOOR(POW(random(), 2) * 100000) + 1)::int,
            (FLOOR(POW(random(), 2) * 10000) + 1)::int,
            gs.day
            + make_interval(hours := GREATEST(0, LEAST(23, ROUND(20 + (random() - 0.5) * 4)::int)))
            + make_interval(mins := FLOOR(random() * 60)::int)
            + make_interval(secs := FLOOR(random() * 60)::int),
            (ARRAY['start','pause','resume','complete','skip'])[FLOOR(random()*5 + 1)],
            (FLOOR(random() * 3571) + 30)::int,
            (ARRAY['mobile','tv','web','tablet'])[FLOOR(random()*4 + 1)],
            (ARRAY['US','UK','CA','AU','OT'])[FLOOR(random()*5 + 1)],
            (ARRAY['480p','720p','1080p','4K'])[FLOOR(random()*4 + 1)],
            ROUND((random() * 49 + 1)::numeric, 2)
        FROM generate_series('{start}'::timestamp, '{end}'::timestamp, interval '1 day') AS gs(day),
            generate_series(1, {per_day}) gs2;
    """)
    conn.commit()
    print("✓ Events generated")

def create_partitioned_table(conn):
    run_query(conn, """
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
    print("✓ Partitioned table created")

def query_min_max_dates(conn):
    res = run_query(conn, "SELECT MIN(event_timestamp), MAX(event_timestamp) FROM viewing_events")
    return res if res else (None, None)

def create_monthly_partitions(conn, start, num_months):
    for i in range(num_months):
        current = (start + relativedelta(months=i)).replace(day=1)
        next_month = current + relativedelta(months=1)
        name = f"viewing_events_{current.strftime('%Y_%m')}"
        exists = run_query(conn, f"SELECT to_regclass('{name}');")
        if exists: continue
        run_query(conn, f"""
            CREATE TABLE {name} PARTITION OF viewing_events_partitioned
            FOR VALUES FROM ('{current.isoformat()}') TO ('{next_month.isoformat()}');
            CREATE INDEX idx_{name}_ts_uid ON {name} (event_timestamp DESC, user_id);
            CREATE INDEX idx_{name}_ts_cid ON {name} (event_timestamp DESC, content_id);
        """)
        conn.commit()
        print(f"✓ Partition {name} created")

def migrate_data(conn, batch=50000):
    cur = conn.cursor()
    last_id = 0
    while True:
        cur.execute("""
            SELECT * FROM viewing_events WHERE event_id > %s ORDER BY event_id ASC LIMIT %s;
        """, (last_id, batch))
        rows = cur.fetchall()
        if not rows: break
        for row in rows:
            ts = row[3]
            pname = f"viewing_events_{ts.strftime('%Y_%m')}"
            exists = run_query(conn, f"SELECT to_regclass('{pname}');")
            if not exists:
                start = ts.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                end = start + relativedelta(months=1)
                run_query(conn, f"""
                    CREATE TABLE {pname} PARTITION OF viewing_events_partitioned
                    FOR VALUES FROM ('{start}') TO ('{end}');
                """)
                conn.commit()
        insert_sql = """
            INSERT INTO viewing_events_partitioned (
                event_id, user_id, content_id, event_timestamp, event_type,
                watch_duration_seconds, device_type, country_code,
                quality, bandwidth_mbps, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        with conn.cursor() as icur:
            icur.executemany(insert_sql, rows)
        conn.commit()
        last_id = rows[-1]
        print(f"✓ Migrated up to event_id {last_id}")
    cur.close()

# Example SQL query generators omitted for brevity (use as in the original code)

def main():
    if len(sys.argv) > 1 and sys.argv[1] in ('--clean', '-c'):
        lf = "migration_progress.log"
        if os.path.exists(lf):
            print("Cleaning up migration log.")
            os.remove(lf)
    conn_params = {
        "host": "localhost", "database": "streamflix",
        "user": "student", "password": "student"
    }
    with get_conn(conn_params) as conn:
        create_monolithic_table(conn)
        generate_viewing_events(conn, num_days=60, per_day=10000)
    with get_conn(conn_params) as conn:
        create_partitioned_table(conn)
        min_date, max_date = query_min_max_dates(conn)
        if min_date and max_date:
            n_months = (max_date.year - min_date.year) * 12 + (max_date.month - min_date.month) + 1
            create_monthly_partitions(conn, min_date, n_months)
        else:
            print("No data in monolithic table.")
    with get_conn(conn_params) as conn:
        migrate_data(conn)

if __name__ == "__main__":
    main()
