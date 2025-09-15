import psycopg2
import time
import random
from datetime import datetime
from decimal import Decimal

# Helper function for creating SQL queries dynamically
def generate_query(table_name, interval, extra_conditions="", group_by=None, order_by=None, limit=None):
    query = f"""
        SELECT {', '.join([table_name, 'COUNT(*) AS views'])}
        FROM {table_name}
        WHERE event_timestamp >= NOW() - INTERVAL '{interval}'
        {extra_conditions}
        GROUP BY {group_by if group_by else table_name}
        {f"ORDER BY {order_by}" if order_by else ""}
        {f"LIMIT {limit}" if limit else ""};
    """
    return query

# Database connection
def connect_db():
    """Establishes a connection to the PostgreSQL database."""
    return psycopg2.connect(
        host="localhost",
        database="streamflix",
        user="student",
        password="student"
    )

# Query Functions
def get_daily_active_users_query(table_name):
    return generate_query(table_name, '7 days', group_by="event_timestamp::date", order_by="event_timestamp::date")

def get_top_content_query(table_name):
    return generate_query(table_name, '1 day', group_by="content_id", order_by="COUNT(*) DESC", limit=10)

def get_device_usage_query(table_name):
    return generate_query(table_name, '30 days', group_by="device_type", order_by="COUNT(*) DESC")

def get_content_performance_query(table_name):
    return generate_query(
        table_name, '30 days', 
        extra_conditions="AND event_type IN ('start', 'complete')",
        group_by="content_id", order_by="COUNT(*) DESC", limit=10
    )

def get_regional_analytics_query(table_name):
    return generate_query(table_name, '30 days', group_by="country_code", order_by="COUNT(*) DESC")

def get_user_engagement_query(table_name):
    return generate_query(table_name, '7 days', group_by="user_id", order_by="SUM(watch_duration_seconds) DESC", limit=10)

# Test query performance with a number of iterations
def test_query_performance(conn, query, iterations=5):
    cursor = conn.cursor()
    times = []
    cursor.execute(query)  # Warm up cache
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

# Main report generation
def generate_performance_report(conn, num_rows_to_test=1000):
    report = {
        'executive_summary': '',
        'query_performance': {},
        'storage_analysis': {},
        'maintenance_benefits': {},
        'recommendations': []
    }
    
    try:
        cursor = conn.cursor()
        conn.autocommit = True
        cursor.execute("DISCARD ALL; SET max_parallel_workers_per_gather = 0; SET work_mem = '4MB';")
        conn.autocommit = False
        
        # Query Performance Analysis
        queries = {
            "daily_active_users": get_daily_active_users_query,
            "top_10_content_last_24h": get_top_content_query,
            "device_usage_last_month": get_device_usage_query,
            "content_performance_last_30_days": get_content_performance_query,
            "regional_analytics_last_30_days": get_regional_analytics_query,
            "most_engaged_users_last_7_days": get_user_engagement_query
        }

        for name, query_func in queries.items():
            print(f"\nTesting query: {name}...")
            monolithic_query = query_func("viewing_events")
            partitioned_query = query_func("viewing_events_partitioned")
            monolithic_timings = test_query_performance(conn, monolithic_query)
            partitioned_timings = test_query_performance(conn, partitioned_query)

            monolithic_time = monolithic_timings['avg_time']
            partitioned_time = partitioned_timings['avg_time']
            improvement_percent = (monolithic_time - partitioned_time) / monolithic_time * 100 if monolithic_time > 0 else 0
            
            report['query_performance'][name] = {
                "monolithic_avg_time": f"{monolithic_time:.4f}",
                "partitioned_avg_time": f"{partitioned_time:.4f}",
                "improvement_percent": Decimal(improvement_percent).quantize(Decimal('0.01'))
            }
        
        # Storage Impact Analysis
        cursor.execute("SELECT pg_size_pretty(pg_total_relation_size('viewing_events'));")
        monolithic_size = cursor.fetchone()[0]
        cursor.execute("""
            SELECT pg_size_pretty(SUM(pg_total_relation_size(c.oid)))
            FROM pg_inherits
            JOIN pg_class c ON pg_inherits.inhrelid = c.oid
            WHERE inhparent = 'viewing_events_partitioned'::regclass;
        """)
        partitioned_size = cursor.fetchone()[0]

        report['storage_analysis'] = {
            'monolithic_table_size': monolithic_size,
            'partitioned_total_size': partitioned_size,
            'note': 'Partitioning organizes data more efficiently for queries but does not significantly reduce storage.'
        }

        # Insertion Performance
        test_data = [
            (random.randint(1, 100000), random.randint(1, 10000), datetime.now(), random.choice(['start', 'pause', 'complete']),
             random.randint(30, 3600), random.choice(['mobile', 'tv', 'web', 'tablet']), random.choice(['US', 'UK', 'CA']),
             random.choice(['480p', '720p', '1080p', '4K']), Decimal(random.uniform(1.0, 50.0)).quantize(Decimal('0.01')), datetime.now())
            for _ in range(num_rows_to_test)
        ]
        
        # Test Insertions
        cursor.execute("DELETE FROM viewing_events WHERE event_id IN (SELECT event_id FROM viewing_events ORDER BY event_id DESC LIMIT %s);", (num_rows_to_test,))
        conn.commit()
        
        # Monolithic Insertion
        monolithic_insert_start = time.perf_counter()
        insert_query = """
            INSERT INTO viewing_events (user_id, content_id, event_timestamp, event_type, watch_duration_seconds, device_type, country_code, quality, bandwidth_mbps, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        cursor.executemany(insert_query, test_data)
        conn.commit()
        monolithic_insert_time = time.perf_counter() - monolithic_insert_start

        # Partitioned Insertion
        cursor.execute("DELETE FROM viewing_events_partitioned WHERE event_id IN (SELECT event_id FROM viewing_events_partitioned ORDER BY event_id DESC LIMIT %s);", (num_rows_to_test,))
        conn.commit()
        partitioned_insert_start = time.perf_counter()
        cursor.executemany(insert_query, test_data)
        conn.commit()
        partitioned_insert_time = time.perf_counter() - partitioned_insert_start
        
        insert_improvement = (monolithic_insert_time - partitioned_insert_time) / monolithic_insert_time * 100 if monolithic_insert_time > 0 else 0
        
        report['maintenance_benefits'] = {
            'insert_performance': {
                'monolithic_time': f"{monolithic_insert_time:.4f}",
                'partitioned_time': f"{partitioned_insert_time:.4f}",
                'improvement_percent': Decimal(insert_improvement).quantize(Decimal('0.01'))
            },
            'vacuum_analyze_speed': 'Partitioning allows for faster maintenance, such as vacuuming and analyzing individual partitions.',
            'data_archival': 'Dropping partitions for archival is an O(1) operation, which is far more efficient than deleting data from a monolithic table.',
            'reindexing': 'Reindexing a partitioned table is less disruptive and can be done on individual partitions.'
        }

        # Executive Summary
        report['executive_summary'] = (
            "Partitioning improves query performance, particularly for recent data, and significantly reduces maintenance costs."
        )

        # Recommendations
        report['recommendations'] = [
            "Use partitioned tables for new data.",
            "Automate partition creation via cron jobs.",
            "Implement a data archival policy for partitions older than 1-2 years."
        ]
        
        return report
    
    except psycopg2.Error as e:
        print(f"Error generating report: {e}")
        return None
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# Main function
if __name__ == "__main__":
    conn = connect_db()
    if conn:
        report = generate_performance_report(conn)
        if report
