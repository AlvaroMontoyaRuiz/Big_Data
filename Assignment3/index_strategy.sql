-- This file contains all SQL commands to create indexes for the `viewing_events` table.
-- The strategies are chosen based on the data access patterns of typical analytics queries.

-- Use a BRIN index for the event_timestamp column.
-- BRIN (Block Range Index) is highly efficient for large, naturally ordered datasets like time-series data.
-- It stores only the min and max values for a range of pages, making it compact and fast for time-range filters.
CREATE INDEX IF NOT EXISTS idx_viewing_events_timestamp_brin 
ON viewing_events USING BRIN(event_timestamp);

-- Use composite B-Tree indexes for event_timestamp and secondary dimensions.
-- B-Tree indexes are versatile and ideal for range queries and sorting.

-- For "daily active users" queries: filter by time, then group by user.
CREATE INDEX IF NOT EXISTS idx_viewing_events_timestamp_user_id 
ON viewing_events (event_timestamp DESC, user_id);

-- For content performance analytics: filter by time, then analyze content.
CREATE INDEX IF NOT EXISTS idx_viewing_events_timestamp_content_id 
ON viewing_events (event_timestamp DESC, content_id);

-- For regional analytics: filter by time, then aggregate by country.
CREATE INDEX IF NOT EXISTS idx_viewing_events_timestamp_country 
ON viewing_events (event_timestamp DESC, country_code);

-- For device-specific analytics: filter by time, then narrow by device type.
CREATE INDEX IF NOT EXISTS idx_viewing_events_timestamp_device 
ON viewing_events (event_timestamp DESC, device_type);

-- No HASH indexes are used.
-- HASH indexes are fast for equality lookups but don't support range queries or sorting.
-- They are also not crash-safe in some PostgreSQL versions.
-- Since our queries rely on ranges and sorting by event_timestamp, B-Tree indexes are preferred.

-- Update table statistics to help the query planner choose optimal indexes.
ANALYZE viewing_events;
