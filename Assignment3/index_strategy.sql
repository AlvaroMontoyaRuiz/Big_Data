-- This file contains all SQL commands to create indexes for the `viewing_events` table.
-- The strategies are chosen based on the data access patterns of typical analytics queries.

-- Use a BRIN index for the event_timestamp column.
-- BRIN stands for Block Range Index. It's highly efficient for large, naturally ordered datasets,
-- like time-series data, because it only stores the min and max values for a range of pages.
-- This makes it very small and fast for queries that filter by a time range (e.g., "last 7 days").
CREATE INDEX IF NOT EXISTS idx_viewing_events_timestamp_brin ON viewing_events USING BRIN(event_timestamp);

-- Use a composite B-Tree index for `event_timestamp` and `user_id`.
-- B-Tree indexes are the standard and most versatile index type in PostgreSQL.
-- A composite index on these two columns is ideal for queries that filter by a time range
-- and then group or count by user ID, such as the "daily active users" query. The `event_timestamp`
-- is the leading column because it's the most selective filter in these queries.
CREATE INDEX IF NOT EXISTS idx_viewing_events_timestamp_user_id ON viewing_events (event_timestamp DESC, user_id);

-- Use a composite B-Tree index for `event_timestamp` and `content_id`.
-- Similar to the user_id index, this one optimizes queries that need to filter by time
-- and then analyze content performance (e.g., "top content last week").
-- The B-Tree structure allows for efficient lookups on the first column (`event_timestamp`)
-- and then a quick scan for the second column (`content_id`).
CREATE INDEX IF NOT EXISTS idx_viewing_events_timestamp_content_id ON viewing_events (event_timestamp DESC, content_id);

-- Use a composite B-Tree index for `event_timestamp` and `country_code`.
-- This index supports queries for regional analytics, which typically filter on a time range
-- and then aggregate by country. The B-Tree is suitable here as the number of unique
-- country codes is relatively small, but the queries still rely on the time filter.
CREATE INDEX IF NOT EXISTS idx_viewing_events_timestamp_country ON viewing_events (event_timestamp DESC, country_code);

-- Use a composite B-Tree index for `event_timestamp` and `device_type`.
-- Although `device_type` has low cardinality (few unique values), a composite B-Tree
-- is still beneficial for queries that filter by the highly selective `event_timestamp` first,
-- then narrow the results based on the `device_type`. This is perfect for device-specific analytics.
CREATE INDEX IF NOT EXISTS idx_viewing_events_timestamp_device ON viewing_events (event_timestamp DESC, device_type);

-- No HASH indexes are used in this strategy.
-- HASH indexes are fast for equality lookups (e.g., WHERE user_id = 123), but they don't support
-- range queries (e.g., WHERE user_id > 100) or sorting, and are not crash-safe in some PostgreSQL versions.
-- Given that our primary analytics queries involve ranges and sorting on `event_timestamp`,
-- B-Tree indexes are a much more versatile and reliable choice.

-- This command updates table statistics, which is crucial for the query planner to make
-- informed decisions on which index to use. It should always be run after creating indexes
-- or loading a significant amount of data.
ANALYZE viewing_events;