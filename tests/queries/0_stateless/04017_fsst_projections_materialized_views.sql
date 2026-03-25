-- Test: FSST with projections and materialized views.

-- Part 1: Projections.
DROP TABLE IF EXISTS test_fsst_proj;
CREATE TABLE test_fsst_proj (id UInt64, category String, message String) ENGINE = MergeTree ORDER BY id
SETTINGS min_avg_string_length_for_fsst_serialization = 8.0, min_total_bytes_for_fsst_serialization = 16384, max_fsst_compression_ratio = 1.0;

ALTER TABLE test_fsst_proj ADD PROJECTION proj_by_category (SELECT id, category, message ORDER BY category);

INSERT INTO test_fsst_proj SELECT number, concat('cat_', toString(number % 10), '_padding_text'),
    concat('Event log: operation=', if(number % 3 = 0, 'READ', if(number % 3 = 1, 'WRITE', 'DELETE')), ' resource=/api/v1/data/', toString(number), ' user=user_', toString(number % 50), ' timestamp=2025-03-', lpad(toString((number % 28) + 1), 2, '0'))
FROM numbers(5000);

SELECT 'proj_fsst', column, serialization_kind FROM system.parts_columns WHERE database = currentDatabase() AND table = 'test_fsst_proj' AND column IN ('category', 'message') AND active GROUP BY column, serialization_kind ORDER BY column;
SELECT 'proj_count', count() FROM test_fsst_proj;
SELECT 'proj_sample', message FROM test_fsst_proj WHERE id = 100;
SELECT 'proj_query', category, count() FROM test_fsst_proj WHERE category = 'cat_5_padding_text' GROUP BY category;

DROP TABLE test_fsst_proj;

-- Part 2: Materialized views.
DROP TABLE IF EXISTS test_fsst_mv;
DROP TABLE IF EXISTS test_fsst_mv_source;
DROP TABLE IF EXISTS test_fsst_mv_target;

CREATE TABLE test_fsst_mv_source (category String, id UInt64, message String) ENGINE = MergeTree ORDER BY id
SETTINGS min_avg_string_length_for_fsst_serialization = 8.0, min_total_bytes_for_fsst_serialization = 16384, max_fsst_compression_ratio = 1.0;

CREATE TABLE test_fsst_mv_target (category String, msg_count UInt64, sample_msg String) ENGINE = MergeTree ORDER BY category
SETTINGS min_avg_string_length_for_fsst_serialization = 8.0, min_total_bytes_for_fsst_serialization = 16384, max_fsst_compression_ratio = 1.0;

CREATE MATERIALIZED VIEW test_fsst_mv TO test_fsst_mv_target AS
SELECT category, count() AS msg_count, any(message) AS sample_msg FROM test_fsst_mv_source GROUP BY category;

INSERT INTO test_fsst_mv_source SELECT concat('service_', toString(number % 8), '_longname'), number,
    concat('Alert: CPU usage at ', toString(50 + number % 50), '% on host-', toString(number % 20), '.datacenter-', toString(number % 3), '.internal.megacorp.com at ', toString(toDateTime('2025-01-01 00:00:00') + number))
FROM numbers(5000);

SELECT 'mv_source_fsst', column, serialization_kind FROM system.parts_columns WHERE database = currentDatabase() AND table = 'test_fsst_mv_source' AND column IN ('category', 'message') AND active GROUP BY column, serialization_kind ORDER BY column;
SELECT 'mv_target_count', count() FROM test_fsst_mv_target;
SELECT 'mv_target_sample', category, substring(sample_msg, 1, 50) FROM test_fsst_mv_target WHERE category = 'service_3_longname' LIMIT 1;

DROP TABLE test_fsst_mv;
DROP TABLE test_fsst_mv_source;
DROP TABLE test_fsst_mv_target;
