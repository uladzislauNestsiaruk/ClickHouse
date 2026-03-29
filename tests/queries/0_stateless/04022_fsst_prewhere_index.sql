-- Tags: no-random-settings
-- Test: FSST with PREWHERE, primary key strings, and secondary skip indices.

SET optimize_functions_to_subcolumns = 0;

-- Part 1: PREWHERE.
DROP TABLE IF EXISTS test_fsst_pw;
CREATE TABLE test_fsst_pw (id UInt64, category String, payload String) ENGINE = MergeTree ORDER BY id
SETTINGS allow_fsst_serialization = 1, min_avg_string_length_for_fsst_serialization = 8.0, min_total_bytes_for_fsst_serialization = 16384, max_fsst_compression_ratio = 1.0;

INSERT INTO test_fsst_pw SELECT number, concat('category_group_', toString(number % 20)),
    concat('Detailed payload data for item #', toString(number), ': status=active, priority=', toString(number % 5), ', description=', repeat('lorem ipsum dolor sit amet ', 3))
FROM numbers(10000);

SELECT 'pw_fsst', column, serialization_kind FROM system.parts_columns WHERE database = currentDatabase() AND table = 'test_fsst_pw' AND column IN ('category', 'payload') AND active GROUP BY column, serialization_kind ORDER BY column;
SELECT 'prewhere_count', count() FROM test_fsst_pw PREWHERE category = 'category_group_7' WHERE length(payload) > 100;
SELECT 'prewhere_sample', id, category FROM test_fsst_pw PREWHERE category = 'category_group_0' ORDER BY id LIMIT 3;

DROP TABLE test_fsst_pw;

-- Part 2: String in primary key.
DROP TABLE IF EXISTS test_fsst_pk;
CREATE TABLE test_fsst_pk (tenant String, event_type String, ts UInt64, data String) ENGINE = MergeTree ORDER BY (tenant, event_type, ts)
SETTINGS allow_fsst_serialization = 1, min_avg_string_length_for_fsst_serialization = 8.0, min_total_bytes_for_fsst_serialization = 16384, max_fsst_compression_ratio = 1.0;

INSERT INTO test_fsst_pk SELECT concat('tenant_', toString(number % 10), '_longname'), concat('event_', toString(number % 5), '_type_name'), number,
    concat('Event data payload #', toString(number), ' with context: ', repeat('key=value ', 10))
FROM numbers(10000);

SELECT 'pk_fsst', column, serialization_kind FROM system.parts_columns WHERE database = currentDatabase() AND table = 'test_fsst_pk' AND column IN ('tenant', 'event_type', 'data') AND active GROUP BY column, serialization_kind ORDER BY column;
SELECT 'pk_lookup', count() FROM test_fsst_pk WHERE tenant = 'tenant_3_longname' AND event_type = 'event_2_type_name';
SELECT 'pk_range', count() FROM test_fsst_pk WHERE tenant >= 'tenant_3_longname' AND tenant < 'tenant_5_longname';
SELECT 'pk_sample', tenant, event_type, ts FROM test_fsst_pk WHERE tenant = 'tenant_0_longname' AND event_type = 'event_0_type_name' ORDER BY ts LIMIT 3;

DROP TABLE test_fsst_pk;

-- Part 3: Secondary skip indices.
DROP TABLE IF EXISTS test_fsst_idx;
CREATE TABLE test_fsst_idx (id UInt64, tag String, message String,
    INDEX idx_tag_set tag TYPE set(100) GRANULARITY 2,
    INDEX idx_msg_bloom message TYPE bloom_filter(0.01) GRANULARITY 2,
    INDEX idx_tag_minmax tag TYPE minmax GRANULARITY 2)
ENGINE = MergeTree ORDER BY id
SETTINGS allow_fsst_serialization = 1, min_avg_string_length_for_fsst_serialization = 8.0, min_total_bytes_for_fsst_serialization = 16384, max_fsst_compression_ratio = 1.0, index_granularity = 256;

INSERT INTO test_fsst_idx SELECT number, concat('tag_value_', toString(number % 50)),
    concat('System alert: component=', toString(number % 30), ' severity=', if(number % 3 = 0, 'CRITICAL', if(number % 3 = 1, 'WARNING', 'INFO')), ' host=server-', toString(number % 100), '.prod.internal')
FROM numbers(10000);

SELECT 'idx_fsst', column, serialization_kind FROM system.parts_columns WHERE database = currentDatabase() AND table = 'test_fsst_idx' AND column IN ('tag', 'message') AND active GROUP BY column, serialization_kind ORDER BY column;
SELECT 'set_idx', count() FROM test_fsst_idx WHERE tag = 'tag_value_25';
SELECT 'bloom_idx', count() FROM test_fsst_idx WHERE message LIKE '%severity=CRITICAL%';
SELECT 'minmax_idx', count() FROM test_fsst_idx WHERE tag = 'tag_value_0';
CHECK TABLE test_fsst_idx;

DROP TABLE test_fsst_idx;
