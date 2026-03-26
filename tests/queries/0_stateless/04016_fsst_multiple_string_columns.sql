-- Tags: no-random-settings
-- Test: FSST with multiple string columns — mixed eligibility, cross-column queries, merge.

DROP TABLE IF EXISTS test_fsst_multi;
CREATE TABLE test_fsst_multi (id UInt64, short_code String, log_message String, url String, metric Float64, event_date Date)
ENGINE = MergeTree ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9, min_avg_string_length_for_fsst_serialization = 8.0,
    min_total_bytes_for_fsst_serialization = 16384, max_fsst_compression_ratio = 0.85;

INSERT INTO test_fsst_multi SELECT number, substring('ABCDEFGHIJKLMNOPQRSTUVWXYZ', (number % 26) + 1, 2),
    concat('[', toString(toDate('2025-01-01') + number % 365), '] INFO server-', toString(number % 5), ': Processed request #', toString(number), ' from client 10.0.', toString(number % 256), '.', toString(number % 256), ' duration=', toString(number % 5000), 'ms status=200 OK'),
    concat('https://api.example.com/v2/users/', toString(number % 1000), '/profile?lang=en&region=us&session=', repeat('x', 20)),
    number * 1.5, toDate('2025-01-01') + number % 365
FROM numbers(5000);

-- short_code: DEFAULT (too short), log_message and url: FSST.
SELECT 'ser_check', column, serialization_kind FROM system.parts_columns WHERE database = currentDatabase() AND table = 'test_fsst_multi' AND column IN ('short_code', 'log_message', 'url') AND active ORDER BY column;

SELECT 'total_rows', count() FROM test_fsst_multi;

-- Cross-column filter.
SELECT 'cross_filter', count() FROM test_fsst_multi WHERE short_code = 'AB' AND log_message LIKE '%server-3%' AND metric > 100;

-- Merge with multiple FSST columns.
SYSTEM STOP MERGES test_fsst_multi;
INSERT INTO test_fsst_multi SELECT number + 5000, substring('ABCDEFGHIJKLMNOPQRSTUVWXYZ', (number % 26) + 1, 2),
    concat('[2025-06-15] WARN server-', toString(number % 5), ': Slow query detected, duration=', toString(number * 7 % 10000), 'ms'),
    concat('https://cdn.example.com/assets/', toString(number), '.js?v=', repeat('y', 20)), number * 2.5, toDate('2025-06-15')
FROM numbers(3000);
SYSTEM START MERGES test_fsst_multi;
OPTIMIZE TABLE test_fsst_multi FINAL;

SELECT 'after_merge', count() FROM test_fsst_multi;
SELECT 'merged_sample', id, short_code, substring(log_message, 1, 30) FROM test_fsst_multi WHERE id = 6000;

DROP TABLE test_fsst_multi;
