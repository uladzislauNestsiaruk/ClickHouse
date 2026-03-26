-- Tags: no-random-settings
-- Test: String functions on FSST-compressed columns.
-- Verifies decompression works transparently for all common string operations.

SET optimize_functions_to_subcolumns = 0;

DROP TABLE IF EXISTS test_fsst_func;
CREATE TABLE test_fsst_func (id UInt64, s String) ENGINE = MergeTree ORDER BY id
SETTINGS min_avg_string_length_for_fsst_serialization = 8.0, min_total_bytes_for_fsst_serialization = 16384, max_fsst_compression_ratio = 1.0;

INSERT INTO test_fsst_func SELECT number, concat('Event type=INFO host=server-', toString(number % 10), '.example.com timestamp=2025-01-', lpad(toString((number % 28) + 1), 2, '0'), ' message="Request processed in ', toString(number * 3 % 1000), 'ms"') FROM numbers(2000);

-- Verify FSST is used.
SELECT 'fsst_check', serialization_kind FROM system.parts_columns WHERE database = currentDatabase() AND table = 'test_fsst_func' AND column = 's' AND active;

-- Length and emptiness.
SELECT 'length', min(length(s)) > 0, max(length(s)) > 50 FROM test_fsst_func;
SELECT 'empty', countIf(empty(s)) FROM test_fsst_func;
SELECT 'notEmpty', countIf(notEmpty(s)) = 2000 FROM test_fsst_func;

-- Case conversion.
SELECT 'upper', upper(s) LIKE 'EVENT TYPE=INFO%' FROM test_fsst_func WHERE id = 0;

-- Substring and position.
SELECT 'substring', substring(s, 1, 10) FROM test_fsst_func WHERE id = 0;
SELECT 'position', position(s, 'host=') > 0 FROM test_fsst_func WHERE id = 0;
SELECT 'startsWith', startsWith(s, 'Event type=INFO') FROM test_fsst_func WHERE id = 0;
SELECT 'endsWith', endsWith(s, 'ms"') FROM test_fsst_func WHERE id = 0;

-- Pattern matching.
SELECT 'like', count() FROM test_fsst_func WHERE s LIKE '%server-5%';
SELECT 'match', count() FROM test_fsst_func WHERE match(s, 'server-[0-9]+\\.example');

-- String manipulation.
SELECT 'reverse', reverse(reverse(s)) = s FROM test_fsst_func WHERE id = 0;
SELECT 'replaceOne', replaceOne(s, 'INFO', 'DEBUG') LIKE '%DEBUG%' FROM test_fsst_func WHERE id = 0;
SELECT 'concat', length(concat(s, '_suffix')) = length(s) + 7 FROM test_fsst_func WHERE id = 0;

-- Hash functions.
SELECT 'cityHash64', cityHash64(s) != 0 FROM test_fsst_func WHERE id = 0;
SELECT 'sipHash64', sipHash64(s) != 0 FROM test_fsst_func WHERE id = 0;

-- Aggregation.
SELECT 'min_max', min(s) < max(s) FROM test_fsst_func;
SELECT 'uniq', uniq(s) = 2000 FROM test_fsst_func;
SELECT 'groupArray', length(groupArray(s)) = 2000 FROM test_fsst_func;

DROP TABLE test_fsst_func;
