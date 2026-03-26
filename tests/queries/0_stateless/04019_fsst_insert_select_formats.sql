-- Tags: no-random-settings
-- Test: FSST with INSERT VALUES, INSERT SELECT, CTEs, UNION ALL, table copies.

SET optimize_functions_to_subcolumns = 0;

DROP TABLE IF EXISTS test_fsst_fmt;
CREATE TABLE test_fsst_fmt (id UInt64, data String) ENGINE = MergeTree ORDER BY id
SETTINGS min_avg_string_length_for_fsst_serialization = 8.0, min_total_bytes_for_fsst_serialization = 1024, max_fsst_compression_ratio = 1.0;

-- INSERT VALUES.
INSERT INTO test_fsst_fmt VALUES (1, 'First value with enough padding to be meaningful for FSST compression testing purposes'), (2, 'Second value with enough padding to be meaningful for FSST compression testing purposes'), (3, 'Third value with enough padding to be meaningful for FSST compression testing purposes');

-- INSERT SELECT.
INSERT INTO test_fsst_fmt SELECT number + 100, concat('Generated row ', toString(number), ' with padding content for FSST: ', repeat('test_data ', 5)) FROM numbers(2000);

SELECT 'fsst_check', serialization_kind FROM system.parts_columns WHERE database = currentDatabase() AND table = 'test_fsst_fmt' AND column = 'data' AND active GROUP BY serialization_kind;
SELECT 'total_count', count() FROM test_fsst_fmt;

-- Exact literal roundtrip.
SELECT 'literal_1', data FROM test_fsst_fmt WHERE id = 1;
SELECT 'literal_2', data FROM test_fsst_fmt WHERE id = 2;

-- CTE.
SELECT 'cte_result', cnt FROM (WITH fsst_data AS (SELECT data, length(data) AS len FROM test_fsst_fmt WHERE id >= 100) SELECT count() AS cnt FROM fsst_data WHERE len > 50);

-- UNION ALL.
SELECT 'union_count', count() FROM (SELECT data FROM test_fsst_fmt WHERE id <= 3 UNION ALL SELECT data FROM test_fsst_fmt WHERE id >= 2100);

-- Copy to another FSST table.
DROP TABLE IF EXISTS test_fsst_copy;
CREATE TABLE test_fsst_copy (id UInt64, data String) ENGINE = MergeTree ORDER BY id
SETTINGS min_avg_string_length_for_fsst_serialization = 8.0, min_total_bytes_for_fsst_serialization = 1024, max_fsst_compression_ratio = 1.0;
INSERT INTO test_fsst_copy SELECT * FROM test_fsst_fmt;
SELECT 'copy_count', count() FROM test_fsst_copy;
SELECT 'copy_match', countIf(a.data = b.data) FROM test_fsst_fmt a JOIN test_fsst_copy b ON a.id = b.id;

-- Copy to non-FSST table.
DROP TABLE IF EXISTS test_fsst_to_regular;
CREATE TABLE test_fsst_to_regular (id UInt64, data String) ENGINE = MergeTree ORDER BY id
SETTINGS min_avg_string_length_for_fsst_serialization = 8.0, min_total_bytes_for_fsst_serialization = 999999999;
INSERT INTO test_fsst_to_regular SELECT * FROM test_fsst_fmt;
SELECT 'to_regular_count', count() FROM test_fsst_to_regular;
SELECT 'to_regular_kind', serialization_kind FROM system.parts_columns WHERE database = currentDatabase() AND table = 'test_fsst_to_regular' AND column = 'data' AND active;

DROP TABLE test_fsst_to_regular;
DROP TABLE test_fsst_copy;
DROP TABLE test_fsst_fmt;
