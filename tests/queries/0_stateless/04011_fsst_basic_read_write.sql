-- Test: FSST basic read/write correctness.
-- Verifies data written with FSST is read back correctly, survives merge and DETACH/ATTACH.

DROP TABLE IF EXISTS test_fsst_rw;
CREATE TABLE test_fsst_rw (id UInt64, msg String) ENGINE = MergeTree ORDER BY id
SETTINGS min_avg_string_length_for_fsst_serialization = 8.0, min_total_bytes_for_fsst_serialization = 16384, max_fsst_compression_ratio = 1.0;

SYSTEM STOP MERGES test_fsst_rw;

INSERT INTO test_fsst_rw SELECT number, concat('Log entry #', toString(number), ': Connection from 192.168.1.', toString(number % 256), ' accepted') FROM numbers(1000);
INSERT INTO test_fsst_rw SELECT number + 1000, concat('Log entry #', toString(number + 1000), ': Connection from 10.0.0.', toString(number % 256), ' accepted') FROM numbers(1000);

-- Verify FSST is used in both parts.
SELECT 'fsst_check', serialization_kind, count() AS parts FROM system.parts_columns WHERE database = currentDatabase() AND table = 'test_fsst_rw' AND column = 'msg' AND active GROUP BY serialization_kind;

-- Verify data before merge.
SELECT 'count_before', count() FROM test_fsst_rw;
SELECT 'row_0', msg FROM test_fsst_rw WHERE id = 0;
SELECT 'row_999', msg FROM test_fsst_rw WHERE id = 999;
SELECT 'row_1000', msg FROM test_fsst_rw WHERE id = 1000;
SELECT 'row_1999', msg FROM test_fsst_rw WHERE id = 1999;
SELECT 'distinct_ids', count(DISTINCT id) FROM test_fsst_rw;

-- Merge and verify.
SYSTEM START MERGES test_fsst_rw;
OPTIMIZE TABLE test_fsst_rw FINAL;

SELECT 'parts_after', count() FROM system.parts WHERE database = currentDatabase() AND table = 'test_fsst_rw' AND active;
SELECT 'count_after', count() FROM test_fsst_rw;
SELECT 'merged_row_0', msg FROM test_fsst_rw WHERE id = 0;
SELECT 'merged_row_1999', msg FROM test_fsst_rw WHERE id = 1999;

-- DETACH/ATTACH cycle.
DETACH TABLE test_fsst_rw;
ATTACH TABLE test_fsst_rw;

SELECT 'reattach_count', count() FROM test_fsst_rw;
SELECT 'reattach_row_500', msg FROM test_fsst_rw WHERE id = 500;
SELECT 'reattach_row_1500', msg FROM test_fsst_rw WHERE id = 1500;

DROP TABLE test_fsst_rw;
