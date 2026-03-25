-- Test: FSST in both compact and wide part formats + CHECK TABLE.

-- Part 1: Compact parts.
DROP TABLE IF EXISTS test_fsst_compact;
CREATE TABLE test_fsst_compact (id UInt64, msg String) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 10000000, min_avg_string_length_for_fsst_serialization = 8.0,
    min_total_bytes_for_fsst_serialization = 16384, max_fsst_compression_ratio = 1.0;

INSERT INTO test_fsst_compact SELECT number, concat('Compact part message #', toString(number), ' with repeated padding: ', repeat('compact_data ', 5)) FROM numbers(2000);

SELECT 'compact_type', part_type FROM system.parts WHERE database = currentDatabase() AND table = 'test_fsst_compact' AND active;
SELECT 'compact_fsst', serialization_kind FROM system.parts_columns WHERE database = currentDatabase() AND table = 'test_fsst_compact' AND column = 'msg' AND active;
SELECT 'compact_count', count() FROM test_fsst_compact;
SELECT 'compact_sample', msg FROM test_fsst_compact WHERE id = 42;
CHECK TABLE test_fsst_compact;

DROP TABLE test_fsst_compact;

-- Part 2: Wide parts.
DROP TABLE IF EXISTS test_fsst_wide;
CREATE TABLE test_fsst_wide (id UInt64, msg String) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 1, min_avg_string_length_for_fsst_serialization = 8.0,
    min_total_bytes_for_fsst_serialization = 16384, max_fsst_compression_ratio = 1.0;

INSERT INTO test_fsst_wide SELECT number, concat('Wide part message #', toString(number), ' with repeated padding: ', repeat('wide_format_data ', 5)) FROM numbers(2000);

SELECT 'wide_type', part_type FROM system.parts WHERE database = currentDatabase() AND table = 'test_fsst_wide' AND active;
SELECT 'wide_fsst', serialization_kind FROM system.parts_columns WHERE database = currentDatabase() AND table = 'test_fsst_wide' AND column = 'msg' AND active;
SELECT 'wide_count', count() FROM test_fsst_wide;
SELECT 'wide_sample', msg FROM test_fsst_wide WHERE id = 42;
CHECK TABLE test_fsst_wide;

-- Merge wide parts.
SYSTEM STOP MERGES test_fsst_wide;
INSERT INTO test_fsst_wide SELECT number + 2000, concat('Wide part 2 message #', toString(number + 2000), ' with different padding: ', repeat('second_part_data ', 5)) FROM numbers(2000);
SYSTEM START MERGES test_fsst_wide;
OPTIMIZE TABLE test_fsst_wide FINAL;

SELECT 'wide_merged_count', count() FROM test_fsst_wide;
SELECT 'wide_merged_sample', msg FROM test_fsst_wide WHERE id = 3000;
CHECK TABLE test_fsst_wide;

DROP TABLE test_fsst_wide;
