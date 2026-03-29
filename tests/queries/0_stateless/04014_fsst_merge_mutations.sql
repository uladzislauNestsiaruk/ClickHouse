-- Tags: no-random-settings
-- Test: FSST behavior during merges and mutations.

SET mutations_sync = 1;

DROP TABLE IF EXISTS test_fsst_mm;
CREATE TABLE test_fsst_mm (id UInt64, msg String, tag String) ENGINE = MergeTree ORDER BY id
SETTINGS allow_fsst_serialization = 1, min_avg_string_length_for_fsst_serialization = 8.0, min_total_bytes_for_fsst_serialization = 16384, max_fsst_compression_ratio = 1.0;

SYSTEM STOP MERGES test_fsst_mm;

INSERT INTO test_fsst_mm SELECT number, concat('Message part1 row=', toString(number), ' data=aaaaaaaaaa'), 'alpha_tag_value' FROM numbers(1000);
INSERT INTO test_fsst_mm SELECT number + 1000, concat('Message part2 row=', toString(number + 1000), ' data=bbbbbbbbbb'), 'beta_tag_value_' FROM numbers(1000);
INSERT INTO test_fsst_mm SELECT number + 2000, concat('Message part3 row=', toString(number + 2000), ' data=cccccccccc'), 'gamma_tag_value' FROM numbers(1000);

-- Verify FSST before merge.
SELECT 'fsst_before', column, serialization_kind FROM system.parts_columns WHERE database = currentDatabase() AND table = 'test_fsst_mm' AND column IN ('msg', 'tag') AND active GROUP BY column, serialization_kind ORDER BY column;
SELECT 'parts_before', count() FROM system.parts WHERE database = currentDatabase() AND table = 'test_fsst_mm' AND active;
SELECT 'count_before', count() FROM test_fsst_mm;

-- Merge.
SYSTEM START MERGES test_fsst_mm;
OPTIMIZE TABLE test_fsst_mm FINAL;

SELECT 'fsst_after_merge', column, serialization_kind FROM system.parts_columns WHERE database = currentDatabase() AND table = 'test_fsst_mm' AND column IN ('msg', 'tag') AND active ORDER BY column;
SELECT 'parts_after', count() FROM system.parts WHERE database = currentDatabase() AND table = 'test_fsst_mm' AND active;
SELECT 'count_after', count() FROM test_fsst_mm;
SELECT 'row_0', msg FROM test_fsst_mm WHERE id = 0;
SELECT 'row_2999', msg FROM test_fsst_mm WHERE id = 2999;
SELECT 'row_1000', msg FROM test_fsst_mm WHERE id = 1000;

-- Sort order preserved.
SELECT 'sorted_check', count() FROM (SELECT id, leadInFrame(id) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS next_id FROM test_fsst_mm) WHERE next_id != 0 AND next_id <= id;

-- ALTER UPDATE.
ALTER TABLE test_fsst_mm UPDATE msg = concat(msg, ' [UPDATED]') WHERE id % 1000 = 0;
SELECT 'after_update', msg FROM test_fsst_mm WHERE id = 0;
SELECT 'update_count', countIf(msg LIKE '%[UPDATED]%') FROM test_fsst_mm;

-- ALTER DELETE.
ALTER TABLE test_fsst_mm DELETE WHERE id >= 2500;
SELECT 'after_delete', count() FROM test_fsst_mm;

-- ALTER ADD COLUMN + OPTIMIZE.
ALTER TABLE test_fsst_mm ADD COLUMN extra String DEFAULT 'default_extra_value_padding';
OPTIMIZE TABLE test_fsst_mm FINAL;
SELECT 'after_add_col', count(), countIf(extra = 'default_extra_value_padding') FROM test_fsst_mm;

-- ALTER DROP COLUMN.
ALTER TABLE test_fsst_mm DROP COLUMN tag;
SELECT 'after_drop_col', count() FROM test_fsst_mm;
SELECT 'remaining_data', msg FROM test_fsst_mm WHERE id = 500;

DROP TABLE test_fsst_mm;
