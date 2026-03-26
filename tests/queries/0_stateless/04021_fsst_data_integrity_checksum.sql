-- Tags: no-random-settings
-- Test: FSST data integrity via checksums across insert, merge, DETACH/ATTACH, copy.

SET allow_experimental_parallel_reading_from_replicas = 0;

DROP TABLE IF EXISTS test_fsst_chk;
DROP TABLE IF EXISTS test_fsst_chk_copy;

CREATE TABLE test_fsst_chk (id UInt64, s1 String, s2 String) ENGINE = MergeTree ORDER BY id
SETTINGS min_avg_string_length_for_fsst_serialization = 8.0, min_total_bytes_for_fsst_serialization = 16384, max_fsst_compression_ratio = 1.0;

INSERT INTO test_fsst_chk SELECT number, concat('string_one_prefix_', toString(number), '_suffix_padpadpadpadpad_repeated'),
    concat('string_two_key=', toString(number * 3 + 7), '&value=', toString(number % 100), '&extra=long_padding_data_here') FROM numbers(5000);

SELECT 'fsst_check', column, serialization_kind FROM system.parts_columns WHERE database = currentDatabase() AND table = 'test_fsst_chk' AND column IN ('s1', 's2') AND active GROUP BY column, serialization_kind ORDER BY column;

-- Baseline checksum.
SELECT 'chk_insert', sum(cityHash64(id, s1, s2)) FROM test_fsst_chk;

-- After merge.
OPTIMIZE TABLE test_fsst_chk FINAL;
SELECT 'chk_merge', sum(cityHash64(id, s1, s2)) FROM test_fsst_chk;

-- After DETACH/ATTACH.
DETACH TABLE test_fsst_chk;
ATTACH TABLE test_fsst_chk;
SELECT 'chk_reattach', sum(cityHash64(id, s1, s2)) FROM test_fsst_chk;

-- Copy to another table.
CREATE TABLE test_fsst_chk_copy (id UInt64, s1 String, s2 String) ENGINE = MergeTree ORDER BY id
SETTINGS min_avg_string_length_for_fsst_serialization = 8.0, min_total_bytes_for_fsst_serialization = 16384, max_fsst_compression_ratio = 1.0;
INSERT INTO test_fsst_chk_copy SELECT * FROM test_fsst_chk;

SELECT 'chk_copy_match', (SELECT sum(cityHash64(id, s1, s2)) FROM test_fsst_chk) = (SELECT sum(cityHash64(id, s1, s2)) FROM test_fsst_chk_copy);

-- Row-by-row comparison (sample).
SELECT 'row_mismatches', count() FROM test_fsst_chk a JOIN test_fsst_chk_copy b ON a.id = b.id WHERE a.s1 != b.s1 OR a.s2 != b.s2;

DROP TABLE test_fsst_chk_copy;
DROP TABLE test_fsst_chk;
