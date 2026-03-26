-- Test: FSST with concurrent inserts and background merges.

SET allow_experimental_parallel_reading_from_replicas = 0;

DROP TABLE IF EXISTS test_fsst_conc;
CREATE TABLE test_fsst_conc (id UInt64, batch String, data String) ENGINE = MergeTree ORDER BY id
SETTINGS min_avg_string_length_for_fsst_serialization = 8.0, min_total_bytes_for_fsst_serialization = 16384, max_fsst_compression_ratio = 1.0;

-- 10 separate inserts creating 10 parts.
INSERT INTO test_fsst_conc SELECT number,       'batch_01_identifier', concat('Data from batch 01, row ', toString(number),       ' padding=', repeat('x', 30)) FROM numbers(500);
INSERT INTO test_fsst_conc SELECT number + 500,  'batch_02_identifier', concat('Data from batch 02, row ', toString(number + 500),  ' padding=', repeat('y', 30)) FROM numbers(500);
INSERT INTO test_fsst_conc SELECT number + 1000, 'batch_03_identifier', concat('Data from batch 03, row ', toString(number + 1000), ' padding=', repeat('z', 30)) FROM numbers(500);
INSERT INTO test_fsst_conc SELECT number + 1500, 'batch_04_identifier', concat('Data from batch 04, row ', toString(number + 1500), ' padding=', repeat('a', 30)) FROM numbers(500);
INSERT INTO test_fsst_conc SELECT number + 2000, 'batch_05_identifier', concat('Data from batch 05, row ', toString(number + 2000), ' padding=', repeat('b', 30)) FROM numbers(500);
INSERT INTO test_fsst_conc SELECT number + 2500, 'batch_06_identifier', concat('Data from batch 06, row ', toString(number + 2500), ' padding=', repeat('c', 30)) FROM numbers(500);
INSERT INTO test_fsst_conc SELECT number + 3000, 'batch_07_identifier', concat('Data from batch 07, row ', toString(number + 3000), ' padding=', repeat('d', 30)) FROM numbers(500);
INSERT INTO test_fsst_conc SELECT number + 3500, 'batch_08_identifier', concat('Data from batch 08, row ', toString(number + 3500), ' padding=', repeat('e', 30)) FROM numbers(500);
INSERT INTO test_fsst_conc SELECT number + 4000, 'batch_09_identifier', concat('Data from batch 09, row ', toString(number + 4000), ' padding=', repeat('f', 30)) FROM numbers(500);
INSERT INTO test_fsst_conc SELECT number + 4500, 'batch_10_identifier', concat('Data from batch 10, row ', toString(number + 4500), ' padding=', repeat('g', 30)) FROM numbers(500);

-- Verify FSST across parts.
SELECT 'fsst_check', column, serialization_kind, count() AS parts FROM system.parts_columns WHERE database = currentDatabase() AND table = 'test_fsst_conc' AND column IN ('batch', 'data') AND active GROUP BY column, serialization_kind ORDER BY column;

-- Pre-merge checks.
SELECT 'pre_count', count() FROM test_fsst_conc;
SELECT 'pre_batches', count(DISTINCT batch) FROM test_fsst_conc;
SELECT 'pre_checksum', sum(cityHash64(id, batch, data)) FROM test_fsst_conc;

-- Merge.
OPTIMIZE TABLE test_fsst_conc FINAL;

-- Post-merge checks.
SELECT 'post_count', count() FROM test_fsst_conc;
SELECT 'post_batches', count(DISTINCT batch) FROM test_fsst_conc;
SELECT 'post_checksum', sum(cityHash64(id, batch, data)) FROM test_fsst_conc;

-- Spot-check rows from different batches.
SELECT 'batch1_row', data FROM test_fsst_conc WHERE id = 0;
SELECT 'batch5_row', data FROM test_fsst_conc WHERE id = 2000;
SELECT 'batch10_row', data FROM test_fsst_conc WHERE id = 4999;

SELECT 'final_parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 'test_fsst_conc' AND active;

DROP TABLE test_fsst_conc;
