-- Tags: no-random-settings
-- Test: FSST serialization selection heuristics.
-- Verifies serialization_kind based on avg length, total bytes, sparse ratio, compression ratio.

DROP TABLE IF EXISTS test_fsst_heuristics;

-- Case 1: Long repetitive strings => FSST selected.
CREATE TABLE test_fsst_heuristics (id UInt64, s String) ENGINE = MergeTree ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9, min_avg_string_length_for_fsst_serialization = 8.0,
    min_total_bytes_for_fsst_serialization = 16384, max_fsst_compression_ratio = 0.85;
INSERT INTO test_fsst_heuristics SELECT number, concat('https://example.com/api/v1/resource/', toString(number % 100), '/action?param=value&session=abc123') FROM numbers(2000);
SELECT 'long_repetitive', column, serialization_kind FROM system.parts_columns WHERE database = currentDatabase() AND table = 'test_fsst_heuristics' AND column = 's' AND active;
DROP TABLE test_fsst_heuristics;

-- Case 2: Very short strings => FSST NOT selected.
CREATE TABLE test_fsst_heuristics (id UInt64, s String) ENGINE = MergeTree ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9, min_avg_string_length_for_fsst_serialization = 8.0,
    min_total_bytes_for_fsst_serialization = 16384, max_fsst_compression_ratio = 0.85;
INSERT INTO test_fsst_heuristics SELECT number, substring('abcdefghijklmnbvcxz', number % 6, (number % 6) + 2) FROM numbers(10000);
SELECT 'short_strings', column, serialization_kind FROM system.parts_columns WHERE database = currentDatabase() AND table = 'test_fsst_heuristics' AND column = 's' AND active;
DROP TABLE test_fsst_heuristics;

-- Case 3: Mostly empty strings => SPARSE wins.
CREATE TABLE test_fsst_heuristics (id UInt64, s String) ENGINE = MergeTree ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9, min_avg_string_length_for_fsst_serialization = 8.0,
    min_total_bytes_for_fsst_serialization = 16384, max_fsst_compression_ratio = 0.85;
INSERT INTO test_fsst_heuristics SELECT number, if(number % 20 = 0, concat('Long log message with id=', toString(number), ' and extra padding text here'), '') FROM numbers(10000);
SELECT 'mostly_empty', column, serialization_kind FROM system.parts_columns WHERE database = currentDatabase() AND table = 'test_fsst_heuristics' AND column = 's' AND active;
DROP TABLE test_fsst_heuristics;

-- Case 4: Too little data (below 16KB) => FSST NOT selected.
CREATE TABLE test_fsst_heuristics (id UInt64, s String) ENGINE = MergeTree ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9, min_avg_string_length_for_fsst_serialization = 8.0,
    min_total_bytes_for_fsst_serialization = 16384, max_fsst_compression_ratio = 0.85;
INSERT INTO test_fsst_heuristics SELECT number, concat('short_prefix_', toString(number)) FROM numbers(50);
SELECT 'too_little_data', column, serialization_kind FROM system.parts_columns WHERE database = currentDatabase() AND table = 'test_fsst_heuristics' AND column = 's' AND active;
DROP TABLE test_fsst_heuristics;

-- Case 5: Random UUIDs => FSST NOT selected (bad compression ratio).
CREATE TABLE test_fsst_heuristics (id UInt64, s String) ENGINE = MergeTree ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9, min_avg_string_length_for_fsst_serialization = 8.0,
    min_total_bytes_for_fsst_serialization = 16384, max_fsst_compression_ratio = 0.5;
INSERT INTO test_fsst_heuristics SELECT number, toString(generateUUIDv4()) FROM numbers(2000);
SELECT 'random_uuids', column, serialization_kind FROM system.parts_columns WHERE database = currentDatabase() AND table = 'test_fsst_heuristics' AND column = 's' AND active;
DROP TABLE test_fsst_heuristics;

-- Case 6: Random UUIDs with max_fsst_compression_ratio = 1.0 => FSST forced.
CREATE TABLE test_fsst_heuristics (id UInt64, s String) ENGINE = MergeTree ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9, min_avg_string_length_for_fsst_serialization = 8.0,
    min_total_bytes_for_fsst_serialization = 16384, max_fsst_compression_ratio = 1.0;
INSERT INTO test_fsst_heuristics SELECT number, toString(generateUUIDv4()) FROM numbers(2000);
SELECT 'forced_fsst', column, serialization_kind FROM system.parts_columns WHERE database = currentDatabase() AND table = 'test_fsst_heuristics' AND column = 's' AND active;
DROP TABLE test_fsst_heuristics;
