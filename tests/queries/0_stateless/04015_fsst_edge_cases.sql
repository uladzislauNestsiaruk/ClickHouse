-- Test: FSST edge cases — empty strings, null bytes, long strings, unicode, nullable, identical.

-- Case 1: Empty/non-empty string mix.
DROP TABLE IF EXISTS test_fsst_edge;
CREATE TABLE test_fsst_edge (id UInt64, s String) ENGINE = MergeTree ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.95, min_avg_string_length_for_fsst_serialization = 1.0, min_total_bytes_for_fsst_serialization = 1024, max_fsst_compression_ratio = 1.0;
INSERT INTO test_fsst_edge SELECT number, if(number % 3 = 0, '', concat('Non-empty string with some repeated content, row number is ', toString(number))) FROM numbers(3000);
SELECT 'fsst_empty_mix', serialization_kind FROM system.parts_columns WHERE database = currentDatabase() AND table = 'test_fsst_edge' AND column = 's' AND active;
SELECT 'empty_mix_count', count() FROM test_fsst_edge;
SELECT 'empty_count', countIf(s = '') FROM test_fsst_edge;
SELECT 'nonempty_count', countIf(s != '') FROM test_fsst_edge;
SELECT 'empty_row', s FROM test_fsst_edge WHERE id = 0;
SELECT 'nonempty_row', length(s) > 0 FROM test_fsst_edge WHERE id = 1;
DROP TABLE test_fsst_edge;

-- Case 2: Strings with null bytes.
CREATE TABLE test_fsst_edge (id UInt64, s String) ENGINE = MergeTree ORDER BY id
SETTINGS min_avg_string_length_for_fsst_serialization = 1.0, min_total_bytes_for_fsst_serialization = 1024, max_fsst_compression_ratio = 1.0;
INSERT INTO test_fsst_edge SELECT number, concat('prefix', char(0), 'middle_padding_repeated_text', char(0), 'suffix_', toString(number)) FROM numbers(2000);
SELECT 'fsst_null_bytes', serialization_kind FROM system.parts_columns WHERE database = currentDatabase() AND table = 'test_fsst_edge' AND column = 's' AND active;
SELECT 'null_bytes_present', countIf(position(s, char(0)) > 0) FROM test_fsst_edge;
DROP TABLE test_fsst_edge;

-- Case 3: Very long strings (~5KB each).
CREATE TABLE test_fsst_edge (id UInt64, s String) ENGINE = MergeTree ORDER BY id
SETTINGS min_avg_string_length_for_fsst_serialization = 8.0, min_total_bytes_for_fsst_serialization = 16384, max_fsst_compression_ratio = 1.0;
INSERT INTO test_fsst_edge SELECT number, repeat(concat('LogLine[', toString(number), '] '), 200) FROM numbers(100);
SELECT 'fsst_long', serialization_kind FROM system.parts_columns WHERE database = currentDatabase() AND table = 'test_fsst_edge' AND column = 's' AND active;
SELECT 'long_len', length(s) > 4000 FROM test_fsst_edge WHERE id = 0;
SELECT 'long_prefix', substring(s, 1, 12) FROM test_fsst_edge WHERE id = 42;
SELECT 'long_endswith', endsWith(s, '] ') FROM test_fsst_edge WHERE id = 42;
DROP TABLE test_fsst_edge;

-- Case 4: Unicode / multibyte strings.
CREATE TABLE test_fsst_edge (id UInt64, s String) ENGINE = MergeTree ORDER BY id
SETTINGS min_avg_string_length_for_fsst_serialization = 1.0, min_total_bytes_for_fsst_serialization = 1024, max_fsst_compression_ratio = 1.0;
INSERT INTO test_fsst_edge SELECT number, concat('Сообщение номер ', toString(number), ' — данные: ', repeat('абвгд ', 10)) FROM numbers(2000);
SELECT 'fsst_unicode', serialization_kind FROM system.parts_columns WHERE database = currentDatabase() AND table = 'test_fsst_edge' AND column = 's' AND active;
SELECT 'unicode_byte_gt_char', length(s) > lengthUTF8(s) FROM test_fsst_edge WHERE id = 0;
SELECT 'unicode_starts', startsWith(s, 'Сообщение номер 0') FROM test_fsst_edge WHERE id = 0;
DROP TABLE test_fsst_edge;

-- Case 5: Nullable(String) with FSST.
CREATE TABLE test_fsst_edge (id UInt64, s Nullable(String)) ENGINE = MergeTree ORDER BY id
SETTINGS min_avg_string_length_for_fsst_serialization = 1.0, min_total_bytes_for_fsst_serialization = 1024, max_fsst_compression_ratio = 1.0;
INSERT INTO test_fsst_edge SELECT number, if(number % 5 = 0, NULL, concat('Nullable string with padding content row=', toString(number), ' extra=xxxxxxxxxx')) FROM numbers(2000);
SELECT 'nullable_count', count() FROM test_fsst_edge;
SELECT 'nullable_nulls', countIf(s IS NULL) FROM test_fsst_edge;
SELECT 'nullable_not_nulls', countIf(s IS NOT NULL) FROM test_fsst_edge;
SELECT 'nullable_coalesce', count() FROM test_fsst_edge WHERE coalesce(s, 'fallback') != 'fallback';
DROP TABLE test_fsst_edge;

-- Case 6: All identical strings.
CREATE TABLE test_fsst_edge (id UInt64, s String) ENGINE = MergeTree ORDER BY id
SETTINGS min_avg_string_length_for_fsst_serialization = 8.0, min_total_bytes_for_fsst_serialization = 16384, max_fsst_compression_ratio = 1.0;
INSERT INTO test_fsst_edge SELECT number, 'The quick brown fox jumps over the lazy dog.' FROM numbers(2000);
SELECT 'fsst_identical', serialization_kind FROM system.parts_columns WHERE database = currentDatabase() AND table = 'test_fsst_edge' AND column = 's' AND active;
SELECT 'identical_uniq', uniq(s) FROM test_fsst_edge;
SELECT 'identical_value', s FROM test_fsst_edge WHERE id = 999;
DROP TABLE test_fsst_edge;
