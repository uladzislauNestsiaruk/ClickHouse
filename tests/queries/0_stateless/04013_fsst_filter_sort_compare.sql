-- Test: FSST column filtering, sorting, comparison, JOIN, GROUP BY, DISTINCT.

DROP TABLE IF EXISTS test_fsst_fsc;
CREATE TABLE test_fsst_fsc (id UInt64, category String, message String) ENGINE = MergeTree ORDER BY id
SETTINGS min_avg_string_length_for_fsst_serialization = 8.0, min_total_bytes_for_fsst_serialization = 16384, max_fsst_compression_ratio = 1.0;

INSERT INTO test_fsst_fsc SELECT number, concat('category_', toString(number % 5)), concat('Error in module ', toString(number % 20), ': timeout after ', toString(number * 7 % 10000), 'ms waiting for response from backend-', toString(number % 8), '.internal.corp') FROM numbers(5000);

-- Verify FSST is used.
SELECT 'fsst_check', column, serialization_kind FROM system.parts_columns WHERE database = currentDatabase() AND table = 'test_fsst_fsc' AND column IN ('category', 'message') AND active ORDER BY column;

-- Filters.
SELECT 'eq_filter', count() FROM test_fsst_fsc WHERE category = 'category_3';
SELECT 'range_filter', count() FROM test_fsst_fsc WHERE category >= 'category_2' AND category < 'category_4';
SELECT 'like_filter', count() FROM test_fsst_fsc WHERE message LIKE '%module 7:%';
SELECT 'in_filter', count() FROM test_fsst_fsc WHERE category IN ('category_0', 'category_4');

-- ORDER BY.
SELECT 'order_first', category, id FROM test_fsst_fsc ORDER BY category, id LIMIT 3;
SELECT 'order_last', category, id FROM test_fsst_fsc ORDER BY category DESC, id DESC LIMIT 3;

-- DISTINCT.
SELECT 'distinct', count() FROM (SELECT DISTINCT category FROM test_fsst_fsc);

-- GROUP BY.
SELECT 'group_by', category, count() AS cnt FROM test_fsst_fsc GROUP BY category ORDER BY category;

-- JOIN.
DROP TABLE IF EXISTS test_fsst_fsc_rhs;
CREATE TABLE test_fsst_fsc_rhs (cat String, priority UInt8) ENGINE = MergeTree ORDER BY cat
SETTINGS min_avg_string_length_for_fsst_serialization = 8.0, min_total_bytes_for_fsst_serialization = 0, max_fsst_compression_ratio = 1.0;
INSERT INTO test_fsst_fsc_rhs VALUES ('category_0', 1), ('category_1', 2), ('category_2', 3), ('category_3', 4), ('category_4', 5);

SELECT 'join', l.category, r.priority, count() AS cnt FROM test_fsst_fsc l JOIN test_fsst_fsc_rhs r ON l.category = r.cat GROUP BY l.category, r.priority ORDER BY l.category;

DROP TABLE test_fsst_fsc_rhs;

-- Subquery.
SELECT 'subquery', count() FROM test_fsst_fsc WHERE category IN (SELECT DISTINCT category FROM test_fsst_fsc WHERE id < 100);

DROP TABLE test_fsst_fsc;
