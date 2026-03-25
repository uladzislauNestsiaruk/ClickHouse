-- Test: FSST with aggregate and window functions.

DROP TABLE IF EXISTS test_fsst_agg;
CREATE TABLE test_fsst_agg (id UInt64, region String, service String, latency_ms UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS min_avg_string_length_for_fsst_serialization = 8.0, min_total_bytes_for_fsst_serialization = 16384, max_fsst_compression_ratio = 1.0;

INSERT INTO test_fsst_agg SELECT number, concat('region-', toString(number % 4), '-datacenter-east'), concat('service-', toString(number % 10), '-api-gateway'), (number * 7 + 13) % 5000 FROM numbers(10000);

SELECT 'fsst_check', column, serialization_kind FROM system.parts_columns WHERE database = currentDatabase() AND table = 'test_fsst_agg' AND column IN ('region', 'service') AND active GROUP BY column, serialization_kind ORDER BY column;

-- Basic aggregates.
SELECT 'min_region', min(region) FROM test_fsst_agg;
SELECT 'max_region', max(region) FROM test_fsst_agg;
SELECT 'min_service', min(service) FROM test_fsst_agg;
SELECT 'max_service', max(service) FROM test_fsst_agg;

-- any.
SELECT 'any_region', any(region) IN ('region-0-datacenter-east', 'region-1-datacenter-east', 'region-2-datacenter-east', 'region-3-datacenter-east') FROM test_fsst_agg;

-- groupUniqArray.
SELECT 'uniq_regions', length(groupUniqArray(region)) FROM test_fsst_agg;
SELECT 'uniq_services', length(groupUniqArray(service)) FROM test_fsst_agg;

-- topK.
SELECT 'topk', arraySort(topK(4)(region)) FROM test_fsst_agg;

-- GROUP BY two FSST columns.
SELECT 'double_group', region, service, avg(latency_ms) AS avg_lat FROM test_fsst_agg GROUP BY region, service ORDER BY region, service LIMIT 5;

-- Window function: ROW_NUMBER partitioned by FSST column.
SELECT 'window_rn', region, id, rn FROM (SELECT region, id, row_number() OVER (PARTITION BY region ORDER BY latency_ms DESC) AS rn FROM test_fsst_agg) WHERE rn <= 2 ORDER BY region, rn LIMIT 8;

DROP TABLE test_fsst_agg;
