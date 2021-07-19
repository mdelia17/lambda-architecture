CREATE EXTERNAL TABLE input(fields string);
LOAD DATA INPATH 'hdfs://namenode:8020/topics/network-data/partition=0/*' OVERWRITE INTO TABLE input;

ADD FILE /hive-job/preprocessing_1.py;

CREATE TABLE new_input AS
	SELECT TRANSFORM(input.fields)
	    USING 'python3 /hive-job/preprocessing_1.py' AS src, dst, len, num
	FROM input;

CREATE VIEW v1 AS
SELECT i.src, i.dst, SUM(i.len) AS bytes, SUM(i.num) AS tot 
FROM new_input as i
GROUP BY i.src, i.dst;

CREATE VIEW v2 AS
SELECT v1.dst, v1.src, v1.bytes, v1.tot 
FROM v1;

CREATE TABLE v3 AS
SELECT v1.src, v1.dst, v1.bytes, v1.tot, v2.dst as dst_i, v2.src as src_i, v2.bytes as bytes_i, v2.tot as tot_i
FROM v1 LEFT OUTER JOIN v2 ON v1.src=v2.dst AND v1.dst = v2.src;

SELECT * FROM v3;

-- ADD FILE /hive-job/filter.py;

-- CREATE TABLE filtered AS
-- 	SELECT TRANSFORM(src, dst, bytes, tot)
-- 	    USING 'python 3 /hive-job/filter.py' AS src, dst, bytes, tot
-- 	FROM v2;

-- SELECT * FROM filtered;

drop table input;
drop table new_input;
DROP VIEW v1;
DROP VIEW v2;
DROP table v3;
drop table filtered;