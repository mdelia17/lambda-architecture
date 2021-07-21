CREATE EXTERNAL TABLE input(fields string);
LOAD DATA INPATH 'hdfs://namenode:8020/user/hive/warehouse/data/*' OVERWRITE INTO TABLE input;

ADD FILE /hive/preprocessing_1.py;

CREATE TABLE new_input AS SELECT TRANSFORM(input.fields) USING 'python3 /hive/preprocessing_1.py' AS src, dst, len, num FROM input;

CREATE VIEW v1 AS
SELECT i.src, i.dst, SUM(i.len) AS bytes, SUM(i.num) AS tot 
FROM new_input as i
GROUP BY i.src, i.dst;

CREATE VIEW v2 AS
SELECT v1.dst, v1.src, v1.bytes, v1.tot 
FROM v1;

CREATE VIEW v3 AS
SELECT v1.src, v1.dst, v1.bytes, v1.tot, v2.dst as dst_i, v2.src as src_i, v2.bytes as bytes_i, v2.tot as tot_i
FROM v1 LEFT OUTER JOIN v2 ON v1.src=v2.dst AND v1.dst = v2.src;

CREATE VIEW v4 AS
SELECT DISTINCT
    CASE WHEN src > dst THEN src ELSE dst END as src,
    CASE WHEN src > dst THEN dst ELSE src END as dst
FROM v3
WHERE dst_i IS NOT NULL;

CREATE TABLE v5 AS 
SELECT v4.src, v4.dst, (v3.bytes + v3.bytes_i) as tot_bytes, (v3.tot + v3.tot_i) as tot
FROM v4 JOIN v3 ON v4.src=v3.src AND v4.dst = v3.dst;

INSERT into v5 (src, dst, tot_bytes, tot)
SELECT v3.src, v3.dst, v3.bytes, v3.tot
FROM v3
WHERE dst_i IS NULL;

CREATE VIEW output AS
SELECT src, dst, tot_bytes/tot as average, tot
FROM v5
ORDER BY average DESC;

SELECT * FROM output;

drop TABLE input;
drop TABLE new_input;
DROP VIEW v1;
DROP VIEW v2;
DROP VIEW v3;
drop VIEW v4;
drop TABLE v5;
drop view output;