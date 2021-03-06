CREATE EXTERNAL TABLE input(fields string);
LOAD DATA INPATH 'hdfs://namenode:8020/user/hive/warehouse/data/*' OVERWRITE INTO TABLE input;

ADD FILE /hive/preprocessing_2.py;

CREATE TABLE new_input AS
	SELECT TRANSFORM(input.fields)
	    USING 'python3 /hive/preprocessing_2.py' AS type, name, ip
	FROM input;

CREATE VIEW v1 AS
SELECT i.type, i.name, size(Collect_set(i.ip)) as n
FROM new_input as i
GROUP BY i.type, i.name
ORDER BY n DESC;

SELECT * FROM v1;

drop table input;
drop table new_input;
drop view v1;