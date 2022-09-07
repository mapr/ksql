--@test: count - count
CREATE STREAM TEST (ID BIGINT KEY, NAME varchar, VALUE double) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE S2 as SELECT id, count() FROM test group by id;
INSERT INTO `TEST` (ID, NAME, VALUE) VALUES (0, 'zero', 0.0);
INSERT INTO `TEST` (ID, NAME, VALUE) VALUES (0, '100', 0.0);
INSERT INTO `TEST` (ID, NAME, VALUE) VALUES (100, '100', 0.0);
ASSERT VALUES `S2` (ID, KSQL_COL_0) VALUES (0, 1);
ASSERT VALUES `S2` (ID, KSQL_COL_0) VALUES (0, 2);
ASSERT VALUES `S2` (ID, KSQL_COL_0) VALUES (100, 1);

--@test: count - count star
CREATE STREAM INPUT (ID STRING KEY, ignored STRING) WITH (kafka_topic='input_topic', value_format='DELIMITED');
CREATE TABLE OUTPUT as SELECT ID, count(*) FROM input group by ID;
INSERT INTO `INPUT` (ID, IGNORED) VALUES ('0', '-');
INSERT INTO `INPUT` (ID, IGNORED) VALUES ('0', '-');
INSERT INTO `INPUT` (ID, IGNORED) VALUES ('100', '-');
ASSERT VALUES `OUTPUT` (ID, KSQL_COL_0) VALUES ('0', 1);
ASSERT VALUES `OUTPUT` (ID, KSQL_COL_0) VALUES ('0', 2);
ASSERT VALUES `OUTPUT` (ID, KSQL_COL_0) VALUES ('100', 1);

--@test: count - count literal
CREATE STREAM INPUT (ID STRING KEY, ignored STRING) WITH (kafka_topic='input_topic', value_format='DELIMITED');
CREATE TABLE OUTPUT as SELECT ID, count(1) FROM input group by ID;
INSERT INTO `INPUT` (ID, IGNORED) VALUES ('0', '-');
INSERT INTO `INPUT` (ID, IGNORED) VALUES ('0', '-');
INSERT INTO `INPUT` (ID, IGNORED) VALUES ('100', '-');
INSERT INTO `INPUT` (ID) VALUES ('100');
INSERT INTO `INPUT` (ID, IGNORED) VALUES ('100', '-');
ASSERT VALUES `OUTPUT` (ID, KSQL_COL_0) VALUES ('0', 1);
ASSERT VALUES `OUTPUT` (ID, KSQL_COL_0) VALUES ('0', 2);
ASSERT VALUES `OUTPUT` (ID, KSQL_COL_0) VALUES ('100', 1);
ASSERT VALUES `OUTPUT` (ID, KSQL_COL_0) VALUES ('100', 2);

--@test: count - count table
CREATE TABLE INPUT (ID STRING PRIMARY KEY, name STRING) WITH (kafka_topic='input_topic', value_format='DELIMITED');
CREATE TABLE OUTPUT as SELECT NAME, count(1) FROM input group by name;
INSERT INTO `INPUT` (ID, NAME) VALUES ('0', 'bob');
INSERT INTO `INPUT` (ID, NAME) VALUES ('0', 'john');
INSERT INTO `INPUT` (ID, NAME) VALUES ('100', 'john');
INSERT INTO `INPUT` (ID) VALUES ('100');
ASSERT VALUES `OUTPUT` (NAME, KSQL_COL_0) VALUES ('bob', 1);
ASSERT VALUES `OUTPUT` (NAME, KSQL_COL_0) VALUES ('bob', 0);
ASSERT VALUES `OUTPUT` (NAME, KSQL_COL_0) VALUES ('john', 1);
ASSERT VALUES `OUTPUT` (NAME, KSQL_COL_0) VALUES ('john', 2);
ASSERT VALUES `OUTPUT` (NAME, KSQL_COL_0) VALUES ('john', 1);

--@test: count - should count back to zero
CREATE TABLE INPUT (K STRING PRIMARY KEY, ID bigint) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE OUTPUT as SELECT ID, COUNT() FROM INPUT GROUP BY ID;
INSERT INTO `INPUT` (K, ID) VALUES ('1', 3);
INSERT INTO `INPUT` (K, ID) VALUES ('2', 3);
INSERT INTO `INPUT` (K) VALUES ('1');
INSERT INTO `INPUT` (K) VALUES ('2');
ASSERT VALUES `OUTPUT` (ID, KSQL_COL_0) VALUES (3, 1);
ASSERT VALUES `OUTPUT` (ID, KSQL_COL_0) VALUES (3, 2);
ASSERT VALUES `OUTPUT` (ID, KSQL_COL_0) VALUES (3, 1);
ASSERT VALUES `OUTPUT` (ID, KSQL_COL_0) VALUES (3, 0);

--@test: count - should support removing zero counts from table
CREATE TABLE INPUT (K STRING PRIMARY KEY, ID bigint) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE OUTPUT as SELECT ID, COUNT() FROM INPUT GROUP BY ID HAVING COUNT() > 0;
INSERT INTO `INPUT` (K, ID) VALUES ('1', 3);
INSERT INTO `INPUT` (K, ID) VALUES ('2', 3);
INSERT INTO `INPUT` (K) VALUES ('1');
INSERT INTO `INPUT` (K) VALUES ('2');
ASSERT VALUES `OUTPUT` (ID, KSQL_COL_0) VALUES (3, 1);
ASSERT VALUES `OUTPUT` (ID, KSQL_COL_0) VALUES (3, 2);
ASSERT VALUES `OUTPUT` (ID, KSQL_COL_0) VALUES (3, 1);
ASSERT VALUES `OUTPUT` (ID) VALUES (3);

--@test: count - auto-incrementing id
CREATE STREAM INPUT (VALUE INT) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE TABLE OUTPUT as SELECT 1 as k, latest_by_offset(value) as value, count(1) AS ID FROM INPUT group by 1;
INSERT INTO `INPUT` (VALUE) VALUES (12);
INSERT INTO `INPUT` (VALUE) VALUES (8367);
INSERT INTO `INPUT` (VALUE) VALUES (764);
ASSERT VALUES `OUTPUT` (K, VALUE, ID) VALUES (1, 12, 1);
ASSERT VALUES `OUTPUT` (K, VALUE, ID) VALUES (1, 8367, 2);
ASSERT VALUES `OUTPUT` (K, VALUE, ID) VALUES (1, 764, 3);

