--@test: string - < operator
CREATE STREAM INPUT (K STRING KEY, text STRING) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE STREAM OUTPUT AS select K, text, text < 'b2' from INPUT;
INSERT INTO `INPUT` (TEXT) VALUES ('a1');
INSERT INTO `INPUT` (TEXT) VALUES ('b1');
INSERT INTO `INPUT` (TEXT) VALUES ('B2');
INSERT INTO `INPUT` (TEXT) VALUES ('b2');
INSERT INTO `INPUT` (TEXT) VALUES ('b3');
INSERT INTO `INPUT` (TEXT) VALUES ('b10');
INSERT INTO `INPUT` (TEXT) VALUES ('b01');
ASSERT VALUES `OUTPUT` (TEXT, KSQL_COL_0) VALUES ('a1', true);
ASSERT VALUES `OUTPUT` (TEXT, KSQL_COL_0) VALUES ('b1', true);
ASSERT VALUES `OUTPUT` (TEXT, KSQL_COL_0) VALUES ('B2', true);
ASSERT VALUES `OUTPUT` (TEXT, KSQL_COL_0) VALUES ('b2', false);
ASSERT VALUES `OUTPUT` (TEXT, KSQL_COL_0) VALUES ('b3', false);
ASSERT VALUES `OUTPUT` (TEXT, KSQL_COL_0) VALUES ('b10', true);
ASSERT VALUES `OUTPUT` (TEXT, KSQL_COL_0) VALUES ('b01', true);
ASSERT stream OUTPUT (K STRING KEY, TEXT STRING, KSQL_COL_0 BOOLEAN) WITH (KAFKA_TOPIC='OUTPUT', VALUE_FORMAT='DELIMITED');

--@test: string - LCASE, UCASE, TRIM SUBSTRING
CREATE STREAM INPUT (K STRING KEY, text STRING) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE STREAM OUTPUT AS select K, LCASE(text), UCASE(text), TRIM(text), SUBSTRING(text, 2, 5) from INPUT;
INSERT INTO `INPUT` (TEXT) VALUES ('lower');
INSERT INTO `INPUT` (TEXT) VALUES ('UPPER');
INSERT INTO `INPUT` (TEXT) VALUES ('MiXeD');
INSERT INTO `INPUT` (TEXT) VALUES (' 	 with white space 	');
INSERT INTO `INPUT` (TEXT) VALUES ('s');
INSERT INTO `INPUT` (TEXT) VALUES ('long enough');
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1, KSQL_COL_2, KSQL_COL_3) VALUES ('lower', 'LOWER', 'lower', 'ower');
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1, KSQL_COL_2, KSQL_COL_3) VALUES ('upper', 'UPPER', 'UPPER', 'PPER');
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1, KSQL_COL_2, KSQL_COL_3) VALUES ('mixed', 'MIXED', 'MiXeD', 'iXeD');
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1, KSQL_COL_2, KSQL_COL_3) VALUES (' 	 with white space 	', ' 	 WITH WHITE SPACE 	', 'with white space', '	 wit');
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1, KSQL_COL_2, KSQL_COL_3) VALUES ('s', 'S', 's', NULL);
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1, KSQL_COL_2, KSQL_COL_3) VALUES ('long enough', 'LONG ENOUGH', 'long enough', 'ong e');
ASSERT stream OUTPUT (K STRING KEY, KSQL_COL_0 STRING, KSQL_COL_1 STRING, KSQL_COL_2 STRING, KSQL_COL_3 STRING) WITH (KAFKA_TOPIC='OUTPUT', VALUE_FORMAT='DELIMITED');

