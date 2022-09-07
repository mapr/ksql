--@test: middle-variadic-udaf - missing first argument
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Function 'MID_VAR_ARG' does not accept parameters (STRING, STRING, INTEGER, INTEGER).
CREATE STREAM INPUT (ID BIGINT KEY, FIRST bigint, SECOND string, THIRD string) WITH (kafka_topic='input_topic', value_format='JSON');
CREATE TABLE OUTPUT as SELECT id, MID_VAR_ARG(SECOND, THIRD, 2, 3) as RESULT FROM INPUT group by id;
--@test: middle-variadic-udaf - missing initial argument
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Function 'MID_VAR_ARG' does not accept parameters (BIGINT, STRING, STRING, INTEGER).
CREATE STREAM INPUT (ID BIGINT KEY, FIRST bigint, SECOND string, THIRD string) WITH (kafka_topic='input_topic', value_format='JSON');
CREATE TABLE OUTPUT as SELECT id, MID_VAR_ARG(FIRST, SECOND, THIRD, 3) as RESULT FROM INPUT group by id;
--@test: middle-variadic-udaf - var args type mismatch
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Function 'MID_VAR_ARG' does not accept parameters (BIGINT, INTEGER, INTEGER, INTEGER).
CREATE STREAM INPUT (ID BIGINT KEY, FIRST bigint, SECOND integer) WITH (kafka_topic='input_topic', value_format='JSON');
CREATE TABLE OUTPUT as SELECT id, MID_VAR_ARG(FIRST, SECOND, 3, 2) as RESULT FROM INPUT group by id;
--@test: middle-variadic-udaf - all arguments
CREATE STREAM INPUT (ID BIGINT KEY, FIRST bigint, SECOND string, THIRD string, FOURTH string) WITH (kafka_topic='input_topic', value_format='JSON');
CREATE TABLE OUTPUT as SELECT id, MID_VAR_ARG(FIRST, SECOND, THIRD, FOURTH, 7, 3) as RESULT FROM INPUT group by id;
INSERT INTO `INPUT` (ID, FIRST, SECOND, THIRD, FOURTH) VALUES (0, 6, 'hi', 'hello', 'world');
INSERT INTO `INPUT` (ID, FIRST, SECOND, THIRD, FOURTH) VALUES (100, 2, 'a', 'hello', 'world');
INSERT INTO `INPUT` (ID, FIRST, SECOND, THIRD, FOURTH) VALUES (0, NULL, 'hello', 'hello', 'world');
INSERT INTO `INPUT` (ID, FIRST, SECOND, THIRD, FOURTH) VALUES (100, 5, 'world', 'hello', 'world');
INSERT INTO `INPUT` (ID, FIRST, SECOND, THIRD, FOURTH) VALUES (0, 5, NULL, 'hello', 'world');
INSERT INTO `INPUT` (ID, FIRST, SECOND, THIRD, FOURTH) VALUES (100, 3, 'test', 'hello', 'world');
INSERT INTO `INPUT` (ID, FIRST, SECOND, THIRD, FOURTH) VALUES (100, 2, 'testing', 'hello', 'world');
INSERT INTO `INPUT` (ID, FIRST, SECOND, THIRD, FOURTH) VALUES (0, 21, 'aggregate', 'hello', 'world');
INSERT INTO `INPUT` (ID, FIRST, SECOND, THIRD, FOURTH) VALUES (100, NULL, 'function', 'hello', 'world');
INSERT INTO `INPUT` (ID, FIRST, SECOND, THIRD, FOURTH) VALUES (100, 3, 'ksql', 'hello', 'world');
INSERT INTO `INPUT` (ID, FIRST, SECOND, THIRD, FOURTH) VALUES (100, 6, 'test', 'hello', 'world');
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES (0, 28);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES (100, 23);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES (0, 43);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES (100, 43);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES (0, 58);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES (100, 60);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES (100, 79);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES (0, 98);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES (100, 97);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES (100, 114);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES (100, 134);

--@test: middle-variadic-udaf - regular arg literal
CREATE STREAM INPUT (ID BIGINT KEY, FIRST bigint, SECOND string) WITH (kafka_topic='input_topic', value_format='JSON');
CREATE TABLE OUTPUT as SELECT id, MID_VAR_ARG(FIRST, 'hello', '10', '20', SECOND, '3', 7, 3) as RESULT FROM INPUT group by id;
INSERT INTO `INPUT` (ID, FIRST, SECOND) VALUES (0, 6, 'hi');
INSERT INTO `INPUT` (ID, FIRST, SECOND) VALUES (100, 2, 'a');
INSERT INTO `INPUT` (ID, FIRST, SECOND) VALUES (0, NULL, 'hello');
INSERT INTO `INPUT` (ID, FIRST, SECOND) VALUES (100, 5, 'world');
INSERT INTO `INPUT` (ID, FIRST, SECOND) VALUES (0, 5, NULL);
INSERT INTO `INPUT` (ID, FIRST, SECOND) VALUES (100, 3, 'test');
INSERT INTO `INPUT` (ID, FIRST, SECOND) VALUES (100, 2, 'testing');
INSERT INTO `INPUT` (ID, FIRST, SECOND) VALUES (0, 21, 'aggregate');
INSERT INTO `INPUT` (ID, FIRST, SECOND) VALUES (100, NULL, 'function');
INSERT INTO `INPUT` (ID, FIRST, SECOND) VALUES (100, 3, 'ksql');
INSERT INTO `INPUT` (ID, FIRST, SECOND) VALUES (100, 6, 'test');
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES (0, 28);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES (100, 23);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES (0, 43);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES (100, 43);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES (0, 58);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES (100, 60);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES (100, 79);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES (0, 98);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES (100, 97);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES (100, 114);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES (100, 134);

--@test: middle-variadic-udaf - no variadic args
CREATE STREAM INPUT (ID BIGINT KEY, FIRST bigint, SECOND string) WITH (kafka_topic='input_topic', value_format='JSON');
CREATE TABLE OUTPUT as SELECT id, MID_VAR_ARG(FIRST, 7, 3) as RESULT FROM INPUT group by id;
INSERT INTO `INPUT` (ID, FIRST, SECOND) VALUES (0, 6, 'hi');
INSERT INTO `INPUT` (ID, FIRST, SECOND) VALUES (100, 2, 'a');
INSERT INTO `INPUT` (ID, FIRST, SECOND) VALUES (0, NULL, 'hello');
INSERT INTO `INPUT` (ID, FIRST, SECOND) VALUES (100, 5, 'world');
INSERT INTO `INPUT` (ID, FIRST, SECOND) VALUES (0, 5, NULL);
INSERT INTO `INPUT` (ID, FIRST, SECOND) VALUES (100, 3, 'test');
INSERT INTO `INPUT` (ID, FIRST, SECOND) VALUES (100, 2, 'testing');
INSERT INTO `INPUT` (ID, FIRST, SECOND) VALUES (0, 21, 'aggregate');
INSERT INTO `INPUT` (ID, FIRST, SECOND) VALUES (100, NULL, 'function');
INSERT INTO `INPUT` (ID, FIRST, SECOND) VALUES (100, 3, 'ksql');
INSERT INTO `INPUT` (ID, FIRST, SECOND) VALUES (100, 6, 'test');
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES (0, 16);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES (100, 12);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES (0, 16);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES (100, 17);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES (0, 21);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES (100, 20);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES (100, 22);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES (0, 42);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES (100, 22);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES (100, 25);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES (100, 31);

