--@test: decimal - DELIMITED in/out
CREATE STREAM TEST (ID STRING KEY, dec DECIMAL(21,19)) WITH (kafka_topic='test', value_format='DELIMITED');
CREATE STREAM TEST2 AS SELECT * FROM TEST;
INSERT INTO `TEST` (DEC) VALUES (10.1234512345123451234);
ASSERT VALUES `TEST2` (DEC) VALUES (10.1234512345123451234);

--@test: decimal - AVRO in/out
CREATE STREAM TEST (ID STRING KEY, dec DECIMAL(21,19)) WITH (kafka_topic='test', value_format='AVRO');
CREATE STREAM TEST2 AS SELECT * FROM TEST;
INSERT INTO `TEST` (DEC) VALUES (10.1234512345123451234);
ASSERT VALUES `TEST2` (DEC) VALUES (10.1234512345123451234);

--@test: decimal - JSON in/out
CREATE STREAM TEST (ID STRING KEY, dec DECIMAL(21,19)) WITH (kafka_topic='test', value_format='JSON');
CREATE STREAM TEST2 AS SELECT * FROM TEST;
INSERT INTO `TEST` (DEC) VALUES (10.1234512345123451234);
ASSERT VALUES `TEST2` (DEC) VALUES (10.1234512345123451234);

--@test: decimal - PROTOBUF in/out
CREATE STREAM TEST (ID STRING KEY, dec DECIMAL(21,19)) WITH (kafka_topic='test', value_format='PROTOBUF');
CREATE STREAM TEST2 AS SELECT * FROM TEST;
INSERT INTO `TEST` (DEC) VALUES (10.1234512345123451234);
ASSERT VALUES `TEST2` (DEC) VALUES (10.1234512345123451234);

--@test: decimal - PROTOBUF_NOSR in/out
CREATE STREAM TEST (ID STRING KEY, dec DECIMAL(21,19)) WITH (kafka_topic='test', value_format='PROTOBUF_NOSR');
CREATE STREAM TEST2 AS SELECT * FROM TEST;
INSERT INTO `TEST` (DEC) VALUES (10.1234512345123451234);
ASSERT VALUES `TEST2` (DEC) VALUES (10.1234512345123451234);

--@test: decimal - JSON scale in data less than scale in type
CREATE STREAM INPUT (ID STRING KEY, dec DECIMAL(6,4)) WITH (kafka_topic='test', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT;
INSERT INTO `INPUT` (DEC) VALUES (10);
INSERT INTO `INPUT` (DEC) VALUES (1);
INSERT INTO `INPUT` (DEC) VALUES (0.1);
INSERT INTO `INPUT` (DEC) VALUES (0.01);
INSERT INTO `INPUT` (DEC) VALUES (0.001);
INSERT INTO `INPUT` (DEC) VALUES (0.0001);
ASSERT VALUES `OUTPUT` (DEC) VALUES (10.0000);
ASSERT VALUES `OUTPUT` (DEC) VALUES (1.0000);
ASSERT VALUES `OUTPUT` (DEC) VALUES (0.1000);
ASSERT VALUES `OUTPUT` (DEC) VALUES (0.0100);
ASSERT VALUES `OUTPUT` (DEC) VALUES (0.0010);
ASSERT VALUES `OUTPUT` (DEC) VALUES (0.0001);
ASSERT stream INPUT (ID STRING KEY, DEC DECIMAL(6,4)) WITH (KAFKA_TOPIC='test');
ASSERT stream OUTPUT (ID STRING KEY, DEC DECIMAL(6,4)) WITH (KAFKA_TOPIC='OUTPUT');

--@test: decimal - AVRO should not trim trailing zeros
CREATE STREAM INPUT (ID STRING KEY, dec DECIMAL(6,4)) WITH (kafka_topic='test', value_format='Avro');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT;
INSERT INTO `INPUT` (DEC) VALUES (10.0000);
INSERT INTO `INPUT` (DEC) VALUES (1.0000);
ASSERT VALUES `OUTPUT` (DEC) VALUES (10.0000);
ASSERT VALUES `OUTPUT` (DEC) VALUES (1.0000);
ASSERT stream INPUT (ID STRING KEY, DEC DECIMAL(6,4)) WITH (KAFKA_TOPIC='test');
ASSERT stream OUTPUT (ID STRING KEY, DEC DECIMAL(6,4)) WITH (KAFKA_TOPIC='OUTPUT');

--@test: decimal - DELIMITED should not trim trailing zeros
CREATE STREAM INPUT (ID STRING KEY, dec DECIMAL(6,4)) WITH (kafka_topic='test', value_format='Delimited');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT;
INSERT INTO `INPUT` (DEC) VALUES (10.0000);
INSERT INTO `INPUT` (DEC) VALUES (1.0000);
ASSERT VALUES `OUTPUT` (DEC) VALUES (10.0000);
ASSERT VALUES `OUTPUT` (DEC) VALUES (1.0000);
ASSERT stream INPUT (ID STRING KEY, DEC DECIMAL(6,4)) WITH (KAFKA_TOPIC='test');
ASSERT stream OUTPUT (ID STRING KEY, DEC DECIMAL(6,4)) WITH (KAFKA_TOPIC='OUTPUT');

--@test: decimal - JSON should not trim trailing zeros
CREATE STREAM INPUT (ID STRING KEY, dec DECIMAL(6,4)) WITH (kafka_topic='test', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT;
INSERT INTO `INPUT` (DEC) VALUES (10.0);
INSERT INTO `INPUT` (DEC) VALUES (1.0000);
ASSERT VALUES `OUTPUT` (DEC) VALUES (10.0000);
ASSERT VALUES `OUTPUT` (DEC) VALUES (1.0000);
ASSERT stream INPUT (ID STRING KEY, DEC DECIMAL(6,4)) WITH (KAFKA_TOPIC='test');
ASSERT stream OUTPUT (ID STRING KEY, DEC DECIMAL(6,4)) WITH (KAFKA_TOPIC='OUTPUT');

--@test: decimal - JSON_SR should not trim trailing zeros
CREATE STREAM INPUT (ID STRING KEY, dec DECIMAL(6,4)) WITH (kafka_topic='test', value_format='JSON_SR');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT;
INSERT INTO `INPUT` (DEC) VALUES (10.0);
INSERT INTO `INPUT` (DEC) VALUES (1.0000);
ASSERT VALUES `OUTPUT` (DEC) VALUES (10.0000);
ASSERT VALUES `OUTPUT` (DEC) VALUES (1.0000);
ASSERT stream INPUT (ID STRING KEY, DEC DECIMAL(6,4)) WITH (KAFKA_TOPIC='test');
ASSERT stream OUTPUT (ID STRING KEY, DEC DECIMAL(6,4)) WITH (KAFKA_TOPIC='OUTPUT');

--@test: decimal - PROTOBUF should not trim trailing zeros
CREATE STREAM INPUT (ID STRING KEY, dec DECIMAL(6,4)) WITH (kafka_topic='test', value_format='PROTOBUF');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT;
INSERT INTO `INPUT` (DEC) VALUES (10.0);
INSERT INTO `INPUT` (DEC) VALUES (1.0000);
ASSERT VALUES `OUTPUT` (DEC) VALUES (10.0000);
ASSERT VALUES `OUTPUT` (DEC) VALUES (1.0000);
ASSERT stream INPUT (ID STRING KEY, DEC DECIMAL(6,4)) WITH (KAFKA_TOPIC='test');
ASSERT stream OUTPUT (ID STRING KEY, DEC DECIMAL(6,4)) WITH (KAFKA_TOPIC='OUTPUT');

--@test: decimal - PROTOBUF_NOSR should not trim trailing zeros
CREATE STREAM INPUT (ID STRING KEY, dec DECIMAL(6,4)) WITH (kafka_topic='test', value_format='PROTOBUF_NOSR');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT;
INSERT INTO `INPUT` (DEC) VALUES (10.0);
INSERT INTO `INPUT` (DEC) VALUES (1.0000);
ASSERT VALUES `OUTPUT` (DEC) VALUES (10.0000);
ASSERT VALUES `OUTPUT` (DEC) VALUES (1.0000);
ASSERT stream INPUT (ID STRING KEY, DEC DECIMAL(6,4)) WITH (KAFKA_TOPIC='test');
ASSERT stream OUTPUT (ID STRING KEY, DEC DECIMAL(6,4)) WITH (KAFKA_TOPIC='OUTPUT');

--@test: decimal - negation
CREATE STREAM TEST (ID STRING KEY, dec DECIMAL(7,5)) WITH (kafka_topic='test', value_format='AVRO');
CREATE STREAM TEST2 AS SELECT ID, -dec AS negated FROM TEST;
INSERT INTO `TEST` (DEC) VALUES (10.12345);
ASSERT VALUES `TEST2` (NEGATED) VALUES (-10.12345);

--@test: decimal - addition
CREATE STREAM TEST (ID STRING KEY, a DECIMAL(4,2), b DECIMAL(4,2)) WITH (kafka_topic='test', value_format='AVRO');
CREATE STREAM TEST2 AS SELECT ID, (a + b) AS RESULT FROM TEST;
INSERT INTO `TEST` (A, B) VALUES (10.01, 5.10);
INSERT INTO `TEST` (A, B) VALUES (10.01, -5.00);
INSERT INTO `TEST` (A, B) VALUES (10.01, 0.00);
ASSERT VALUES `TEST2` (RESULT) VALUES (15.11);
ASSERT VALUES `TEST2` (RESULT) VALUES (5.01);
ASSERT VALUES `TEST2` (RESULT) VALUES (10.01);

--@test: decimal - addition with double
CREATE STREAM TEST (ID STRING KEY, a DECIMAL(4,2), b DOUBLE) WITH (kafka_topic='test', value_format='AVRO');
CREATE STREAM TEST2 AS SELECT ID, (a + b) AS RESULT FROM TEST;
INSERT INTO `TEST` (A, B) VALUES (10.01, 5.1);
INSERT INTO `TEST` (A, B) VALUES (10.01, -5.0);
INSERT INTO `TEST` (A, B) VALUES (10.01, 0.0);
ASSERT VALUES `TEST2` (RESULT) VALUES (15.11);
ASSERT VALUES `TEST2` (RESULT) VALUES (5.01);
ASSERT VALUES `TEST2` (RESULT) VALUES (10.01);

--@test: decimal - addition with int
CREATE STREAM TEST (ID STRING KEY, a DECIMAL(4,2), b INT) WITH (kafka_topic='test', value_format='AVRO');
CREATE STREAM TEST2 AS SELECT ID, (a + b) AS RESULT FROM TEST;
INSERT INTO `TEST` (A, B) VALUES (10.01, 5);
INSERT INTO `TEST` (A, B) VALUES (10.01, -5);
INSERT INTO `TEST` (A, B) VALUES (10.01, 0);
ASSERT VALUES `TEST2` (RESULT) VALUES (15.01);
ASSERT VALUES `TEST2` (RESULT) VALUES (5.01);
ASSERT VALUES `TEST2` (RESULT) VALUES (10.01);

--@test: decimal - addition 3 columns
CREATE STREAM TEST (ID STRING KEY, a DECIMAL(4,2), b DECIMAL(4,2)) WITH (kafka_topic='test', value_format='AVRO');
CREATE STREAM TEST2 AS SELECT ID, (a + a + b) AS RESULT FROM TEST;
INSERT INTO `TEST` (A, B) VALUES (10.01, 5.10);
ASSERT VALUES `TEST2` (RESULT) VALUES (25.12);

--@test: decimal - subtraction
CREATE STREAM TEST (ID STRING KEY, a DECIMAL(4,2), b DECIMAL(4,2)) WITH (kafka_topic='test', value_format='AVRO');
CREATE STREAM TEST2 AS SELECT ID, (a - b) AS RESULT FROM TEST;
INSERT INTO `TEST` (A, B) VALUES (10.10, 5.10);
INSERT INTO `TEST` (A, B) VALUES (10.10, -5.00);
INSERT INTO `TEST` (A, B) VALUES (10.10, 0.00);
ASSERT VALUES `TEST2` (RESULT) VALUES (5.00);
ASSERT VALUES `TEST2` (RESULT) VALUES (15.10);
ASSERT VALUES `TEST2` (RESULT) VALUES (10.10);

--@test: decimal - multiplication
CREATE STREAM TEST (ID STRING KEY, a DECIMAL(4,2), b DECIMAL(4,2)) WITH (kafka_topic='test', value_format='AVRO');
CREATE STREAM TEST2 AS SELECT ID, (a * b) AS RESULT FROM TEST;
INSERT INTO `TEST` (A, B) VALUES (10.10, 2.00);
INSERT INTO `TEST` (A, B) VALUES (10.10, -2.00);
INSERT INTO `TEST` (A, B) VALUES (10.10, 0.00);
ASSERT VALUES `TEST2` (RESULT) VALUES (20.2000);
ASSERT VALUES `TEST2` (RESULT) VALUES (-20.2000);
ASSERT VALUES `TEST2` (RESULT) VALUES (0.0000);

--@test: decimal - division
CREATE STREAM TEST (ID STRING KEY, a DECIMAL(4,2), b DECIMAL(4,2)) WITH (kafka_topic='test', value_format='AVRO');
CREATE STREAM TEST2 AS SELECT ID, (a / b) AS RESULT FROM TEST;
INSERT INTO `TEST` (A, B) VALUES (10.10, 2.00);
INSERT INTO `TEST` (A, B) VALUES (10.10, -2.00);
INSERT INTO `TEST` (A, B) VALUES (10.10, 0.00);
ASSERT VALUES `TEST2` (RESULT) VALUES (5.0500000);
ASSERT VALUES `TEST2` (RESULT) VALUES (-5.0500000);
ASSERT VALUES `TEST2` (RESULT) VALUES (NULL);

--@test: decimal - mod
CREATE STREAM TEST (ID STRING KEY, a DECIMAL(4,2), b DECIMAL(4,2)) WITH (kafka_topic='test', value_format='AVRO');
CREATE STREAM TEST2 AS SELECT ID, (a % b) AS RESULT FROM TEST;
INSERT INTO `TEST` (A, B) VALUES (10.10, 2.00);
INSERT INTO `TEST` (A, B) VALUES (10.10, -2.00);
INSERT INTO `TEST` (A, B) VALUES (10.10, 0.00);
ASSERT VALUES `TEST2` (RESULT) VALUES (0.10);
ASSERT VALUES `TEST2` (RESULT) VALUES (0.10);
ASSERT VALUES `TEST2` (RESULT) VALUES (NULL);

--@test: decimal - equal - decimal decimal
CREATE STREAM TEST (ID STRING KEY, a DECIMAL(4,2), b DECIMAL(4,2)) WITH (kafka_topic='test', value_format='AVRO');
CREATE STREAM TEST2 AS SELECT ID, (a = b) AS RESULT FROM TEST;
INSERT INTO `TEST` (A, B) VALUES (10.01, 10.01);
INSERT INTO `TEST` (A, B) VALUES (10.01, 12.01);
INSERT INTO `TEST` (A, B) VALUES (NULL, 10.01);
INSERT INTO `TEST` (A, B) VALUES (NULL, NULL);
ASSERT VALUES `TEST2` (RESULT) VALUES (true);
ASSERT VALUES `TEST2` (RESULT) VALUES (false);
ASSERT VALUES `TEST2` (RESULT) VALUES (false);
ASSERT VALUES `TEST2` (RESULT) VALUES (false);

--@test: decimal - not equal - decimal decimal
CREATE STREAM TEST (ID STRING KEY, a DECIMAL(4,2), b DECIMAL(4,2)) WITH (kafka_topic='test', value_format='AVRO');
CREATE STREAM TEST2 AS SELECT ID, (a <> b) AS RESULT FROM TEST;
INSERT INTO `TEST` (A, B) VALUES (10.01, 10.01);
INSERT INTO `TEST` (A, B) VALUES (10.01, 12.01);
INSERT INTO `TEST` (A, B) VALUES (NULL, 10.01);
INSERT INTO `TEST` (A, B) VALUES (NULL, NULL);
ASSERT VALUES `TEST2` (RESULT) VALUES (false);
ASSERT VALUES `TEST2` (RESULT) VALUES (true);
ASSERT VALUES `TEST2` (RESULT) VALUES (false);
ASSERT VALUES `TEST2` (RESULT) VALUES (false);

--@test: decimal - is distinct - decimal decimal
CREATE STREAM TEST (ID STRING KEY, a DECIMAL(4,2), b DECIMAL(4,2)) WITH (kafka_topic='test', value_format='AVRO');
CREATE STREAM TEST2 AS SELECT ID, (a IS DISTINCT FROM b) AS RESULT FROM TEST;
INSERT INTO `TEST` (A, B) VALUES (10.01, 10.01);
INSERT INTO `TEST` (A, B) VALUES (10.01, 12.01);
INSERT INTO `TEST` (A, B) VALUES (NULL, 10.01);
INSERT INTO `TEST` (A, B) VALUES (NULL, NULL);
ASSERT VALUES `TEST2` (RESULT) VALUES (false);
ASSERT VALUES `TEST2` (RESULT) VALUES (true);
ASSERT VALUES `TEST2` (RESULT) VALUES (true);
ASSERT VALUES `TEST2` (RESULT) VALUES (false);

--@test: decimal - less than - decimal decimal
CREATE STREAM TEST (ID STRING KEY, a DECIMAL(4,2), b DECIMAL(4,2)) WITH (kafka_topic='test', value_format='AVRO');
CREATE STREAM TEST2 AS SELECT ID, (a < b) AS RESULT FROM TEST;
INSERT INTO `TEST` (A, B) VALUES (10.01, 10.01);
INSERT INTO `TEST` (A, B) VALUES (10.01, 12.01);
INSERT INTO `TEST` (A, B) VALUES (NULL, 10.01);
INSERT INTO `TEST` (A, B) VALUES (NULL, NULL);
ASSERT VALUES `TEST2` (RESULT) VALUES (false);
ASSERT VALUES `TEST2` (RESULT) VALUES (true);
ASSERT VALUES `TEST2` (RESULT) VALUES (false);
ASSERT VALUES `TEST2` (RESULT) VALUES (false);

--@test: decimal - less than - decimal decimal differing scale
CREATE STREAM TEST (ID STRING KEY, a DECIMAL(4,2), b DECIMAL(5,3)) WITH (kafka_topic='test', value_format='AVRO');
CREATE STREAM TEST2 AS SELECT ID, (a < b) AS RESULT FROM TEST;
INSERT INTO `TEST` (A, B) VALUES (10.01, 10.010);
INSERT INTO `TEST` (A, B) VALUES (10.01, 10.012);
INSERT INTO `TEST` (A, B) VALUES (NULL, 10.010);
INSERT INTO `TEST` (A, B) VALUES (NULL, NULL);
ASSERT VALUES `TEST2` (RESULT) VALUES (false);
ASSERT VALUES `TEST2` (RESULT) VALUES (true);
ASSERT VALUES `TEST2` (RESULT) VALUES (false);
ASSERT VALUES `TEST2` (RESULT) VALUES (false);

--@test: decimal - LEQ - decimal decimal
CREATE STREAM TEST (ID STRING KEY, a DECIMAL(4,2), b DECIMAL(4,2)) WITH (kafka_topic='test', value_format='AVRO');
CREATE STREAM TEST2 AS SELECT ID, (a <= b) AS RESULT FROM TEST;
INSERT INTO `TEST` (A, B) VALUES (10.01, 3.01);
INSERT INTO `TEST` (A, B) VALUES (10.01, 10.01);
INSERT INTO `TEST` (A, B) VALUES (10.01, 12.01);
INSERT INTO `TEST` (A, B) VALUES (NULL, 10.01);
INSERT INTO `TEST` (A, B) VALUES (NULL, NULL);
ASSERT VALUES `TEST2` (RESULT) VALUES (false);
ASSERT VALUES `TEST2` (RESULT) VALUES (true);
ASSERT VALUES `TEST2` (RESULT) VALUES (true);
ASSERT VALUES `TEST2` (RESULT) VALUES (false);
ASSERT VALUES `TEST2` (RESULT) VALUES (false);

--@test: decimal - GEQ - decimal decimal
CREATE STREAM TEST (ID STRING KEY, a DECIMAL(4,2), b DECIMAL(4,2)) WITH (kafka_topic='test', value_format='AVRO');
CREATE STREAM TEST2 AS SELECT ID, (a >= b) AS RESULT FROM TEST;
INSERT INTO `TEST` (A, B) VALUES (10.01, 3.01);
INSERT INTO `TEST` (A, B) VALUES (10.01, 10.01);
INSERT INTO `TEST` (A, B) VALUES (10.01, 12.01);
INSERT INTO `TEST` (A, B) VALUES (NULL, 10.01);
INSERT INTO `TEST` (A, B) VALUES (NULL, NULL);
ASSERT VALUES `TEST2` (RESULT) VALUES (true);
ASSERT VALUES `TEST2` (RESULT) VALUES (true);
ASSERT VALUES `TEST2` (RESULT) VALUES (false);
ASSERT VALUES `TEST2` (RESULT) VALUES (false);
ASSERT VALUES `TEST2` (RESULT) VALUES (false);

--@test: decimal - greater than - decimal decimal
CREATE STREAM TEST (ID STRING KEY, a DECIMAL(4,2), b DECIMAL(4,2)) WITH (kafka_topic='test', value_format='AVRO');
CREATE STREAM TEST2 AS SELECT ID, (a > b) AS RESULT FROM TEST;
INSERT INTO `TEST` (A, B) VALUES (10.01, 3.01);
INSERT INTO `TEST` (A, B) VALUES (10.01, 10.01);
INSERT INTO `TEST` (A, B) VALUES (10.01, 12.01);
INSERT INTO `TEST` (A, B) VALUES (NULL, 10.01);
INSERT INTO `TEST` (A, B) VALUES (NULL, NULL);
ASSERT VALUES `TEST2` (RESULT) VALUES (true);
ASSERT VALUES `TEST2` (RESULT) VALUES (false);
ASSERT VALUES `TEST2` (RESULT) VALUES (false);
ASSERT VALUES `TEST2` (RESULT) VALUES (false);
ASSERT VALUES `TEST2` (RESULT) VALUES (false);

--@test: decimal - less than - decimal int
CREATE STREAM TEST (ID STRING KEY, a DECIMAL(4,2), b INTEGER) WITH (kafka_topic='test', value_format='AVRO');
CREATE STREAM TEST2 AS SELECT ID, (a < b) AS RESULT FROM TEST;
INSERT INTO `TEST` (A, B) VALUES (10.01, 1);
INSERT INTO `TEST` (A, B) VALUES (10.01, 12);
INSERT INTO `TEST` (A, B) VALUES (NULL, 12);
INSERT INTO `TEST` (A, B) VALUES (NULL, NULL);
ASSERT VALUES `TEST2` (RESULT) VALUES (false);
ASSERT VALUES `TEST2` (RESULT) VALUES (true);
ASSERT VALUES `TEST2` (RESULT) VALUES (false);
ASSERT VALUES `TEST2` (RESULT) VALUES (false);

--@test: decimal - decimal between -1 and 1
CREATE STREAM TEST (ID STRING KEY, dec DECIMAL(4,2)) WITH (kafka_topic='test', value_format='DELIMITED');
CREATE STREAM TEST2 AS SELECT * FROM TEST WHERE dec < 0.08 AND dec > -0.08;
INSERT INTO `TEST` (DEC) VALUES (0.05);
INSERT INTO `TEST` (DEC) VALUES (0.55);
INSERT INTO `TEST` (DEC) VALUES (-0.5);
ASSERT VALUES `TEST2` (DEC) VALUES (0.05);

