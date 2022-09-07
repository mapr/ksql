--@test: bytes - DELIMITED in/out
CREATE STREAM TEST (ID STRING KEY, b BYTES) WITH (kafka_topic='test', value_format='DELIMITED');
CREATE STREAM TEST2 AS SELECT * FROM TEST;
INSERT INTO `TEST` (B) VALUES ('dmFyaWF0aW9ucw==');
ASSERT VALUES `TEST2` (B) VALUES ('dmFyaWF0aW9ucw==');

--@test: bytes - KAFKA in/out
CREATE STREAM TEST (ID STRING KEY, b BYTES) WITH (kafka_topic='test', value_format='KAFKA');
CREATE STREAM TEST2 AS SELECT * FROM TEST;
INSERT INTO `TEST` (B) VALUES ('dmFyaWF0aW9ucw==');
ASSERT VALUES `TEST2` (B) VALUES ('dmFyaWF0aW9ucw==');

--@test: bytes - JSON in/out
CREATE STREAM TEST (ID STRING KEY, b BYTES) WITH (kafka_topic='test', value_format='JSON');
CREATE STREAM TEST2 AS SELECT * FROM TEST;
INSERT INTO `TEST` (B) VALUES ('dmFyaWF0aW9ucw==');
ASSERT VALUES `TEST2` (B) VALUES ('dmFyaWF0aW9ucw==');

--@test: bytes - PROTOBUF_NOSR in/out
CREATE STREAM TEST (ID STRING KEY, b BYTES) WITH (kafka_topic='test', value_format='PROTOBUF_NOSR');
CREATE STREAM TEST2 AS SELECT * FROM TEST;
INSERT INTO `TEST` (b) VALUES ('dmFyaWF0aW9ucw==');
ASSERT VALUES `TEST2` (B) VALUES ('dmFyaWF0aW9ucw==');

--@test: bytes - bytes in complex types
CREATE STREAM TEST (ID STRING KEY, S STRUCT<B BYTES>, A ARRAY<BYTES>, M MAP<STRING, BYTES>) WITH (kafka_topic='test', value_format='JSON');
CREATE STREAM TEST2 AS SELECT ID, S -> B AS S, A[1] AS A, M['B'] AS M FROM TEST;
INSERT INTO `TEST` (S, A, M) VALUES (STRUCT(B:='ew=='), ARRAY['ew==', 'ew=='], MAP('B':='ew=='));
ASSERT VALUES `TEST2` (S, A, M) VALUES ('ew==', 'ew==', 'ew==');

--@test: bytes - greater than
CREATE STREAM TEST (ID STRING KEY, a BYTES, b BYTES) WITH (kafka_topic='test', value_format='JSON');
CREATE STREAM TEST2 AS SELECT ID, (a > b) AS RESULT FROM TEST;
INSERT INTO `TEST` (A, B) VALUES ('YQ==', 'YQ==');
INSERT INTO `TEST` (A, B) VALUES (NULL, NULL);
INSERT INTO `TEST` (A, B) VALUES ('YQ==', NULL);
INSERT INTO `TEST` (A, B) VALUES (NULL, 'YQ==');
INSERT INTO `TEST` (A, B) VALUES ('Yg==', 'YQ==');
ASSERT VALUES `TEST2` (RESULT) VALUES (false);
ASSERT VALUES `TEST2` (RESULT) VALUES (false);
ASSERT VALUES `TEST2` (RESULT) VALUES (false);
ASSERT VALUES `TEST2` (RESULT) VALUES (false);
ASSERT VALUES `TEST2` (RESULT) VALUES (true);

--@test: bytes - filter
CREATE STREAM TEST (ID STRING KEY, a BYTES, b BYTES, c BYTES) WITH (kafka_topic='test', value_format='JSON');
CREATE STREAM TEST2 AS SELECT ID, C AS RESULT FROM TEST WHERE b BETWEEN a AND c;
INSERT INTO `TEST` (A, B, C) VALUES ('Yg==', 'YQ==', 'Yg==');
INSERT INTO `TEST` (A, B, C) VALUES (NULL, 'YQ==', 'YQ==');
INSERT INTO `TEST` (A, B, C) VALUES ('YQ==', NULL, 'YQ==');
INSERT INTO `TEST` (A, B, C) VALUES ('YQ==', 'YQ==', NULL);
INSERT INTO `TEST` (A, B, C) VALUES ('YQ==', 'Yg==', 'Yw==');
ASSERT VALUES `TEST2` (RESULT) VALUES ('Yw==');

