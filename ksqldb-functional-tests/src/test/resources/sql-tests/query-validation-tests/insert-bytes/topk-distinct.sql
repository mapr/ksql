--@test: topk-distinct - topk distinct integer
CREATE STREAM TEST (ID BIGINT KEY, NAME varchar, VALUE integer) WITH (kafka_topic='test_topic',value_format='JSON');
CREATE TABLE S2 as SELECT ID, topkdistinct(value, 3) as topk FROM test group by id;
INSERT INTO `TEST` (ID, NAME, VALUE) VALUES (0, 'zero', 0);
INSERT INTO `TEST` (ID, NAME, VALUE) VALUES (0, 'zero', 100);
INSERT INTO `TEST` (ID, NAME, VALUE) VALUES (0, 'zero', 99);
INSERT INTO `TEST` (ID, NAME, VALUE) VALUES (0, 'zero', 7);
INSERT INTO `TEST` (ID, NAME, VALUE) VALUES (0, 'zero', 100);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[0]);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[100, 0]);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[100, 99, 0]);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[100, 99, 7]);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[100, 99, 7]);

--@test: topk-distinct - topk distinct long
CREATE STREAM TEST (ID BIGINT KEY, NAME varchar, VALUE bigint) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE TABLE S2 as SELECT ID, topkdistinct(value, 3) as topk FROM test group by id;
INSERT INTO `TEST` (ID, NAME, VALUE) VALUES (0, 'zero', 2147483648);
INSERT INTO `TEST` (ID, NAME, VALUE) VALUES (0, 'zero', 100);
INSERT INTO `TEST` (ID, NAME, VALUE) VALUES (0, 'zero', 99);
INSERT INTO `TEST` (ID, NAME, VALUE) VALUES (0, 'zero', 7);
INSERT INTO `TEST` (ID, NAME, VALUE) VALUES (0, 'zero', 100);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[2147483648]);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[2147483648, 100]);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[2147483648, 100, 99]);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[2147483648, 100, 99]);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[2147483648, 100, 99]);

--@test: topk-distinct - topk distinct string
CREATE STREAM TEST (ID BIGINT KEY, NAME varchar, VALUE string) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE TABLE S2 as SELECT ID, topkdistinct(value, 3) as topk FROM test group by id;
INSERT INTO `TEST` (ID, NAME, VALUE) VALUES (0, 'zero', 'a');
INSERT INTO `TEST` (ID, NAME, VALUE) VALUES (0, 'zero', 'b');
INSERT INTO `TEST` (ID, NAME, VALUE) VALUES (0, 'zero', 'c');
INSERT INTO `TEST` (ID, NAME, VALUE) VALUES (0, 'zero', 'b');
INSERT INTO `TEST` (ID, NAME, VALUE) VALUES (0, 'zero', 'd');
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY['a']);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY['b', 'a']);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY['c', 'b', 'a']);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY['c', 'b', 'a']);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY['d', 'c', 'b']);

--@test: topk-distinct - topk distinct decimal
CREATE STREAM TEST (ID BIGINT KEY, NAME varchar, VALUE decimal(2,1)) WITH (kafka_topic='test_topic', value_format='AVRO');
CREATE TABLE S2 as SELECT ID, topkdistinct(value, 3) as topk FROM test group by id;
INSERT INTO `TEST` (ID, NAME, VALUE) VALUES (0, 'zero', 9.8);
INSERT INTO `TEST` (ID, NAME, VALUE) VALUES (0, 'zero', 8.9);
INSERT INTO `TEST` (ID, NAME, VALUE) VALUES (0, 'zero', NULL);
INSERT INTO `TEST` (ID, NAME, VALUE) VALUES (0, 'zero', 7.8);
INSERT INTO `TEST` (ID, NAME, VALUE) VALUES (0, 'zero', 6.5);
INSERT INTO `TEST` (ID, NAME, VALUE) VALUES (0, 'zero', 9.9);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[9.8]);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[9.8, 8.9]);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[9.8, 8.9]);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[9.8, 8.9, 7.8]);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[9.8, 8.9, 7.8]);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[9.9, 9.8, 8.9]);

--@test: topk-distinct - topk distinct bytes
CREATE STREAM TEST (ID BIGINT KEY, NAME varchar, VALUE bytes) WITH (kafka_topic='test_topic',value_format='AVRO');
CREATE TABLE S2 as SELECT ID, topkdistinct(value, 3) as topk FROM test group by id;
INSERT INTO `TEST` (ID, NAME, VALUE) VALUES (0, 'zero', 'Qg==');
INSERT INTO `TEST` (ID, NAME, VALUE) VALUES (0, 'zero', 'Rg==');
INSERT INTO `TEST` (ID, NAME, VALUE) VALUES (0, 'zero', 'Rw==');
INSERT INTO `TEST` (ID, NAME, VALUE) VALUES (0, 'zero', NULL);
INSERT INTO `TEST` (ID, NAME, VALUE) VALUES (0, 'zero', 'QQ==');
INSERT INTO `TEST` (ID, NAME, VALUE) VALUES (0, 'zero', 'Rg==');
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY['Qg==']);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY['Rg==', 'Qg==']);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY['Rw==', 'Rg==', 'Qg==']);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY['Rw==', 'Rg==', 'Qg==']);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY['Rw==', 'Rg==', 'Qg==']);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY['Rw==', 'Rg==', 'Qg==']);

--@test: topk-distinct - topk distinct date
CREATE STREAM TEST (ID BIGINT KEY, NAME varchar, VALUE date) WITH (kafka_topic='test_topic',value_format='AVRO');
CREATE TABLE S2 as SELECT ID, topkdistinct(value, 3) as topk FROM test group by id;
INSERT INTO `TEST` (ID, NAME, VALUE) VALUES (0, 'zero', '1970-01-01');
INSERT INTO `TEST` (ID, NAME, VALUE) VALUES (0, 'zero', '1970-04-11');
INSERT INTO `TEST` (ID, NAME, VALUE) VALUES (0, 'zero', '1970-04-10');
INSERT INTO `TEST` (ID, NAME, VALUE) VALUES (0, 'zero', NULL);
INSERT INTO `TEST` (ID, NAME, VALUE) VALUES (0, 'zero', '1969-12-25');
INSERT INTO `TEST` (ID, NAME, VALUE) VALUES (0, 'zero', '1970-04-11');
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY['1970-01-01']);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY['1970-04-11', '1970-01-01']);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY['1970-04-11', '1970-04-10', '1970-01-01']);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY['1970-04-11', '1970-04-10', '1970-01-01']);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY['1970-04-11', '1970-04-10', '1970-01-01']);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY['1970-04-11', '1970-04-10', '1970-01-01']);

--@test: topk-distinct - topk distinct time
CREATE STREAM TEST (ID BIGINT KEY, NAME varchar, VALUE time) WITH (kafka_topic='test_topic',value_format='AVRO');
CREATE TABLE S2 as SELECT ID, topkdistinct(value, 3) as topk FROM test group by id;
INSERT INTO `TEST` (ID, NAME, VALUE) VALUES (0, 'zero', '00:00');
INSERT INTO `TEST` (ID, NAME, VALUE) VALUES (0, 'zero', '00:00:01');
INSERT INTO `TEST` (ID, NAME, VALUE) VALUES (0, 'zero', '00:00:03');
INSERT INTO `TEST` (ID, NAME, VALUE) VALUES (0, 'zero', NULL);
INSERT INTO `TEST` (ID, NAME, VALUE) VALUES (0, 'zero', '00:00:03');
INSERT INTO `TEST` (ID, NAME, VALUE) VALUES (0, 'zero', '00:00:04');
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY['00:00']);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY['00:00:01', '00:00']);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY['00:00:03', '00:00:01', '00:00']);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY['00:00:03', '00:00:01', '00:00']);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY['00:00:03', '00:00:01', '00:00']);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY['00:00:04', '00:00:03', '00:00:01']);

--@test: topk-distinct - topk distinct timestamp
CREATE STREAM TEST (ID BIGINT KEY, NAME varchar, VALUE timestamp) WITH (kafka_topic='test_topic',value_format='AVRO');
CREATE TABLE S2 as SELECT ID, topkdistinct(value, 3) as topk FROM test group by id;
INSERT INTO `TEST` (ID, NAME, VALUE) VALUES (0, 'zero', '1970-01-01T00:00:00.000');
INSERT INTO `TEST` (ID, NAME, VALUE) VALUES (0, 'zero', '1970-01-01T00:00:00.100');
INSERT INTO `TEST` (ID, NAME, VALUE) VALUES (0, 'zero', '1970-01-01T00:00:00.099');
INSERT INTO `TEST` (ID, NAME, VALUE) VALUES (0, 'zero', NULL);
INSERT INTO `TEST` (ID, NAME, VALUE) VALUES (0, 'zero', '1969-12-31T23:59:59.993');
INSERT INTO `TEST` (ID, NAME, VALUE) VALUES (0, 'zero', '1970-01-01T00:00:00.100');
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY['1970-01-01T00:00:00.000']);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY['1970-01-01T00:00:00.100', '1970-01-01T00:00:00.000']);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY['1970-01-01T00:00:00.100', '1970-01-01T00:00:00.099', '1970-01-01T00:00:00.000']);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY['1970-01-01T00:00:00.100', '1970-01-01T00:00:00.099', '1970-01-01T00:00:00.000']);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY['1970-01-01T00:00:00.100', '1970-01-01T00:00:00.099', '1970-01-01T00:00:00.000']);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY['1970-01-01T00:00:00.100', '1970-01-01T00:00:00.099', '1970-01-01T00:00:00.000']);

