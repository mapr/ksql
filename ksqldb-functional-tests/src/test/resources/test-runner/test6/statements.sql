CREATE STREAM TEST (NAME varchar KEY, ID bigint, VALUE double) WITH (kafka_topic='/s:test_topic', value_format='JSON');
INSERT INTO TEST VALUES ('abc', 101, 13.54);
INSERT INTO TEST VALUES ('foo', 30, 4.5);
INSERT INTO TEST (ID, NAME) VALUES (123, 'bar');
INSERT INTO TEST VALUES ('far', 43245, 43);
INSERT INTO TEST (NAME, ID, ROWTIME) VALUES ('near', 55501, 100000);
CREATE STREAM S1 as SELECT name, value FROM test where id > 100;