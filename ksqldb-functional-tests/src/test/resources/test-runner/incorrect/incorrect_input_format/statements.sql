CREATE STREAM TEST (ID bigint KEY, NAME varchar, VALUE double) WITH (kafka_topic='/s:test_topic', value_format='DELIMITED');
CREATE STREAM S1 as SELECT name FROM test where id > 100;