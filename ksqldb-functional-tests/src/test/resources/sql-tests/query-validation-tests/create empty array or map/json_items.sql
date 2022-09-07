--@test: json_items - convert a json array string to an array of json objects
CREATE STREAM test (K STRING KEY, EVENTS STRING) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT K, JSON_ITEMS(EVENTS) AS EVENTS FROM test;
INSERT INTO `TEST` (K, events) VALUES ('1', '[{"type": "A", "timestamp": "2022-01-27"}, {"type": "B", "timestamp": "2022-05-18"}]');
INSERT INTO `TEST` (K, events) VALUES ('1', '[]');
ASSERT VALUES `OUTPUT` (K, events) VALUES ('1', ARRAY['{"type":"A","timestamp":"2022-01-27"}', '{"type":"B","timestamp":"2022-05-18"}']);
ASSERT VALUES `OUTPUT` (K, events) VALUES ('1', ARRAY[]);

--@test: json_items - extract timestamps from json objects
CREATE STREAM test (K STRING KEY, EVENTS STRING) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT K, TRANSFORM(JSON_ITEMS(EVENTS), (obj) => EXTRACTJSONFIELD(obj, '$.timestamp')) AS EVENTS FROM test;
INSERT INTO `TEST` (K, events) VALUES ('1', '[{"type": "A", "timestamp": "2022-01-27"}, {"type": "B", "timestamp": "2022-05-18"}]');
INSERT INTO `TEST` (K, events) VALUES ('1', '[]');
ASSERT VALUES `OUTPUT` (K, events) VALUES ('1', ARRAY['2022-01-27', '2022-05-18']);
ASSERT VALUES `OUTPUT` (K, events) VALUES ('1', ARRAY[]);

