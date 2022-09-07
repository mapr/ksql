--@test: array - entries sorted
CREATE STREAM TEST (ID STRING KEY, INTMAP MAP<STRING, INT>, BIGINTMAP MAP<STRING, BIGINT>, DOUBLEMAP MAP<STRING, DOUBLE>, BOOLEANMAP MAP<STRING, BOOLEAN>, STRINGMAP MAP<STRING, STRING>, NULLMAP MAP<STRING, STRING>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, ENTRIES(INTMAP, TRUE), ENTRIES(BIGINTMAP, TRUE), ENTRIES(DOUBLEMAP, TRUE), ENTRIES(BOOLEANMAP, TRUE), ENTRIES(STRINGMAP, TRUE), ENTRIES(NULLMAP, TRUE) FROM TEST;
INSERT INTO `TEST` (ID, INTMAP, BIGINTMAP, DOUBLEMAP, BOOLEANMAP, STRINGMAP, NULLMAP) VALUES ('1', MAP('K1':=1, 'K2':=2, 'K3':=3), MAP('K1':=1, 'K2':=2, 'K3':=3), MAP('K1':=1.0, 'K2':=2.0, 'K3':=3.0), MAP('K1':=true, 'K2':=false, 'K3':=true), MAP('K1':='V1', 'K2':='V2', 'K3':='V3'), NULL);
ASSERT VALUES `OUTPUT` (ID, KSQL_COL_0, KSQL_COL_1, KSQL_COL_2, KSQL_COL_3, KSQL_COL_4, KSQL_COL_5) VALUES ('1', ARRAY[STRUCT(K:='K1', V:=1), STRUCT(K:='K2', V:=2), STRUCT(K:='K3', V:=3)], ARRAY[STRUCT(K:='K1', V:=1), STRUCT(K:='K2', V:=2), STRUCT(K:='K3', V:=3)], ARRAY[STRUCT(K:='K1', V:=1.0), STRUCT(K:='K2', V:=2.0), STRUCT(K:='K3', V:=3.0)], ARRAY[STRUCT(K:='K1', V:=true), STRUCT(K:='K2', V:=false), STRUCT(K:='K3', V:=true)], ARRAY[STRUCT(K:='K1', V:='V1'), STRUCT(K:='K2', V:='V2'), STRUCT(K:='K3', V:='V3')], NULL);

--@test: array - GENERATE_SERIES
CREATE STREAM TEST (ID STRING KEY, F0 INT, F1 INT) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, GENERATE_SERIES(F0, F1) FROM TEST;
INSERT INTO `TEST` (ID, F0, F1) VALUES ('1', 0, 3);
INSERT INTO `TEST` (ID, F0, F1) VALUES ('1', -2, 1);
INSERT INTO `TEST` (ID, F0, F1) VALUES ('1', 4, 3);
INSERT INTO `TEST` (ID, F0, F1) VALUES ('1', 4, 0);
ASSERT VALUES `OUTPUT` (ID, KSQL_COL_0) VALUES ('1', ARRAY[0, 1, 2, 3]);
ASSERT VALUES `OUTPUT` (ID, KSQL_COL_0) VALUES ('1', ARRAY[-2, -1, 0, 1]);
ASSERT VALUES `OUTPUT` (ID, KSQL_COL_0) VALUES ('1', ARRAY[4, 3]);
ASSERT VALUES `OUTPUT` (ID, KSQL_COL_0) VALUES ('1', ARRAY[4, 3, 2, 1, 0]);

--@test: array - GENERATE_SERIES with step
CREATE STREAM TEST (ID STRING KEY, F0 INT, F1 INT, F2 INT) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, GENERATE_SERIES(F0, F1, F2) FROM TEST;
INSERT INTO `TEST` (ID, F0, F1, F2) VALUES ('1', 0, 3, 1);
INSERT INTO `TEST` (ID, F0, F1, F2) VALUES ('1', -2, 1, 2);
INSERT INTO `TEST` (ID, F0, F1, F2) VALUES ('1', 0, 9, 3);
INSERT INTO `TEST` (ID, F0, F1, F2) VALUES ('1', 3, 0, -1);
INSERT INTO `TEST` (ID, F0, F1, F2) VALUES ('1', 1, -2, -2);
INSERT INTO `TEST` (ID, F0, F1, F2) VALUES ('1', 9, 0, -3);
ASSERT VALUES `OUTPUT` (ID, KSQL_COL_0) VALUES ('1', ARRAY[0, 1, 2, 3]);
ASSERT VALUES `OUTPUT` (ID, KSQL_COL_0) VALUES ('1', ARRAY[-2, 0]);
ASSERT VALUES `OUTPUT` (ID, KSQL_COL_0) VALUES ('1', ARRAY[0, 3, 6, 9]);
ASSERT VALUES `OUTPUT` (ID, KSQL_COL_0) VALUES ('1', ARRAY[3, 2, 1, 0]);
ASSERT VALUES `OUTPUT` (ID, KSQL_COL_0) VALUES ('1', ARRAY[1, -1]);
ASSERT VALUES `OUTPUT` (ID, KSQL_COL_0) VALUES ('1', ARRAY[9, 6, 3, 0]);

--@test: array - array_length - primitives
CREATE STREAM INPUT (ID STRING KEY, boolean_array ARRAY<BOOLEAN>, int_array ARRAY<INT>, bigint_array ARRAY<BIGINT>, double_array ARRAY<DOUBLE>, string_array ARRAY<STRING>, decimal_array ARRAY<DECIMAL(2,1)>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, ARRAY_LENGTH(boolean_array) AS boolean_len, ARRAY_LENGTH(int_array) AS int_len, ARRAY_LENGTH(bigint_array) AS bigint_len, ARRAY_LENGTH(double_array) AS double_len, ARRAY_LENGTH(string_array) AS string_len , ARRAY_LENGTH(decimal_array) AS decimal_len FROM INPUT;
INSERT INTO `INPUT` (boolean_array, int_array, bigint_array, double_array, string_array, decimal_array) VALUES (ARRAY[true], ARRAY[-1, 0], ARRAY[-1, 0, 1], ARRAY[0.0, 0.1, 0.2, 0.3], ARRAY['a', 'b', 'c', 'd', 'e'], ARRAY[1.0, 1.1, 1.2, 1.3, 1.4, 1.5]);
INSERT INTO `INPUT` (id) VALUES ('hi');
ASSERT VALUES `OUTPUT` (BOOLEAN_LEN, INT_LEN, BIGINT_LEN, DOUBLE_LEN, STRING_LEN, DECIMAL_LEN) VALUES (1, 2, 3, 4, 5, 6);
ASSERT VALUES `OUTPUT` (BOOLEAN_LEN, INT_LEN, BIGINT_LEN, DOUBLE_LEN, STRING_LEN, DECIMAL_LEN) VALUES (NULL, NULL, NULL, NULL, NULL, NULL);

--@test: array - array_length - structured
CREATE STREAM INPUT (ID STRING KEY, array_array ARRAY<ARRAY<BOOLEAN>>, map_array ARRAY<MAP<STRING,INT>>, struct_array ARRAY<STRUCT<V BIGINT>>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, ARRAY_LENGTH(array_array) AS array_len, ARRAY_LENGTH(map_array) AS map_len, ARRAY_LENGTH(struct_array) AS struct_len FROM INPUT;
INSERT INTO `INPUT` (array_array, map_array, struct_array) VALUES (ARRAY[ARRAY[]], ARRAY[MAP(), MAP()], ARRAY[STRUCT(), STRUCT(), STRUCT()]);
INSERT INTO `INPUT` (id) VALUES ('bye');
ASSERT VALUES `OUTPUT` (ARRAY_LEN, MAP_LEN, STRUCT_LEN) VALUES (1, 2, 3);
ASSERT VALUES `OUTPUT` (ARRAY_LEN, MAP_LEN, STRUCT_LEN) VALUES (NULL, NULL, NULL);

--@test: array - multi-dimensional
CREATE STREAM INPUT (ID STRING KEY, col0 ARRAY<ARRAY<INT>>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, col0[1][2] FROM INPUT;
INSERT INTO `INPUT` (col0) VALUES (ARRAY[ARRAY[0, 1], ARRAY[2]]);
ASSERT VALUES `OUTPUT` (KSQL_COL_0) VALUES (1);

--@test: array - Output array with an error
CREATE STREAM test (K STRING KEY, val VARCHAR) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT K, ARRAY[44, stringtodate(val, 'yyyyMMdd')] AS VALUE FROM test;
INSERT INTO `TEST` (K, val, ROWTIME) VALUES ('1', 'foo', 0);
ASSERT VALUES `OUTPUT` (K, VALUE, ROWTIME) VALUES ('1', ARRAY[44, NULL], 0);

