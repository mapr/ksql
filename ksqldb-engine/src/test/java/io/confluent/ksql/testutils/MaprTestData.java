package io.confluent.ksql.testutils;

import com.google.common.collect.ImmutableMap;
import io.confluent.common.utils.TestUtils;
import io.confluent.ksql.util.KsqlConfig;
import org.apache.kafka.streams.StreamsConfig;

import java.util.HashMap;
import java.util.Map;

public class MaprTestData {
  public static final String MAPR_STREAM = "/sample-stream";

  public static Map<String, Object> compatibleKsqlConfig() {
    return ImmutableMap.of(
        StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath(),
        KsqlConfig.KSQL_DEFAULT_STREAM_CONFIG, MAPR_STREAM,
            StreamsConfig.APPLICATION_SERVER_CONFIG, "http://somehost:1234"
    );
  }

  public static Map<String, Object> compatibleKsqlConfig(Map<String, Object> overrides) {
    Map<String, Object> map = new HashMap<>(compatibleKsqlConfig());
    map.putAll(overrides);
    return map;
  }
}
