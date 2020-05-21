/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.util;

import java.util.Map;
import org.apache.kafka.clients.CommonClientConfigs;

public final class TelemetryReporterUtil {

  private TelemetryReporterUtil() {
    
  }

  public static final String METRICS_CONTEXT_RESOURCE_CLUSTER_ID =
      CommonClientConfigs.METRICS_CONTEXT_PREFIX + "resource.cluster.id";

  public static KsqlConfig addTelemetryReporterConfigs(
      final KsqlConfig ksqlConfig
  ) {
    return new KsqlConfig(addTelemetryReporterConfigs(
        ksqlConfig.originals(),
        ksqlConfig.getString(KsqlConfig.KSQL_SERVICE_ID_CONFIG)));
  }
  
  public static Map<String, Object> addTelemetryReporterConfigs(
      final Map<String,Object> props,
      final String ksqlServiceId) {
    props.put("metric.reporters", "io.confluent.telemetry.reporter.TelemetryReporter");
    props.put("ksql.metric.reporters", "io.confluent.telemetry.reporter.TelemetryReporter");
    props.put("confluent.telemetry.exporter.kafka.producer.bootstrap.servers",
            props.get("bootstrap.servers"));
    props.put("confluent.telemetry.exporter.kafka.topic.replicas", "1");
    props.put(METRICS_CONTEXT_RESOURCE_CLUSTER_ID, ksqlServiceId);
    return props;
  }
}
