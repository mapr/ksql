/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.rest.integration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;

import com.fasterxml.jackson.core.type.TypeReference;
import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.rest.ApiJsonMapper;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.rest.util.OpencensusMetricsProto;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.PageViewDataProvider;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import io.opencensus.proto.metrics.v1.Metric;
import io.opencensus.proto.resource.v1.Resource;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Serde;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;

@Category({IntegrationTest.class})
public class TelemetryReporterInteractiveModeFunctionalTest {

  protected KafkaConsumer<byte[], byte[]> consumer;
  protected Serde<Metric> serde = new OpencensusMetricsProto();
  
  private static final PageViewDataProvider PAGE_VIEWS_PROVIDER = new PageViewDataProvider();
  private static final String PAGE_VIEW_TOPIC = PAGE_VIEWS_PROVIDER.topicName();
  private static final String TELEMETRY_METRICS_TOPIC = "_confluent-telemetry-metrics";

  private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();

  private static final TestKsqlRestApp REST_APP = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .withProperty(KsqlRestConfig.LISTENERS_CONFIG, "http://localhost:8088")
      .withProperty(KsqlRestConfig.ADVERTISED_LISTENER_CONFIG, "http://localhost:8088")
      .withProperty("confluent.telemetry.exporter.kafka.enabled", true)
      .withProperty("confluent.telemetry.metrics.collector.interval.ms", 1000)
          .withProperty("confluent.telemetry.metrics.collector.whitelist", "")
      .withProperty("confluent.telemetry.debug.enabled", true)
      .build();

  @ClassRule
  public static final RuleChain CHAIN = RuleChain.outerRule(TEST_HARNESS).around(REST_APP);

  private ServiceContext serviceContext;

  @BeforeClass
  public static void setUpClass() {
    TEST_HARNESS.ensureTopics(PAGE_VIEW_TOPIC);
    TEST_HARNESS.produceRows(PAGE_VIEW_TOPIC, PAGE_VIEWS_PROVIDER, FormatFactory.JSON);

    RestIntegrationTestUtil.createStream(REST_APP, PAGE_VIEWS_PROVIDER);
    makeKsqlRequest("CREATE STREAM TEST AS SELECT * FROM PAGEVIEW_KSTREAM;");
  }

  @After
  public void tearDown() {
    if (serviceContext != null) {
      serviceContext.close();
    }
  }

  @AfterClass
  public static void classTearDown() {
    REST_APP.getPersistentQueries().forEach(str -> makeKsqlRequest("TERMINATE " + str + ";"));
    REST_APP.stop();
  }

  @Test
  public void shouldReadTelemetryMetrics() throws InterruptedException {
    // When:
    final KafkaConsumer<byte[], byte[]> consumer = createNewConsumer(TEST_HARNESS.kafkaBootstrapServers());
    consumer.subscribe(Collections.singleton("_confluent-telemetry-metrics"));

    TEST_HARNESS.getKafkaCluster().waitForTopicsToBePresent(TELEMETRY_METRICS_TOPIC);
    TEST_HARNESS.getTopics();

    final long startMs = System.currentTimeMillis();
    boolean clientMetricsPresent = false;
    boolean ksqlMetricsPresent = false;

    while (System.currentTimeMillis() - startMs < 20000 && (!clientMetricsPresent || !ksqlMetricsPresent)) {
      final ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(200));
      for (ConsumerRecord<byte[], byte[]> record : records) {

        // Verify that the message de-serializes successfully
        Metric m = null;
        try {
          m = this.serde.deserializer()
              .deserialize(record.topic(), record.headers(), record.value());
        } catch (SerializationException e) {
          throw new SerializationException("Failed to deserialize message: " + e.getMessage());
        }

        // Verify labels

        // Check the resource labels are present
        final Resource resource = m.getResource();

        final Map<String, String> resourceLabels = resource.getLabelsMap();

        // Check that the labels from the config are present.

        if (m.getMetricDescriptor().getName().startsWith("io.confluent.ksql")) {
          System.out.println(m);
          System.out.println(resource);
          ksqlMetricsPresent = true;
        }
        if (m.getMetricDescriptor().getName().startsWith("io.confluent.kafka.client")) {
          System.out.println("clients");
          clientMetricsPresent = true;
        }
      }
    }
    Assert.assertTrue(clientMetricsPresent);
    Assert.assertTrue(ksqlMetricsPresent);
  }

  private static void makeKsqlRequest(final String sql) {
    RestIntegrationTestUtil.makeKsqlRequest(REST_APP, sql);
  }

  private static KafkaConsumer<byte[], byte[]> createNewConsumer(String brokerList) {
    Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, "telemetry-metric-reporter-consumer");
    // The metric topic may not be there initially. So, we need to refresh metadata more frequently to pick it up once created.
    properties.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "400");
    return new KafkaConsumer<>(properties, new ByteArrayDeserializer(),
        new ByteArrayDeserializer());
  }
}
