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

package io.confluent.ksql.logging.processing;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyShort;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.engine.KsqlEngineTestUtil;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.metrics.MetricCollectors;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.services.TestServiceContext;
import io.confluent.ksql.testutils.AvoidMaprFSAppDirCreation;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Optional;

import io.confluent.ksql.util.MaprFSUtils;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.MockPolicy;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@MockPolicy(AvoidMaprFSAppDirCreation.class)
@PowerMockIgnore({"javax.management.*", "com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*", "org.w3c.*", "javax.net.ssl.*"})
public class ProcessingLogServerUtilsTest {
  static {
    PowerMock.mockStaticPartial(MaprFSUtils.class,
            "createAppDirAndInternalStreamsIfNotExist");
  }

  private static final String STREAM = "PROCESSING_LOG_STREAM";
  private static final String TOPIC = "processing_log_topic";
  private static final String CLUSTER_ID = "ksql_cluster.";
  private static final int PARTITIONS = 10;
  private static final short REPLICAS = 3;
  private static final String DEFAULT_TOPIC = "/apps/ksql/ksql_cluster./KSQL_PROCESSING_LOG:" +
      CLUSTER_ID + ProcessingLogConfig.TOPIC_NAME_DEFAULT_SUFFIX;


  private static final KsqlConfig ksqlConfig = new KsqlConfig(
          ImmutableMap.of(KsqlConfig.KSQL_SERVICE_ID_CONFIG, CLUSTER_ID,
                  KsqlConfig.KSQL_DEFAULT_STREAM_CONFIG, "/sample-stream")
  );

  private static final ServiceContext serviceContext = TestServiceContext.create(ksqlConfig);
  private final KafkaTopicClient spyTopicClient = spy(serviceContext.getTopicClient());
  private static final MutableMetaStore metaStore = new MetaStoreImpl(new InternalFunctionRegistry());
  private static KsqlEngine ksqlEngine;
  private final ProcessingLogConfig config = new ProcessingLogConfig(
      ImmutableMap.of(
          ProcessingLogConfig.TOPIC_AUTO_CREATE,
          true,
          ProcessingLogConfig.TOPIC_NAME,
          TOPIC,
          ProcessingLogConfig.TOPIC_PARTITIONS,
          PARTITIONS,
          ProcessingLogConfig.TOPIC_REPLICATION_FACTOR,
          REPLICAS,
          ProcessingLogConfig.STREAM_NAME,
          STREAM
      )
  );

  private KafkaTopicClient mockTopicClient = mock(KafkaTopicClient.class);

  @BeforeClass
  public static void setUp() {
    ksqlEngine = KsqlEngineTestUtil.createKsqlEngine(
            serviceContext,
            metaStore,
            ksqlConfig
    );
  }

  @After
  public void teardown() {
    ksqlEngine.close();
  }

  @Test
  public void shouldBuildCorrectStreamCreateDDL() {
    // Given:
    serviceContext.getTopicClient().createTopic(TOPIC, 1, (short) 1);

    // When:
    final String statement =
        ProcessingLogServerUtils.processingLogStreamCreateStatement(
            config,
            ksqlConfig);

    // Then:
    assertThat(statement, equalTo(
        "CREATE STREAM PROCESSING_LOG_STREAM ("
            + "logger VARCHAR, "
            + "level VARCHAR, "
            + "time BIGINT, "
            + "message STRUCT<"
            + "type INT, "
            + "deserializationError STRUCT<target VARCHAR, errorMessage VARCHAR, recordB64 VARCHAR, cause ARRAY<VARCHAR>, `topic` VARCHAR>, "
            + "recordProcessingError STRUCT<errorMessage VARCHAR, record VARCHAR, cause ARRAY<VARCHAR>>, "
            + "productionError STRUCT<errorMessage VARCHAR>, "
            + "serializationError STRUCT<target VARCHAR, errorMessage VARCHAR, record VARCHAR, cause ARRAY<VARCHAR>, `topic` VARCHAR>, "
            + "kafkaStreamsThreadError STRUCT<errorMessage VARCHAR, threadName VARCHAR, cause ARRAY<VARCHAR>>"
            + ">"
            + ") WITH(KAFKA_TOPIC='processing_log_topic', VALUE_FORMAT='JSON', KEY_FORMAT='KAFKA');"));
  }

  @Test
  public void shouldBuildCorrectStreamCreateDDLWithDefaultTopicName() {
    // Given:
    serviceContext.getTopicClient().createTopic(DEFAULT_TOPIC, 1, (short) 1);

    // When:
    final String statement =
        ProcessingLogServerUtils.processingLogStreamCreateStatement(
            new ProcessingLogConfig(
                ImmutableMap.of(
                    ProcessingLogConfig.STREAM_AUTO_CREATE, true,
                    ProcessingLogConfig.STREAM_NAME, STREAM
                )
            ),
            ksqlConfig);

    // Then:
    assertThat(statement,
        containsString("KAFKA_TOPIC='/apps/ksql/ksql_cluster./PROCESSING_LOG_STREAM:ksql_cluster.ksql_processing_log'"));
  }

  @Test
  public void shouldNotCreateLogTopicIfNotConfigured() {
    // Given:
    final ProcessingLogConfig config = new ProcessingLogConfig(
        ImmutableMap.of(ProcessingLogConfig.TOPIC_AUTO_CREATE, false)
    );

    // When:
    final Optional<String> createdTopic = ProcessingLogServerUtils.maybeCreateProcessingLogTopic(
        spyTopicClient,
        config,
        ksqlConfig);

    // Then:
    assertThat(createdTopic.isPresent(), is(false));
    verifyNoMoreInteractions(spyTopicClient);
  }

  @Test
  public void shouldThrowOnUnexpectedKafkaClientError() {
    // Given:
    doThrow(new RuntimeException("bad"))
        .when(mockTopicClient)
        .createTopic(anyString(), anyInt(), anyShort());

    // When:
    final Exception e = assertThrows(
        RuntimeException.class,
        () -> ProcessingLogServerUtils.maybeCreateProcessingLogTopic(
            mockTopicClient, config, ksqlConfig)
    );

    // Then:
    assertThat(e.getMessage(), containsString("bad"));
  }

  @Test
  public void shouldCreateProcessingLogTopic() {
    // When:
    final Optional<String> createdTopic = ProcessingLogServerUtils.maybeCreateProcessingLogTopic(
        mockTopicClient,
        config,
        ksqlConfig);

    // Then:
    assertThat(createdTopic.isPresent(), is(true));
    assertThat(createdTopic.get(), equalTo(TOPIC));
    verify(mockTopicClient).createTopic(TOPIC, PARTITIONS, REPLICAS);
  }

  @Test
  public void shouldCreateProcessingLogTopicWithCorrectDefaultName() {
    // Given:
    final ProcessingLogConfig config = new ProcessingLogConfig(
        ImmutableMap.of(
            ProcessingLogConfig.TOPIC_AUTO_CREATE,
            true,
            ProcessingLogConfig.TOPIC_PARTITIONS,
            PARTITIONS,
            ProcessingLogConfig.TOPIC_REPLICATION_FACTOR,
            REPLICAS
        )
    );

    // When:
    final Optional<String> createdTopic = ProcessingLogServerUtils.maybeCreateProcessingLogTopic(
        mockTopicClient,
        config,
        ksqlConfig);

    // Then:
    assertThat(createdTopic.isPresent(), is(true));
    assertThat(createdTopic.get(), equalTo(DEFAULT_TOPIC));
    verify(mockTopicClient).createTopic(DEFAULT_TOPIC, PARTITIONS, REPLICAS);
  }
}
