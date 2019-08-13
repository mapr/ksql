/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.ddl.commands.CommandFactories;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.testutils.AvoidMaprFSAppDirCreation;
import io.confluent.ksql.testutils.MaprTestData;
import io.confluent.ksql.util.FakeKafkaClientSupplier;
import io.confluent.ksql.util.FakeKafkaTopicClient;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.MetaStoreFixture;
import java.util.List;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.MockPolicy;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@MockPolicy(AvoidMaprFSAppDirCreation.class)
public class QueryEngineTest {

  private KafkaTopicClient topicClient;
  private SchemaRegistryClient schemaRegistryClient;
  private MetaStore metaStore;
  private KsqlConfig ksqlConfig;
  private KsqlEngine ksqlEngine;

  @Before
  public void setUp() {
    KafkaClientSupplier kafkaClientSupplier = new FakeKafkaClientSupplier();
    ksqlConfig = new KsqlConfig(MaprTestData.compatibleKsqlConfig());
    metaStore = MetaStoreFixture.getNewMetaStore(new InternalFunctionRegistry());
    schemaRegistryClient = new MockSchemaRegistryClient();
    topicClient = new FakeKafkaTopicClient(ksqlConfig);
    ksqlEngine = KsqlEngineTestUtil.createKsqlEngine(
        topicClient,
        () -> schemaRegistryClient,
        kafkaClientSupplier,
        metaStore,
        ksqlConfig,
        kafkaClientSupplier.getAdminClient(ksqlConfig.getKsqlAdminClientConfigProps()));
  }

  @After
  public void closeEngine() {
    ksqlEngine.close();
  }

  @Test
  public void shouldThrowExpectedExceptionForDuplicateTable() {
    final QueryEngine queryEngine = new QueryEngine(ksqlEngine,
        new CommandFactories(topicClient, schemaRegistryClient));
    try {
      final List<PreparedStatement> statementList = ksqlEngine.parseStatements(
          "CREATE TABLE FOO AS SELECT * FROM TEST2; CREATE TABLE FOO WITH (KAFKA_TOPIC='BAR') AS SELECT * FROM TEST2;", metaStore.clone(), true);
      queryEngine.buildLogicalPlans(metaStore, statementList, ksqlConfig);
      Assert.fail();
    } catch (final KsqlException e) {
      assertThat(e.getMessage(), equalTo("Exception while processing statement: Cannot add the new data source. Another data source with the same name already exists: KsqlStream name:FOO"));
    }

  }

  @Test
  public void shouldThrowExpectedExceptionForDuplicateStream() {
    final QueryEngine queryEngine = new QueryEngine(ksqlEngine,
        new CommandFactories(topicClient, schemaRegistryClient));
    try {
      final List<PreparedStatement> statementList = ksqlEngine.parseStatements(
          "CREATE STREAM FOO AS SELECT * FROM ORDERS; CREATE STREAM FOO WITH (KAFKA_TOPIC='BAR') AS SELECT * FROM ORDERS;", metaStore.clone(), true);
      queryEngine.buildLogicalPlans(metaStore, statementList, ksqlConfig);
      Assert.fail();
    } catch (final KsqlException e) {
      assertThat(e.getMessage(), equalTo("Exception while processing statement: Cannot add the new data source. Another data source with the same name already exists: KsqlStream name:FOO"));
    }

  }
}
