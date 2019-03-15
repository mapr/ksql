/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.rest.server.execution;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.rest.entity.QueryDescription;
import io.confluent.ksql.rest.entity.QueryDescriptionEntity;
import io.confluent.ksql.rest.server.TemporaryEngine;
import io.confluent.ksql.serde.DataSource.DataSourceType;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import java.util.Optional;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ExplainExecutorTest {

  @Rule public final TemporaryEngine engine = new TemporaryEngine();
  @Rule public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void shouldExplainQueryId() {
    // Given:
    final PreparedStatement<?> explain = engine.prepare("EXPLAIN id;");
    final PersistentQueryMetadata metadata = engine.givenPersistentQuery("id");

    KsqlEngine engine = mock(KsqlEngine.class);
    when(engine.getPersistentQuery(metadata.getQueryId())).thenReturn(Optional.of(metadata));

    // When:
    final QueryDescriptionEntity query = (QueryDescriptionEntity) CustomExecutors.EXPLAIN.execute(
        explain,
        engine,
        this.engine.getServiceContext(),
        this.engine.getKsqlConfig(),
        ImmutableMap.of()
    ).orElseThrow(IllegalStateException::new);

    // Then:
    assertThat(query.getQueryDescription(), equalTo(QueryDescription.forQueryMetadata(metadata)));
  }


  @Test
  public void shouldExplainPersistentStatement() {
    // Given:
    engine.givenSource(DataSourceType.KSTREAM, "Y");
    final String statementText = "CREATE STREAM X AS SELECT * FROM Y;";
    final PreparedStatement<?> explain = engine.prepare("EXPLAIN " + statementText);

    // When:
    final QueryDescriptionEntity query = (QueryDescriptionEntity) CustomExecutors.EXPLAIN.execute(
        explain,
        engine.getEngine(),
        engine.getServiceContext(),
        engine.getKsqlConfig(),
        ImmutableMap.of()
    ).orElseThrow(IllegalStateException::new);

    // Then:
    assertThat(query.getQueryDescription().getStatementText(), equalTo(statementText));
    assertThat(query.getQueryDescription().getSources(), containsInAnyOrder("Y"));
    assertThat("No side effects should happen", engine.getEngine().getPersistentQueries(), is(empty()));
  }

  @Test
  public void shouldExplainStatement() {
    // Given:
    engine.givenSource(DataSourceType.KSTREAM, "Y");
    final String statementText = "SELECT * FROM Y;";
    final PreparedStatement<?> explain = engine.prepare("EXPLAIN " + statementText);

    // When:
    final QueryDescriptionEntity query = (QueryDescriptionEntity) CustomExecutors.EXPLAIN.execute(
        explain,
        engine.getEngine(),
        engine.getServiceContext(),
        engine.getKsqlConfig(),
        ImmutableMap.of()
    ).orElseThrow(IllegalStateException::new);

    // Then:
    assertThat(query.getQueryDescription().getStatementText(), equalTo(statementText));
    assertThat(query.getQueryDescription().getSources(), containsInAnyOrder("Y"));
  }

  @Test
  public void shouldFailOnNonQueryExplain() {
    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("The provided statement does not run a ksql query");

    // When:
    CustomExecutors.EXPLAIN.execute(
        engine.prepare("Explain SHOW TOPICS;"),
        engine.getEngine(),
        engine.getServiceContext(),
        engine.getKsqlConfig(),
        ImmutableMap.of()
    );
  }


}
