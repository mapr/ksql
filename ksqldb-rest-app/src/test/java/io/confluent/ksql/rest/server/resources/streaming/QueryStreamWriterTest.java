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

package io.confluent.ksql.rest.server.resources.streaming;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.newCapture;
import static org.easymock.EasyMock.niceMock;
import static org.easymock.EasyMock.replay;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.MatcherAssert.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.query.BlockingRowQueue;
import io.confluent.ksql.query.LimitHandler;
import io.confluent.ksql.rest.ApiJsonMapper;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.TransientQueryMetadata;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.easymock.Capture;
import org.easymock.EasyMockRunner;
import org.easymock.IAnswer;
import org.easymock.Mock;
import org.easymock.MockType;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;

@SuppressWarnings("unchecked")
@RunWith(EasyMockRunner.class)
public class QueryStreamWriterTest {

  @Rule
  public final Timeout timeout = Timeout.builder()
      .withTimeout(30, TimeUnit.SECONDS)
      .withLookingForStuckThread(true)
      .build();

  @Mock(MockType.NICE)
  private TransientQueryMetadata queryMetadata;
  @Mock(MockType.NICE)
  private BlockingRowQueue rowQueue;
  private Capture<Thread.UncaughtExceptionHandler> ehCapture;
  private Capture<Collection<GenericRow>> drainCapture;
  private Capture<LimitHandler> limitHandlerCapture;
  private QueryStreamWriter writer;
  private ByteArrayOutputStream out;
  private LimitHandler limitHandler;
  private ObjectMapper objectMapper;

  @Before
  public void setUp() {

    objectMapper = ApiJsonMapper.INSTANCE.get();

    ehCapture = newCapture();
    drainCapture = newCapture();
    limitHandlerCapture = newCapture();

    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(SystemColumns.ROWKEY_NAME, SqlTypes.STRING)
        .valueColumn(ColumnName.of("col1"), SqlTypes.STRING)
        .build();

    final KafkaStreams kStreams = niceMock(KafkaStreams.class);

    kStreams.setStateListener(anyObject());
    expectLastCall();
    expect(kStreams.state()).andReturn(State.RUNNING);

    expect(queryMetadata.getRowQueue()).andReturn(rowQueue).anyTimes();
    expect(queryMetadata.getLogicalSchema()).andReturn(schema).anyTimes();

    queryMetadata.setLimitHandler(capture(limitHandlerCapture));
    expectLastCall().once();

    queryMetadata.setUncaughtExceptionHandler(capture(ehCapture));
    expectLastCall();

    replay(kStreams);
  }

  @Test
  public void shouldWriteAnyPendingRowsBeforeReportingException() {
    // Given:
    expect(queryMetadata.isRunning()).andReturn(true).anyTimes();
    rowQueue.drainTo(capture(drainCapture));
    expectLastCall().andAnswer(rows("Row1", "Row2", "Row3"));

    createWriter();

    givenUncaughtException(new KsqlException("Server went Boom"));

    // When:
    writer.write(out);

    // Then:
    final List<String> lines = getOutput(out);
    assertThat(lines, hasItems(
        containsString("Row1"),
        containsString("Row2"),
        containsString("Row3")));
  }

  @Test
  public void shouldExitAndDrainIfQueryStopsRunning() {
    // Given:
    expect(queryMetadata.isRunning()).andReturn(true).andReturn(false);
    rowQueue.drainTo(capture(drainCapture));
    expectLastCall().andAnswer(rows("Row1", "Row2", "Row3"));

    createWriter();

    // When:
    writer.write(out);

    // Then:
    final List<String> lines = getOutput(out);
    assertThat(lines, hasItems(
        containsString("Row1"),
        containsString("Row2"),
        containsString("Row3")));
  }

  @Test
  public void shouldExitAndDrainIfLimitReached() {
    // Given:
    expect(queryMetadata.isRunning()).andReturn(true).anyTimes();
    rowQueue.drainTo(capture(drainCapture));
    expectLastCall().andAnswer(rows("Row1", "Row2", "Row3"));

    createWriter();

    limitHandler.limitReached();

    // When:
    writer.write(out);

    // Then:
    final List<String> lines = getOutput(out);
    assertThat(lines, hasItems(
        containsString("Row1"),
        containsString("Row2"),
        containsString("Row3")));
  }

  private void createWriter() {
    replay(queryMetadata, rowQueue);

    writer = new QueryStreamWriter(queryMetadata, 1000, objectMapper, new CompletableFuture<>());

    out = new ByteArrayOutputStream();
    limitHandler = limitHandlerCapture.getValue();
  }

  private void givenUncaughtException(final KsqlException e) {
    ehCapture.getValue().uncaughtException(new Thread(), e);
  }

  private IAnswer<Integer> rows(final Object... rows) {
    return () -> {
      final Collection<GenericRow> output = drainCapture.getValue();

      Arrays.stream(rows)
          .map(ImmutableList::of)
          .map(QueryStreamWriterTest::genericRow)
          .forEach(output::add);

      return rows.length;
    };
  }

  private static GenericRow genericRow(final List<Object> values) {
    return new GenericRow().appendAll(values);
  }

  private static List<String> getOutput(final ByteArrayOutputStream out) {
    final String[] lines = new String(out.toByteArray(), StandardCharsets.UTF_8).split("\n");
    return Arrays.stream(lines)
        .filter(line -> !line.isEmpty())
        .collect(Collectors.toList());
  }
}
