/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.ksql.security.filter.util;

import io.confluent.ksql.test.util.UserGroupInformationMockPolicy;
import io.confluent.rest.exceptions.RestServerErrorException;
import io.confluent.rest.impersonation.Errors;
import org.apache.hadoop.security.IdMappingServiceProvider;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.easymock.EasyMockSupport;
import org.hamcrest.MatcherAssert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.MockPolicy;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import static org.apache.hadoop.security.UserGroupInformation.createRemoteUser;
import static org.apache.hadoop.security.UserGroupInformation.getCurrentUser;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

@RunWith(PowerMockRunner.class)
@MockPolicy(UserGroupInformationMockPolicy.class)
public class ByteProducerPoolTest extends EasyMockSupport {
  private static final byte[] ANY_BYTES = "text".getBytes(StandardCharsets.UTF_8);
  private static final ProducerRecord<byte[], byte[]> ANY_RECORD = new ProducerRecord<>("topic", ANY_BYTES);
  private Map<String, Object> producerConfig = new HashMap<>();

  private KafkaClientSupplier clientSupplier;

  private ByteProducerPool producerPool;

  private IdMappingServiceProvider idMapper;

  @Before
  public void setUp() {
    this.clientSupplier = mock(KafkaClientSupplier.class);
    this.idMapper = mock(IdMappingServiceProvider.class);
    this.producerPool = new ByteProducerPool(producerConfig, clientSupplier, idMapper);
  }

  @Test
  public void sendsRecordByCreatedProducer() throws IOException {
    final CompletableFuture<RecordMetadata> expectedFuture = new CompletableFuture<>();

    UserGroupInformation currentUser = getCurrentUser();
    expect(idMapper.getUid(currentUser.getUserName())).andReturn(1000);
    KafkaProducer<byte[], byte[]> producer = mockProducerFor(currentUser);
    expect(producer.send(ANY_RECORD)).andReturn(expectedFuture);
    replayAll();

    Future<RecordMetadata> result = producerPool.send(ANY_RECORD);

    assertSame(expectedFuture, result);
    verifyAll();
  }

  @Test
  public void cachesProducerByUser() throws IOException {
    final CompletableFuture<RecordMetadata> expectedForU1C1 = new CompletableFuture<>();
    final CompletableFuture<RecordMetadata> expectedForU1C2 = new CompletableFuture<>();
    final CompletableFuture<RecordMetadata> expectedForU2C1 = new CompletableFuture<>();

    final UserGroupInformation u1 = createRemoteUser("U1");
    expect(idMapper.getUid(u1.getUserName())).andReturn(1001).times(2);
    final UserGroupInformation u2 = createRemoteUser("U2");
    expect(idMapper.getUid(u2.getUserName())).andReturn(1002);
    KafkaProducer<byte[], byte[]> producerForU1 = mockProducerFor(u1);
    KafkaProducer<byte[], byte[]> producerForU2 = mockProducerFor(u2);

    expect(producerForU1.send(ANY_RECORD)).andReturn(expectedForU1C1).andReturn(expectedForU1C2);
    expect(producerForU2.send(ANY_RECORD)).andReturn(expectedForU2C1);
    replayAll();

    final PrivilegedAction<Future<RecordMetadata>> sendAnyRecord = () -> producerPool.send(ANY_RECORD);
    MatcherAssert.assertThat(u1.doAs(sendAnyRecord), is(expectedForU1C1));
    MatcherAssert.assertThat(u2.doAs(sendAnyRecord), is(expectedForU2C1));
    MatcherAssert.assertThat(u1.doAs(sendAnyRecord), is(expectedForU1C2));

    verifyAll();
  }

  @SuppressWarnings("unchecked")
  private KafkaProducer<byte[], byte[]> mockProducerFor(UserGroupInformation user) {
    KafkaProducer<byte[], byte[]> producer = mock(KafkaProducer.class);
    expect(clientSupplier.getProducer(producerConfig))
        .andAnswer(() -> {
          MatcherAssert.assertThat("Should be created by another user", getCurrentUser(), is(user));
          return producer;
        })
        .once();
    return producer;
  }

  @Test
  public void throwsServerLoginExceptionOnFailure() throws IOException {
    RuntimeException cause = new RuntimeException("Somehow cannot create producer");
    UserGroupInformation currentUser = getCurrentUser();
    expect(idMapper.getUid(currentUser.getUserName())).andReturn(1000);
    final KafkaProducer<byte[], byte[]> producer = mockProducerFor(currentUser);
    expect(producer.send(anyObject())).andThrow(cause);
    replayAll();

    try {
      producerPool.send(ANY_RECORD);
      fail();
    } catch (RestServerErrorException e) {
      final RestServerErrorException expected = Errors.serverLoginException(cause);
      MatcherAssert.assertThat(e.getCause(), is(expected.getCause()));
      MatcherAssert.assertThat(e.getErrorCode(), is(expected.getErrorCode()));
      MatcherAssert.assertThat(e.getMessage(), is(expected.getMessage()));
    }
    verifyAll();
  }

  @Test
  public void closesCachedProducer() throws IOException {
    final CompletableFuture<RecordMetadata> expectedFuture = new CompletableFuture<>();

    UserGroupInformation currentUser = getCurrentUser();
    expect(idMapper.getUid(currentUser.getUserName())).andReturn(1000);
    KafkaProducer<byte[], byte[]> producer = mockProducerFor(currentUser);
    expect(producer.send(ANY_RECORD)).andReturn(expectedFuture);
    producer.close();
    expectLastCall().once();
    replayAll();

    producerPool.send(ANY_RECORD);
    producerPool.close();

    verifyAll();
  }
}