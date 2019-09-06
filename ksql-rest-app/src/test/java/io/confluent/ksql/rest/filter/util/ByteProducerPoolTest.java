package io.confluent.ksql.rest.filter.util;

import io.confluent.ksql.rest.filter.UserGroupInformationMockPolicy;
import io.confluent.rest.exceptions.RestServerErrorException;
import io.confluent.rest.impersonation.Errors;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.easymock.TestSubject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.MockPolicy;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.Function;

import static org.apache.hadoop.security.UserGroupInformation.createRemoteUser;
import static org.apache.hadoop.security.UserGroupInformation.getCurrentUser;
import static org.easymock.EasyMock.*;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

@RunWith(PowerMockRunner.class)
@MockPolicy(UserGroupInformationMockPolicy.class)
public class ByteProducerPoolTest extends EasyMockSupport {
  private static final byte[] ANY_BYTES = "text".getBytes();
  private static final ProducerRecord<byte[], byte[]> ANY_RECORD = new ProducerRecord<>("topic", ANY_BYTES);
  private Properties producerConfig = new Properties();

  @TestSubject
  private ByteProducerPool producerPool = new ByteProducerPool(producerConfig);

  @Mock
  private Function<Properties, KafkaProducer<byte[], byte[]>> producerFactory;

  @Test
  public void sendsRecordByCreatedProducer() throws IOException {
    final CompletableFuture<RecordMetadata> expectedFuture = new CompletableFuture<>();

    KafkaProducer<byte[], byte[]> producer = mockProducerFor(getCurrentUser());
    expect(producer.send(ANY_RECORD)).andReturn(expectedFuture);
    replayAll();

    Future<RecordMetadata> result = producerPool.send(ANY_RECORD);

    assertSame(expectedFuture, result);
    verifyAll();
  }

  @Test
  public void cachesProducerByUser() {
    final CompletableFuture<RecordMetadata> expectedForU1C1 = new CompletableFuture<>();
    final CompletableFuture<RecordMetadata> expectedForU1C2 = new CompletableFuture<>();
    final CompletableFuture<RecordMetadata> expectedForU2C1 = new CompletableFuture<>();

    final UserGroupInformation u1 = createRemoteUser("U1");
    final UserGroupInformation u2 = createRemoteUser("U2");
    KafkaProducer<byte[], byte[]> producerForU1 = mockProducerFor(u1);
    KafkaProducer<byte[], byte[]> producerForU2 = mockProducerFor(u2);

    expect(producerForU1.send(ANY_RECORD)).andReturn(expectedForU1C1).andReturn(expectedForU1C2);
    expect(producerForU2.send(ANY_RECORD)).andReturn(expectedForU2C1);
    replayAll();

    final PrivilegedAction<Future<RecordMetadata>> sendAnyRecord = () -> producerPool.send(ANY_RECORD);
    assertThat(u1.doAs(sendAnyRecord), is(expectedForU1C1));
    assertThat(u2.doAs(sendAnyRecord), is(expectedForU2C1));
    assertThat(u1.doAs(sendAnyRecord), is(expectedForU1C2));

    verifyAll();
  }

  @SuppressWarnings("unchecked")
  private KafkaProducer<byte[], byte[]> mockProducerFor(UserGroupInformation user) {
    KafkaProducer<byte[], byte[]> producer = mock(KafkaProducer.class);
    expect(producerFactory.apply(producerConfig))
        .andAnswer(() -> {
          assertThat("Should be created by another user", getCurrentUser(), is(user));
          return producer;
        })
        .once();
    return producer;
  }

  @Test
  public void throwsServerLoginExceptionOnFailure() throws IOException {
    RuntimeException cause = new RuntimeException("Somehow cannot create producer");
    final KafkaProducer<byte[], byte[]> producer = mockProducerFor(getCurrentUser());
    expect(producer.send(anyObject())).andThrow(cause);
    replayAll();

    try {
      producerPool.send(ANY_RECORD);
      fail();
    } catch (RestServerErrorException e) {
      final RestServerErrorException expected = Errors.serverLoginException(cause);
      assertThat(e.getCause(), is(expected.getCause()));
      assertThat(e.getErrorCode(), is(expected.getErrorCode()));
      assertThat(e.getMessage(), is(expected.getMessage()));
    }
    verifyAll();
  }

  @Test
  public void closesCachedProducer() throws IOException {
    final CompletableFuture<RecordMetadata> expectedFuture = new CompletableFuture<>();

    KafkaProducer<byte[], byte[]> producer = mockProducerFor(getCurrentUser());
    expect(producer.send(ANY_RECORD)).andReturn(expectedFuture);
    producer.close();
    expectLastCall().once();
    replayAll();

    producerPool.send(ANY_RECORD);
    producerPool.close();

    verifyAll();
  }
}