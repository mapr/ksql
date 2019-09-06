package io.confluent.ksql.rest.filter.util;

import io.confluent.ksql.rest.filter.UserGroupInformationMockPolicy;
import io.confluent.rest.exceptions.RestServerErrorException;
import io.confluent.rest.impersonation.Errors;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.easymock.TestSubject;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.MockPolicy;
import org.powermock.modules.junit4.PowerMockRunner;

import java.security.PrivilegedAction;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

import static org.easymock.EasyMock.*;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

@RunWith(PowerMockRunner.class)
@MockPolicy(UserGroupInformationMockPolicy.class)
public class ByteConsumerPoolTest extends EasyMockSupport {
  private static final String ANY_TOPIC = "/some-stream:topic";
  private static final String ANY_USER = System.getProperty("user.name");

  private Properties consumerConfig = new Properties();
  @TestSubject
  private ByteConsumerPool consumerPool = new ByteConsumerPool(consumerConfig, ANY_TOPIC);

  @Mock
  private Function<Properties, KafkaConsumer<byte[], byte[]>> consumerFactory;

  @Before
  public void setUp() {
    consumerConfig.put("any.config", "has-to-be-copied");
  }

  @Test
  public void consumesRecordsFromCreatedConsumer() {
    KafkaConsumer<byte[], byte[]> consumer = mockConsumerForCurrentThreadAndUser(ANY_USER);
    expectConsumerAssignTopic(consumer, ANY_TOPIC);

    ConsumerRecords<byte[], byte[]> expectedRecords = new ConsumerRecords<>(Collections.emptyMap());
    expect(consumer.poll(anyObject(Duration.class))).andReturn(expectedRecords);
    replayAll();

    ByteConsumerPool consumerPool = this.consumerPool;

    assertThat(consumerPool.poll(), sameInstance(expectedRecords));
    verifyAll();
  }

  @Test
  public void cachesConsumerByUser() {
    ConsumerRecords<byte[], byte[]> expectedU1C1 = new ConsumerRecords<>(Collections.emptyMap());
    ConsumerRecords<byte[], byte[]> expectedU1C2 = new ConsumerRecords<>(Collections.emptyMap());
    ConsumerRecords<byte[], byte[]> expectedU2C1 = new ConsumerRecords<>(Collections.emptyMap());

    UserGroupInformation u1 = UserGroupInformation.createRemoteUser("U1");
    KafkaConsumer<byte[], byte[]> consumerU1 = mockConsumerForCurrentThreadAndUser(u1.getUserName());
    expect(consumerU1.poll(anyObject(Duration.class))).andReturn(expectedU1C1).andReturn(expectedU1C2);
    expectConsumerAssignTopic(consumerU1, ANY_TOPIC);

    UserGroupInformation u2 = UserGroupInformation.createRemoteUser("U2");
    KafkaConsumer<byte[], byte[]> consumerU2 = mockConsumerForCurrentThreadAndUser(u2.getUserName());
    expect(consumerU2.poll(anyObject(Duration.class))).andReturn(expectedU2C1);
    expectConsumerAssignTopic(consumerU2, ANY_TOPIC);

    replayAll();

    ByteConsumerPool consumerPool = this.consumerPool;

    final PrivilegedAction<ConsumerRecords<byte[], byte[]>> poll = consumerPool::poll;
    assertThat(u1.doAs(poll), sameInstance(expectedU1C1));
    assertThat(u2.doAs(poll), sameInstance(expectedU2C1));
    assertThat(u1.doAs(poll), sameInstance(expectedU1C2));

    verifyAll();
  }

  @Test
  public void cachesConsumerByThread() throws Exception {
    ConsumerRecords<byte[], byte[]> expectedT1C1 = new ConsumerRecords<>(Collections.emptyMap());
    ConsumerRecords<byte[], byte[]> expectedT1C2 = new ConsumerRecords<>(Collections.emptyMap());
    ConsumerRecords<byte[], byte[]> expectedT2C1 = new ConsumerRecords<>(Collections.emptyMap());

    ExecutorService t1 = Executors.newSingleThreadExecutor();
    Long idT1 = t1.submit(() -> Thread.currentThread().getId()).get();
    KafkaConsumer<byte[], byte[]> consumerT1 = mockConsumerForUserAndThread(idT1, ANY_USER);
    expect(consumerT1.poll(anyObject(Duration.class))).andReturn(expectedT1C1).andReturn(expectedT1C2);
    expectConsumerAssignTopic(consumerT1, ANY_TOPIC);

    ExecutorService t2 = Executors.newSingleThreadExecutor();
    Long idT2 = t2.submit(() -> Thread.currentThread().getId()).get();
    KafkaConsumer<byte[], byte[]> consumerT2 = mockConsumerForUserAndThread(idT2, ANY_USER);
    expect(consumerT2.poll(anyObject(Duration.class))).andReturn(expectedT2C1);
    expectConsumerAssignTopic(consumerT2, ANY_TOPIC);

    replayAll();

    ByteConsumerPool consumerPool = this.consumerPool;

    assertThat(t1.submit(consumerPool::poll).get(), sameInstance(expectedT1C1));
    assertThat(t2.submit(consumerPool::poll).get(), sameInstance(expectedT2C1));
    assertThat(t1.submit(consumerPool::poll).get(), sameInstance(expectedT1C2));

    verifyAll();
  }

  @Test
  public void throwsServerLoginExceptionOnConsumerFailure() {
    final Exception cause = new RuntimeException("Why not");
    final KafkaConsumer<byte[], byte[]> consumer = mockConsumerForCurrentThreadAndUser(ANY_USER);
    expect(consumer.poll(anyObject())).andThrow(cause);
    expectConsumerAssignTopic(consumer, ANY_TOPIC);
    ByteConsumerPool pool = consumerPool;
    replayAll();

    try {
      pool.poll();
      fail();
    } catch (RestServerErrorException e) {
      RestServerErrorException expected = Errors.serverLoginException(cause);
      assertThat(e.getCause(), is(expected.getCause()));
      assertThat(e.getErrorCode(), is(expected.getErrorCode()));
      assertThat(e.getMessage(), is(expected.getMessage()));
    }

    verifyAll();
  }

  private KafkaConsumer<byte[], byte[]> mockConsumerForCurrentThreadAndUser(String user) {
    return mockConsumerForUserAndThread(Thread.currentThread().getId(), user);
  }

  @SuppressWarnings("unchecked")
  private KafkaConsumer<byte[], byte[]> mockConsumerForUserAndThread(long threadId, String userName) {
    KafkaConsumer<byte[], byte[]> consumer = mock(KafkaConsumer.class);
    Properties expectedProps = new Properties();
    expectedProps.putAll(consumerConfig);
    expectedProps.put(ConsumerConfig.GROUP_ID_CONFIG, threadId + "_" + userName);
    expect(consumerFactory.apply(expectedProps)).andReturn(consumer).once();
    return consumer;
  }

  private void expectConsumerAssignTopic(KafkaConsumer<byte[], byte[]> consumer, String topic) {
    consumer.assign(Collections.singletonList(new TopicPartition(topic, 0)));
    expectLastCall();
  }
}